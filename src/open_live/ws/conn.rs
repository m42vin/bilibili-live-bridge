//! 一场官方 WebSocket。
//!
//! 按 `wss_link` 的顺序连接，直到某个集群收下鉴权包。之后由 [`Connection::recv`]
//! 在等待推送时发送 WebSocket 心跳。项目心跳不在这里发送。

use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{Interval, MissedTickBehavior, interval};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as SocketError;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, USER_AGENT};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use super::cmd::LiveEvent;
use super::error::WsError;
use super::packet::{self, FrameItem, Operation, decode_frame};
use crate::open_live::api::StartResult;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_EARLY_EVENTS: usize = 32;

type Upstream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Writer = SplitSink<Upstream, Message>;
type Reader = SplitStream<Upstream>;

/// 已经完成鉴权的官方长连接。
///
/// 由一个任务调用 [`Connection::recv`] 驱动。读的过程中会回答 Ping，并按给定间隔发送空正文的心跳包。
pub struct Connection {
    endpoint: String,
    heartbeat_every: Duration,
    write: Writer,
    read: Reader,
    heartbeat: Interval,
    pending_write: Option<Message>,
    pending_events: VecDeque<LiveEvent>,
    closed: bool,
}

impl Connection {
    /// 按地址顺序连接，直到某个集群返回鉴权成功。
    ///
    /// 连接、超时、或在鉴权回复前断开时会换下一个地址。鉴权回复明确失败时不再尝试其余地址。
    /// `heartbeat` 是 WebSocket 心跳间隔，必须大于 0。项目心跳要另外调用应用 API。
    #[must_use = "连接失败时需要处理 WsError"]
    #[tracing::instrument(name = "websocket_connect", skip_all)]
    pub async fn connect(
        links: impl IntoIterator<Item = impl AsRef<str>>,
        auth_body: &str,
        heartbeat: Duration,
    ) -> Result<Self, WsError> {
        let auth_body = auth_body.trim();
        if auth_body.is_empty() {
            return Err(WsError::invalid("auth_body 不能为空"));
        }
        if heartbeat.is_zero() {
            return Err(WsError::invalid("心跳间隔必须大于 0"));
        }
        let links: Vec<String> = links
            .into_iter()
            .map(|link| link.as_ref().trim().to_owned())
            .filter(|link| !link.is_empty())
            .collect();
        if links.is_empty() {
            return Err(WsError::invalid("没有可用的官方长连接地址"));
        }

        let mut failures = Vec::new();
        for (index, link) in links.iter().enumerate() {
            tracing::debug!(endpoint = %link, attempt = index + 1, "尝试连接官方 WebSocket");
            let attempt =
                tokio::time::timeout(HANDSHAKE_TIMEOUT, establish(link, auth_body, heartbeat))
                    .await;
            match attempt {
                Ok(Ok(connection)) => {
                    tracing::info!(endpoint = %link, "官方 WebSocket 已连接并完成鉴权");
                    return Ok(connection);
                }
                Ok(Err(Attempt::Fatal(error))) => {
                    tracing::warn!(endpoint = %link, %error, "官方 WebSocket 鉴权失败");
                    return Err(error);
                }
                Ok(Err(Attempt::Retry(message))) => {
                    tracing::warn!(endpoint = %link, error = %message, "官方 WebSocket 连接失败，尝试下一地址");
                    failures.push(format!("{link}：{message}"));
                }
                Err(_elapsed) => {
                    tracing::warn!(endpoint = %link, "官方 WebSocket 连接超时，尝试下一地址");
                    failures.push(format!("{link}：连接超时"));
                }
            }
        }
        Err(WsError::connect(format!(
            "官方长连接全部失败：{}",
            failures.join("；")
        )))
    }

    /// 用 [`StartResult`] 里的长连接信息建立连接。
    #[must_use = "连接失败时需要处理 WsError"]
    #[tracing::instrument(skip_all, fields(game_id = started.game_id(), room_id = started.anchor().room_id))]
    pub async fn from_start(started: &StartResult, heartbeat: Duration) -> Result<Self, WsError> {
        Self::connect(started.wss_link(), started.auth_body(), heartbeat).await
    }

    /// 当前连上的官方地址。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// 读下一条直播间事件。
    ///
    /// 连接正常关闭时返回 `Ok(None)`。同一帧里的多条事件会在后续调用里依次返回。
    #[must_use = "读取失败时需要处理 WsError"]
    pub async fn recv(&mut self) -> Result<Option<LiveEvent>, WsError> {
        loop {
            if self.closed {
                return Ok(None);
            }
            if let Some(message) = self.pending_write.take() {
                self.write
                    .send(message)
                    .await
                    .map_err(|error| self.fail("发送官方长连接数据失败", error))?;
                continue;
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
            tokio::select! {
                _ = self.heartbeat.tick() => {
                    let packet = packet::encode(Operation::Heartbeat, &[])?;
                    self.pending_write = Some(Message::binary(packet));
                    tracing::trace!(endpoint = %self.endpoint, "排入 WebSocket 心跳");
                }
                incoming = self.read.next() => {
                    self.consume(incoming)?;
                }
            }
        }
    }

    /// 发送关闭帧。重复调用没有额外效果。
    #[must_use = "关闭失败时需要处理 WsError"]
    pub async fn close(&mut self) -> Result<(), WsError> {
        if self.closed {
            return Ok(());
        }
        self.pending_write = None;
        self.closed = true;
        match self.write.send(Message::Close(None)).await {
            Ok(()) => Ok(()),
            Err(error) if socket_closed(&error) => Ok(()),
            Err(error) => Err(WsError::transport("关闭官方长连接失败", error)),
        }
    }

    fn consume(&mut self, incoming: Option<Result<Message, SocketError>>) -> Result<(), WsError> {
        let message = match incoming {
            None => {
                self.closed = true;
                return Ok(());
            }
            Some(Err(error)) if socket_closed(&error) => {
                self.closed = true;
                return Ok(());
            }
            Some(Err(error)) => return Err(self.fail("读取官方长连接失败", error)),
            Some(Ok(message)) => message,
        };
        match message {
            Message::Binary(bytes) => {
                if bytes.is_empty() {
                    return Err(WsError::protocol("官方长连接收到空二进制帧"));
                }
                for item in decode_frame(&bytes)? {
                    if let FrameItem::Event(event) = item {
                        self.pending_events.push_back(*event);
                    }
                }
                Ok(())
            }
            Message::Ping(payload) => {
                self.pending_write = Some(Message::Pong(payload));
                Ok(())
            }
            Message::Pong(_) | Message::Frame(_) => Ok(()),
            Message::Close(_) => {
                self.closed = true;
                Ok(())
            }
            Message::Text(_) => Err(WsError::protocol("官方长连接应使用二进制帧")),
        }
    }

    fn fail(&mut self, message: &'static str, error: SocketError) -> WsError {
        self.closed = true;
        WsError::transport(message, error)
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("endpoint", &self.endpoint)
            .field("heartbeat_every", &self.heartbeat_every)
            .field("closed", &self.closed)
            .finish()
    }
}

enum Attempt {
    Retry(String),
    Fatal(WsError),
}

async fn establish(
    link: &str,
    auth_body: &str,
    heartbeat_every: Duration,
) -> Result<Connection, Attempt> {
    if !is_websocket_url(link) {
        return Err(Attempt::Retry("地址必须是 ws 或 wss".to_owned()));
    }
    let mut request = link
        .into_client_request()
        .map_err(|error| Attempt::Retry(error.to_string()))?;
    request.headers_mut().insert(
        USER_AGENT,
        HeaderValue::from_static(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        )),
    );
    let config = WebSocketConfig::default()
        .write_buffer_size(0)
        .max_message_size(Some(packet::MAX_PACKET_LEN))
        .max_frame_size(Some(packet::MAX_PACKET_LEN));
    let (socket, _response) = connect_async_with_config(request, Some(config), true)
        .await
        .map_err(|error| Attempt::Retry(error.to_string()))?;
    let (mut write, mut read) = socket.split();
    let auth = packet::encode(Operation::Auth, auth_body.as_bytes()).map_err(Attempt::Fatal)?;
    write
        .send(Message::binary(auth))
        .await
        .map_err(|error| Attempt::Retry(error.to_string()))?;

    let mut early_events = VecDeque::new();
    loop {
        let incoming = read
            .next()
            .await
            .ok_or_else(|| Attempt::Retry("连接在鉴权回复前关闭".to_owned()))?
            .map_err(|error| Attempt::Retry(error.to_string()))?;
        match incoming {
            Message::Ping(payload) => {
                write
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|error| Attempt::Retry(error.to_string()))?;
            }
            Message::Pong(_) | Message::Frame(_) => {}
            Message::Close(_) => {
                return Err(Attempt::Retry("连接在鉴权回复前关闭".to_owned()));
            }
            Message::Text(_) => {
                return Err(Attempt::Retry("官方长连接应使用二进制帧".to_owned()));
            }
            Message::Binary(bytes) => {
                let items =
                    decode_frame(&bytes).map_err(|error| Attempt::Retry(error.to_string()))?;
                let mut authenticated = false;
                for item in items {
                    match item {
                        FrameItem::AuthReply(body) => {
                            packet::auth_accepted(&body).map_err(Attempt::Fatal)?;
                            authenticated = true;
                        }
                        FrameItem::Event(event) => {
                            if early_events.len() >= MAX_EARLY_EVENTS {
                                return Err(Attempt::Retry("鉴权完成前收到过多推送".to_owned()));
                            }
                            early_events.push_back(*event);
                        }
                    }
                }
                if authenticated {
                    let mut heartbeat = interval(heartbeat_every);
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    heartbeat.tick().await;
                    return Ok(Connection {
                        endpoint: link.to_owned(),
                        heartbeat_every,
                        write,
                        read,
                        heartbeat,
                        pending_write: None,
                        pending_events: early_events,
                        closed: false,
                    });
                }
            }
        }
    }
}

fn is_websocket_url(link: &str) -> bool {
    let scheme = link.split_once("://").map(|(scheme, _)| scheme);
    scheme.is_some_and(|scheme| {
        scheme.eq_ignore_ascii_case("ws") || scheme.eq_ignore_ascii_case("wss")
    })
}

fn socket_closed(error: &SocketError) -> bool {
    matches!(
        error,
        SocketError::ConnectionClosed | SocketError::AlreadyClosed
    )
}

#[cfg(test)]
#[path = "conn_test.rs"]
mod conn_test;
