//! 一场官方 WebSocket。
//!
//! 按 `wss_link` 的顺序连接，直到某个集群收下鉴权包。之后由 [`Connection::recv`]
//! 在等待推送时发送 WebSocket 心跳。项目心跳不在这里发送。

use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::task::{Poll, ready};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::Error as SocketError;
use tokio_tungstenite::tungstenite::http::header::{HeaderValue, USER_AGENT};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use super::HealthPolicy;
use super::cmd::LiveEvent;
use super::error::WsError;
use super::packet::{self, FrameItem, Operation, decode_frame};
use crate::open_live::api::StartResult;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_EARLY_EVENTS: usize = 32;

type Upstream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Writer = SplitSink<Upstream, Message>;
type Reader = SplitStream<Upstream>;

/// 已经完成鉴权的官方长连接。
///
/// 由一个任务调用 [`Connection::recv`] 驱动。读的过程中会回答 Ping，并按给定间隔发送空正文的心跳包。
/// 不启动独立后台任务；长时间停止轮询或阻塞在事件处理上会影响心跳。
/// 连接断开后不会自动重连，丢弃此值也不会调用项目结束 API。
pub struct Connection {
    endpoint: String,
    heartbeat_every: Duration,
    write: Option<Writer>,
    read: Option<Reader>,
    heartbeat_at: Instant,
    health: HealthPolicy,
    activity_deadline: Instant,
    pending_write: PendingWrite,
    pending_events: VecDeque<LiveEvent>,
    closed: bool,
}

impl Connection {
    /// 按地址顺序连接，直到某个集群返回鉴权成功。
    ///
    /// 连接、超时、或在鉴权回复前断开时会换下一个地址。鉴权回复明确失败时不再尝试其余地址。
    /// `heartbeat` 是 WebSocket 心跳间隔，必须大于 0。项目心跳要另外调用应用 API。
    /// 每个候选地址的连接和鉴权总共最多等待 10 秒。
    /// 鉴权成功前最多缓存 32 条事件；成功回复之后的同帧事件按正常帧上限处理，
    /// 所有已缓存事件由 [`Connection::recv`] 按接收顺序返回。
    ///
    /// # Errors
    ///
    /// 鉴权正文为空、心跳间隔为零或没有非空地址时返回 [`WsError::Invalid`]；
    /// 所有地址均无法完成连接时返回 [`WsError::Connect`]；
    /// 鉴权回复明确失败时返回 [`WsError::Protocol`]。
    #[must_use = "连接失败时需要处理 WsError"]
    #[tracing::instrument(name = "websocket_connect", skip_all)]
    pub async fn connect(
        links: impl IntoIterator<Item = impl AsRef<str>>,
        auth_body: &str,
        heartbeat: Duration,
    ) -> Result<Self, WsError> {
        Self::connect_with_policy(links, auth_body, heartbeat, HealthPolicy::default()).await
    }

    /// 用自定义发送和存活期限建立官方连接。
    ///
    /// 持续轮询 [`Self::recv`] 驱动检测。有效事件及官方心跳回复刷新存活期限。
    ///
    /// # Errors
    ///
    /// 除 [`Self::connect`] 的错误外，无效健康策略返回 [`WsError::Invalid`]。
    #[tracing::instrument(name = "websocket_connect", skip_all)]
    pub async fn connect_with_policy(
        links: impl IntoIterator<Item = impl AsRef<str>>,
        auth_body: &str,
        heartbeat: Duration,
        health: HealthPolicy,
    ) -> Result<Self, WsError> {
        let auth_body = auth_body.trim();
        if auth_body.is_empty() {
            return Err(WsError::invalid("auth_body 不能为空"));
        }
        if heartbeat.is_zero() {
            return Err(WsError::invalid("心跳间隔必须大于 0"));
        }
        health.validate(heartbeat).map_err(WsError::invalid)?;
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
            let attempt = tokio::time::timeout(
                HANDSHAKE_TIMEOUT,
                establish(link, auth_body, heartbeat, health),
            )
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
    ///
    /// # Errors
    ///
    /// 与 [`Connection::connect`] 相同。失败后调用方仍需结束已经开启的场次。
    #[must_use = "连接失败时需要处理 WsError"]
    #[tracing::instrument(skip_all, fields(game_id = started.game_id(), room_id = started.anchor().room_id))]
    pub async fn from_start(started: &StartResult, heartbeat: Duration) -> Result<Self, WsError> {
        Self::connect(started.wss_link(), started.auth_body(), heartbeat).await
    }

    /// 使用场次信息及自定义健康期限建立连接。
    ///
    /// # Errors
    ///
    /// 与 [`Self::connect_with_policy`] 相同；失败后调用方仍需结束场次。
    #[tracing::instrument(skip_all, fields(game_id = started.game_id(), room_id = started.anchor().room_id))]
    pub async fn from_start_with_policy(
        started: &StartResult,
        heartbeat: Duration,
        health: HealthPolicy,
    ) -> Result<Self, WsError> {
        Self::connect_with_policy(started.wss_link(), started.auth_body(), heartbeat, health).await
    }

    /// 当前连上的官方地址。
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// 读下一条直播间事件。
    ///
    /// 连接正常关闭时返回 `Ok(None)`。同一帧里的多条事件会在后续调用里依次返回。
    /// 等待期间也驱动心跳和 Ping/Pong；本方法不维护项目心跳。
    ///
    /// # Errors
    ///
    /// 读写失败、发送或存活期限到期、二进制协议不符或事件 JSON 无法解析时返回 [`WsError`]。
    /// 超时后立即释放连接；取消读取不会重置期限，调用方仍须持续轮询驱动心跳和检查。
    #[must_use = "读取失败时需要处理 WsError"]
    pub async fn recv(&mut self) -> Result<Option<LiveEvent>, WsError> {
        let result = self.recv_inner().await;
        if result.is_err() || self.closed {
            self.closed = true;
            self.write.take();
            self.read.take();
            self.pending_write = PendingWrite::default();
            self.pending_events.clear();
        }
        result
    }

    async fn recv_inner(&mut self) -> Result<Option<LiveEvent>, WsError> {
        loop {
            if self.closed {
                return Ok(None);
            }
            if Instant::now() >= self.activity_deadline {
                return Err(deadline_error("activity_timeout"));
            }
            if self.pending_write.deadline.is_some() {
                self.pending_write
                    .drive(self.write.as_mut().unwrap(), self.activity_deadline)
                    .await?;
                continue;
            }
            if Instant::now() >= self.heartbeat_at {
                let packet = packet::encode(Operation::Heartbeat, &[])?;
                self.pending_write
                    .queue(Message::binary(packet), self.health.write_timeout);
                self.heartbeat_at = Instant::now() + self.heartbeat_every;
                tracing::trace!(endpoint = %self.endpoint, "排入 WebSocket 心跳");
                continue;
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(Some(event));
            }
            tokio::select! {
                biased;
                _ = sleep_until(self.activity_deadline) => return Err(deadline_error("activity_timeout")),
                _ = sleep_until(self.heartbeat_at) => {},
                incoming = self.read.as_mut().unwrap().next() => {
                    self.consume(incoming)?;
                }
            }
        }
    }

    /// 发送关闭帧。已经标记关闭后再次调用没有额外效果，也不会重试失败的发送。
    ///
    /// 发送最多等待 5 秒；失败或超时后不再读写，调用方应丢弃连接以释放传输层。
    /// 本方法只处理官方长连接，不调用 [`crate::open_live::api::Client::end`]。
    ///
    /// # Errors
    ///
    /// 发送超时，或发送失败且连接尚未由底层确认关闭时返回 [`WsError::Transport`]。
    #[must_use = "关闭失败时需要处理 WsError"]
    pub async fn close(&mut self) -> Result<(), WsError> {
        if self.closed {
            return Ok(());
        }
        self.pending_write = PendingWrite::default();
        self.closed = true;
        let result = send_close(self.write.as_mut().unwrap()).await;
        self.write.take();
        self.read.take();
        result
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
                let mut active = false;
                for item in decode_frame(&bytes)? {
                    match item {
                        FrameItem::Event(event) => {
                            active = true;
                            self.pending_events.push_back(*event);
                        }
                        FrameItem::HeartbeatReply => active = true,
                        FrameItem::AuthReply(_) => {}
                    }
                }
                if active {
                    self.activity_deadline = Instant::now() + self.health.activity_timeout;
                }
                Ok(())
            }
            Message::Ping(payload) => {
                self.pending_write
                    .queue(Message::Pong(payload), self.health.write_timeout);
                Ok(())
            }
            Message::Pong(_) | Message::Frame(_) => Ok(()),
            Message::Close(frame) => {
                self.closed = true;
                if frame.is_some_and(|frame| fatal_close(frame.code)) {
                    return Err(WsError::protocol("官方长连接拒绝继续通信"));
                }
                Ok(())
            }
            Message::Text(_) => Err(WsError::protocol("官方长连接应使用二进制帧")),
        }
    }

    fn fail(&mut self, message: &'static str, error: SocketError) -> WsError {
        self.closed = true;
        socket_error(message, error)
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
    health: HealthPolicy,
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
            Message::Close(frame) => {
                if frame.is_some_and(|frame| fatal_close(frame.code)) {
                    return Err(Attempt::Fatal(WsError::protocol("官方长连接拒绝继续通信")));
                }
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
                        FrameItem::HeartbeatReply => {}
                        FrameItem::AuthReply(body) => {
                            packet::auth_accepted(&body).map_err(Attempt::Fatal)?;
                            authenticated = true;
                        }
                        FrameItem::Event(event) => {
                            // 同帧鉴权成功后的事件仅受正常帧预算约束。
                            if !authenticated && early_events.len() >= MAX_EARLY_EVENTS {
                                return Err(Attempt::Retry("鉴权完成前收到过多推送".to_owned()));
                            }
                            early_events.push_back(*event);
                        }
                    }
                }
                if authenticated {
                    return Ok(Connection {
                        endpoint: link.to_owned(),
                        heartbeat_every,
                        write: Some(write),
                        read: Some(read),
                        heartbeat_at: Instant::now() + heartbeat_every,
                        health,
                        activity_deadline: Instant::now() + health.activity_timeout,
                        pending_write: PendingWrite::default(),
                        pending_events: early_events,
                        closed: false,
                    });
                }
            }
        }
    }
}

/// 发送状态保存在连接中，取消 drive future 不会丢失消息或刷新绝对期限。
#[derive(Default)]
struct PendingWrite {
    message: Option<Message>,
    flushing: bool,
    deadline: Option<Instant>,
}

impl PendingWrite {
    fn queue(&mut self, message: Message, timeout: Duration) {
        debug_assert!(self.deadline.is_none());
        self.message = Some(message);
        self.deadline = Some(Instant::now() + timeout);
    }

    async fn drive(
        &mut self,
        writer: &mut (impl Sink<Message, Error = SocketError> + Unpin),
        activity_deadline: Instant,
    ) -> Result<(), WsError> {
        let write_deadline = self.deadline.unwrap();
        let deadline = write_deadline.min(activity_deadline);
        if Instant::now() >= deadline {
            return Err(deadline_error(if activity_deadline <= write_deadline {
                "activity_timeout"
            } else {
                "write_timeout"
            }));
        }
        let result = timeout_at(
            deadline,
            poll_fn(|cx| {
                // timeout_at 会先轮询内部 future；到期时不能再推进写入。
                if Instant::now() >= deadline {
                    return Poll::Pending;
                }
                if !self.flushing {
                    ready!(Pin::new(&mut *writer).poll_ready(cx))?;
                    Pin::new(&mut *writer).start_send(self.message.take().unwrap())?;
                    self.flushing = true;
                }
                ready!(Pin::new(&mut *writer).poll_flush(cx))?;
                self.flushing = false;
                self.deadline = None;
                Poll::Ready(Ok(()))
            }),
        )
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(socket_error("发送官方长连接数据失败", error)),
            Err(_) => Err(deadline_error(if activity_deadline <= write_deadline {
                "activity_timeout"
            } else {
                "write_timeout"
            })),
        }
    }
}

fn deadline_error(reason: &'static str) -> WsError {
    tracing::warn!(reason, "官方长连接健康期限已到");
    let message = if reason == "activity_timeout" {
        "官方长连接存活超时"
    } else {
        "官方长连接发送超时"
    };
    WsError::transport(message, io::Error::new(io::ErrorKind::TimedOut, reason))
}

fn socket_error(message: &'static str, error: SocketError) -> WsError {
    match error {
        SocketError::Protocol(_) | SocketError::Utf8(_) | SocketError::Capacity(_) => {
            WsError::protocol("官方长连接传输协议错误")
        }
        error => WsError::transport(message, error),
    }
}

fn fatal_close(code: CloseCode) -> bool {
    matches!(
        code,
        CloseCode::Protocol | CloseCode::Unsupported | CloseCode::Invalid | CloseCode::Policy
    )
}

async fn send_close(
    write: &mut (impl Sink<Message, Error = SocketError> + Unpin),
) -> Result<(), WsError> {
    match tokio::time::timeout(CLOSE_TIMEOUT, write.send(Message::Close(None))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) if socket_closed(&error) => Ok(()),
        Ok(Err(error)) => Err(WsError::transport("关闭官方长连接失败", error)),
        Err(error) => Err(WsError::transport("关闭官方长连接超时", error)),
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

#[cfg(test)]
#[path = "health_test.rs"]
mod health_test;
