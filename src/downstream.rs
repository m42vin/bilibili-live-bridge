//! 面向本机客户端的 WebSocket 事件服务。
//!
//! 每条 `/ws` 连接先发送 `{"type":"subscribe","code":"身份码"}`，接入完成后收到
//! `ready` 和按序的 `event` 文本消息。慢客户端收到 `lagged`，不补发历史事件。
//! 完整协议见仓库的 `docs/downstream-protocol.md`。
//! 服务共享传入的 [`Manager`]，其闲置宽限期由 [`crate::session::ManagerOptions`] 决定。

use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use crate::session::{Manager, RecvError, Subscription};

mod protocol;
use protocol::{ServerMessage, subscription_code};

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const MAX_CLIENT_MESSAGE: usize = 8 * 1024;

type Socket = WebSocketStream<TcpStream>;
type Writer = SplitSink<Socket, Message>;
type Reader = SplitStream<Socket>;

#[derive(Clone, Copy)]
enum PeerState {
    Connected,
    Invalid(&'static str),
    Closed,
}

#[derive(Debug, PartialEq, Eq)]
enum Exit {
    End,
    Invalid(&'static str),
    AttachFailed,
    Disconnected,
    Failed,
}

/// 只发布当前 Ping 的确认，主动或旧 Pong 不会覆盖有效确认。
struct Pongs {
    expected: Mutex<Option<[u8; 8]>>,
    acknowledged: watch::Sender<Option<[u8; 8]>>,
}

impl Pongs {
    fn new() -> Self {
        Self {
            expected: Mutex::new(None),
            acknowledged: watch::channel(None).0,
        }
    }

    fn expect(&self, payload: [u8; 8]) {
        *self
            .expected
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(payload);
    }

    fn acknowledge(&self, payload: &[u8]) {
        let mut expected = self
            .expected
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if expected
            .as_ref()
            .is_some_and(|expected| payload == expected)
        {
            self.acknowledged.send_replace(expected.take());
        }
    }
}

/// 在已绑定的监听器上服务下游 WebSocket，直到退出信号就绪或监听失败。
///
/// 只接受 `/ws`，每条连接订阅一个身份码。握手、首包和单次发送最多等待 10 秒，
/// 入站消息最多 8 KiB；活动连接每 20 秒发送 Ping，10 秒内未收到对应 Pong 时断开。
/// 客户端断开后释放订阅；接入中的请求仍推进至完成，避免取消平台开启请求。
///
/// 退出时停止接入，关闭下游，等待所有连接任务和 [`Manager::shutdown`] 完成。
/// 本函数负责关闭传入管理器及其所有克隆共享的会话。调用方应通过 `shutdown` 退出，
/// 并等待本函数返回，而非取消此 future。
///
/// # Errors
///
/// 监听器接入失败时，在完成会话清理后返回 I/O 错误。单个客户端失败只记录日志。
pub async fn serve(
    listener: TcpListener,
    manager: Manager,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let (stopping, _) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            finished = connections.join_next(), if !connections.is_empty() => {
                log_join(finished);
            }
            incoming = listener.accept() => match incoming {
                Ok((stream, peer)) => {
                    let manager = manager.clone();
                    let signal = stopping.subscribe();
                    let span = tracing::info_span!("downstream", connection_id = %uuid::Uuid::new_v4(), %peer);
                    connections.spawn(
                        connection(stream, manager, signal).instrument(span).with_current_subscriber()
                    );
                }
                Err(error) => break Err(error),
            }
        }
    };
    drop(listener);
    stopping.send_replace(true);
    tokio::join!(manager.shutdown(), async {
        while let Some(result) = connections.join_next().await {
            log_join(Some(result));
        }
    });
    result
}

fn log_join(result: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(error)) = result {
        tracing::error!(%error, "下游连接任务失败");
    }
}

async fn stopped(signal: &mut watch::Receiver<bool>) {
    let _ = signal.wait_for(|stopping| *stopping).await;
}

async fn connection(stream: TcpStream, manager: Manager, mut shutdown: watch::Receiver<bool>) {
    let config = WebSocketConfig::default()
        .read_buffer_size(MAX_CLIENT_MESSAGE)
        .max_message_size(Some(MAX_CLIENT_MESSAGE))
        .max_frame_size(Some(MAX_CLIENT_MESSAGE));
    let handshake = accept_hdr_async_with_config(stream, check_path, Some(config));
    let socket = tokio::select! {
        biased;
        _ = stopped(&mut shutdown) => return,
        result = timeout(IO_TIMEOUT, handshake) => match result {
            Ok(Ok(socket)) => socket,
            _ => {
                // 握手错误可能包含客户端提交的头或 URL，不记录错误正文。
                tracing::debug!("下游 WebSocket 握手失败或超时");
                return;
            }
        },
    };
    tracing::info!("下游 WebSocket 已连接");
    let (mut writer, reader) = socket.split();
    let (first_tx, first_rx) = oneshot::channel();
    let (peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let pongs = Arc::new(Pongs::new());
    let (done, done_rx) = watch::channel(false);
    tokio::join!(
        read_peer(reader, first_tx, peer_tx, Arc::clone(&pongs), done_rx),
        async {
            let exit = write_peer(
                &mut writer,
                &manager,
                first_rx,
                &mut peer,
                &pongs,
                &mut shutdown,
            )
            .await;
            finish(&mut writer, &mut peer, exit).await;
            done.send_replace(true);
        }
    );
    tracing::info!("下游 WebSocket 已释放");
}

// tungstenite 的握手回调要求直接返回 HTTP ErrorResponse，不能改为 Box。
#[allow(clippy::result_large_err)]
fn check_path(request: &Request, response: Response) -> Result<Response, ErrorResponse> {
    if request.uri().path() == "/ws" {
        Ok(response)
    } else {
        let mut response = ErrorResponse::new(None);
        *response.status_mut() = StatusCode::NOT_FOUND;
        Err(response)
    }
}

async fn read_peer(
    mut reader: Reader,
    first: oneshot::Sender<String>,
    state: watch::Sender<PeerState>,
    pongs: Arc<Pongs>,
    mut done: watch::Receiver<bool>,
) {
    let mut first = Some(first);
    let mut invalid = false;
    loop {
        let incoming = tokio::select! {
            biased;
            _ = stopped(&mut done) => return,
            incoming = reader.next() => incoming,
        };
        match incoming {
            Some(Ok(Message::Text(text))) if !invalid => {
                let result = if first.is_some() {
                    subscription_code(&text)
                } else {
                    Err("每条连接只允许订阅一次，换房间请重新连接")
                };
                match result {
                    Ok(code) => {
                        let _ = first.take().unwrap().send(code);
                    }
                    Err(message) => {
                        invalid = true;
                        state.send_replace(PeerState::Invalid(message));
                        first.take();
                    }
                }
            }
            Some(Ok(Message::Binary(_))) if !invalid => {
                invalid = true;
                state.send_replace(PeerState::Invalid("只接受 JSON 文本消息"));
                first.take();
            }
            Some(Ok(Message::Pong(payload))) => {
                pongs.acknowledge(&payload);
            }
            Some(Ok(Message::Close(_))) => {
                state.send_replace(PeerState::Closed);
                // 继续轮询，驱动自动关闭应答。
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => {
                state.send_replace(PeerState::Closed);
                return;
            }
        }
    }
}

async fn peer_exit(peer: &mut watch::Receiver<PeerState>) -> Exit {
    match peer
        .wait_for(|state| !matches!(state, PeerState::Connected))
        .await
    {
        Ok(state) => match *state {
            PeerState::Invalid(message) => Exit::Invalid(message),
            _ => Exit::Disconnected,
        },
        Err(_) => Exit::Disconnected,
    }
}

async fn peer_closed(peer: &mut watch::Receiver<PeerState>) {
    let _ = peer
        .wait_for(|state| matches!(state, PeerState::Closed))
        .await;
}

async fn write_peer(
    writer: &mut Writer,
    manager: &Manager,
    first: oneshot::Receiver<String>,
    peer: &mut watch::Receiver<PeerState>,
    pongs: &Pongs,
    shutdown: &mut watch::Receiver<bool>,
) -> Exit {
    let code = match receive_code(first, peer, shutdown).await {
        Ok(code) => code,
        Err(exit) => return exit,
    };
    // 客户端断开时不丢弃 attach future；取得的订阅必须正常释放，平台请求才能完整清理。
    let attach = manager.attach(&code);
    let mut attach = std::pin::pin!(attach);
    let result = tokio::select! {
        biased;
        _ = stopped(shutdown) => { drop(attach.await); return Exit::End; }
        exit = peer_exit(peer) => { drop(attach.await); return exit; }
        result = &mut attach => result,
    };
    let mut subscription = match result {
        Ok(subscription) => subscription,
        Err(error) => {
            tracing::warn!(platform_code = ?error.platform_code().map(|code| code.raw()), "下游订阅接入失败");
            return Exit::AttachFailed;
        }
    };
    let ready = ServerMessage::Ready {
        room_id: subscription.room_id(),
        game_id: subscription.game_id(),
    };
    if let Err(exit) = send_json(writer, &ready, peer, shutdown).await {
        return exit;
    }
    forward(writer, &mut subscription, peer, pongs, shutdown).await
}

async fn receive_code(
    first: oneshot::Receiver<String>,
    peer: &mut watch::Receiver<PeerState>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<String, Exit> {
    tokio::select! {
        biased;
        _ = stopped(shutdown) => Err(Exit::End),
        exit = peer_exit(peer) => Err(exit),
        result = timeout(IO_TIMEOUT, first) => match result {
            Ok(Ok(code)) => Ok(code),
            Ok(Err(_)) => Err(peer_exit(peer).await),
            Err(_) => Err(Exit::Invalid("等待订阅消息超时")),
        },
    }
}

async fn forward<S: Sink<Message> + Unpin>(
    writer: &mut S,
    subscription: &mut Subscription,
    peer: &mut watch::Receiver<PeerState>,
    pongs: &Pongs,
    shutdown: &mut watch::Receiver<bool>,
) -> Exit {
    let mut ticker = interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut deadline = None;
    let mut pong = pongs.acknowledged.subscribe();
    let mut expected_pong = None;
    let mut ping_id = 0_u64;
    loop {
        tokio::select! {
            biased;
            _ = stopped(shutdown) => return Exit::End,
            exit = peer_exit(peer) => return exit,
            _ = pong.changed() => {
                if *pong.borrow_and_update() == expected_pong { deadline = None; }
            }
            _ = until_deadline(deadline) => {
                tracing::debug!("下游 Pong 超时");
                return Exit::Failed;
            }
            _ = ticker.tick() => {
                ping_id = ping_id.wrapping_add(1);
                let payload = ping_id.to_be_bytes();
                expected_pong = Some(payload);
                // 发送开始前登记，读端可能在 send 的 flush 完成之前收到应答。
                pongs.expect(payload);
                if let Err(exit) = send_frame(writer, Message::Ping(payload.to_vec().into()), peer, shutdown).await {
                    return exit;
                }
                deadline = Some(Instant::now() + IO_TIMEOUT);
            }
            incoming = subscription.recv() => {
                let message = match &incoming {
                    Ok(event) => ServerMessage::event(subscription.room_id(), event),
                    Err(RecvError::Lagged { skipped }) => ServerMessage::Lagged { skipped: *skipped },
                    Err(RecvError::Closed) => return Exit::End,
                };
                if let Err(exit) = send_json(writer, &message, peer, shutdown).await { return exit; }
                if incoming.is_ok_and(|event| event.ends_push()) { return Exit::End; }
            }
        }
    }
}

async fn until_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn send_json<S: Sink<Message> + Unpin>(
    writer: &mut S,
    message: &ServerMessage<'_>,
    peer: &mut watch::Receiver<PeerState>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), Exit> {
    let text = serde_json::to_string(message).map_err(|_| Exit::Failed)?;
    send_frame(writer, Message::text(text), peer, shutdown).await
}

async fn send_frame<S: Sink<Message> + Unpin>(
    writer: &mut S,
    message: Message,
    peer: &mut watch::Receiver<PeerState>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), Exit> {
    tokio::select! {
        biased;
        _ = stopped(shutdown) => Err(Exit::Failed),
        _ = peer_closed(peer) => Err(Exit::Disconnected),
        result = timeout(IO_TIMEOUT, writer.send(message)) => match result {
            Ok(Ok(())) => Ok(()),
            _ => {
                tracing::debug!("下游发送失败或超时");
                Err(Exit::Failed)
            }
        },
    }
}

async fn finish(writer: &mut Writer, peer: &mut watch::Receiver<PeerState>, exit: Exit) {
    if exit == Exit::Failed {
        return;
    }
    if matches!(*peer.borrow(), PeerState::Closed) || exit == Exit::Disconnected {
        let _ = timeout(IO_TIMEOUT, writer.flush()).await;
        return;
    }
    let (message, code) = match exit {
        Exit::End => (ServerMessage::Closed, CloseCode::Normal),
        Exit::Invalid(message) => (
            ServerMessage::Error {
                code: "invalid_request",
                message,
            },
            CloseCode::Policy,
        ),
        Exit::AttachFailed => (
            ServerMessage::Error {
                code: "attach_failed",
                message: "接入直播间失败，请检查身份码或服务端日志",
            },
            CloseCode::Error,
        ),
        _ => return,
    };
    let Ok(text) = serde_json::to_string(&message) else {
        return;
    };
    if !matches!(
        timeout(IO_TIMEOUT, writer.send(Message::text(text))).await,
        Ok(Ok(()))
    ) {
        return;
    }
    let close = Message::Close(Some(CloseFrame {
        code,
        reason: "".into(),
    }));
    if matches!(timeout(IO_TIMEOUT, writer.send(close)).await, Ok(Ok(()))) {
        let _ = timeout(IO_TIMEOUT, peer_closed(peer)).await;
    }
}

#[cfg(test)]
#[path = "downstream/downstream_test.rs"]
mod downstream_test;
