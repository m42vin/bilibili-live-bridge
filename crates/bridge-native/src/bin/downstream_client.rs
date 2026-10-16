//! 下游客户端示例：身份码订阅、处理后 ACK、状态处理和按已处理游标续传。
//!
//! 先启动 `server`，再设置 `AUTH_CODE` 并运行本程序；无需平台 Access Key。
//! `BRIDGE_URL` 可设为 HTTP(S) 接入地址，先 POST 获取短期连接凭证；默认直连地址为 `ws://127.0.0.1:8080/ws`。网络断开按 2–10 秒退避重连；
//! `closed`、服务端 `error`、续传失败或无效业务消息结束程序。游标仅存于进程内。
//! 业务处理独立于网络读取，Ctrl-C 在连接和退避期间均有效。

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bilibili_live_bridge::session::EventCursor;
use bridge_core::downstream::admission::{
    AdmissionFailure, AdmissionRequest, AdmissionResponse, is_retryable_code,
};
use bridge_core::downstream::protocol::ClientMessage;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::{Error, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const IO_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const RETRY_INITIAL: Duration = Duration::from_secs(2);
const RETRY_MAX: Duration = Duration::from_secs(10);
const STABLE_TIME: Duration = Duration::from_secs(60);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bilibili_live_bridge::logging::init()?;
    let settings = Settings::from_env()?;
    run_client(&settings, tokio::signal::ctrl_c()).await?;
    Ok(())
}

struct Settings {
    url: String,
    code: String,
}

impl Settings {
    fn is_http(&self) -> bool {
        self.url.starts_with("http://") || self.url.starts_with("https://")
    }

    fn from_env() -> Result<Self, String> {
        let code = std::env::var("AUTH_CODE").map_err(|_| "缺少环境变量 AUTH_CODE")?;
        let code = code.trim().to_owned();
        if code.is_empty() {
            return Err("AUTH_CODE 不能为空".into());
        }
        let url = match std::env::var("BRIDGE_URL") {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent) => "ws://127.0.0.1:8080/ws".into(),
            Err(_) => return Err("BRIDGE_URL 必须是有效文本".into()),
        };
        validate_endpoint(&url)?;
        let url = reqwest::Url::parse(&url)
            .map_err(|_| "无效 BRIDGE_URL")?
            .to_string();
        Ok(Self { url, code })
    }
}

fn validate_endpoint(url: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "BRIDGE_URL 不是有效地址")?;
    if !matches!(parsed.scheme(), "http" | "https" | "ws" | "wss")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err("BRIDGE_URL 必须为无用户信息和片段的 http(s)/ws(s) 地址".into());
    }
    Ok(())
}

enum ConnectFailure {
    Retry(Option<u64>),
    Fatal(String),
}

async fn connect(
    settings: &Settings,
    after: Option<EventCursor>,
) -> Result<Socket, ConnectFailure> {
    let (url, expires) = if settings.is_http() {
        let started = Instant::now();
        let response = timeout(IO_TIMEOUT, admit(settings, after))
            .await
            .map_err(|_| ConnectFailure::Retry(None))??;
        let remaining =
            Duration::from_millis(response.expires_in_ms).saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(ConnectFailure::Retry(None));
        }
        (response.websocket_url, IO_TIMEOUT.min(remaining))
    } else {
        (settings.url.clone(), IO_TIMEOUT)
    };
    match timeout(expires, connect_async(url.as_str())).await {
        Ok(Ok((socket, _))) => Ok(socket),
        Ok(Err(Error::Http(response))) => {
            let status = response.status().as_u16();
            if matches!(status, 429 | 503) {
                return Err(ConnectFailure::Retry(retry_after(
                    response
                        .headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok()),
                )));
            }
            // 已签发的凭证可能在网络传输中到期；重新接入，不复用旧 URL。
            if settings.is_http() && matches!(status, 401 | 410) {
                return Err(ConnectFailure::Retry(None));
            }
            if response.status().is_client_error() || response.status().is_redirection() {
                return Err(ConnectFailure::Fatal(format!(
                    "桥接服务拒绝握手，HTTP {status}，请检查 BRIDGE_URL 或访问权限"
                )));
            }
            Err(ConnectFailure::Retry(None))
        }
        _ => Err(ConnectFailure::Retry(None)),
    }
}

fn retry_after(value: Option<&str>) -> Option<u64> {
    value?.parse::<u64>().ok()?.checked_mul(1000)
}

async fn admit(
    settings: &Settings,
    after: Option<EventCursor>,
) -> Result<AdmissionResponse, ConnectFailure> {
    let request = AdmissionRequest {
        code: settings.code.clone(),
        ack: true,
        ack_window: Some(64),
        resume_after: after,
    }
    .validate()
    .map_err(|message| ConnectFailure::Fatal(message.into()))?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .build()
        .map_err(|_| ConnectFailure::Fatal("无法初始化 HTTP 接入客户端".into()))?;
    let mut response = client
        .post(&settings.url)
        .json(&request)
        .send()
        .await
        .map_err(|_| ConnectFailure::Retry(None))?;
    let status = response.status();
    let advice = retry_after(
        response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok()),
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ConnectFailure::Retry(None))?
    {
        if bytes.len().saturating_add(chunk.len()) > 8192 {
            return Err(ConnectFailure::Fatal("HTTP 接入响应超过 8 KiB".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    if status.as_u16() == 200 {
        let granted: AdmissionResponse = serde_json::from_slice(&bytes)
            .map_err(|_| ConnectFailure::Fatal("无效 HTTP 接入响应".into()))?;
        validate_endpoint(&granted.websocket_url)
            .map_err(|_| ConnectFailure::Fatal("接入服务返回无效 WebSocket 地址".into()))?;
        let url = reqwest::Url::parse(&granted.websocket_url).unwrap();
        if !matches!(url.scheme(), "ws" | "wss")
            || (settings.url.starts_with("https://") && url.scheme() != "wss")
            || granted.expires_in_ms == 0
        {
            return Err(ConnectFailure::Fatal(
                "接入服务返回无效或不安全的凭证".into(),
            ));
        }
        return Ok(granted);
    }
    if let Ok(failure) = serde_json::from_slice::<AdmissionFailure>(&bytes) {
        return Err(match failure {
            AdmissionFailure::ResumeUnavailable { .. } => {
                ConnectFailure::Fatal("续传不可用，不能自动放弃游标".into())
            }
            AdmissionFailure::Error {
                code,
                retry_after_ms,
                ..
            } if is_retryable_code(&code) => {
                ConnectFailure::Retry(retry_after_ms.into_iter().chain(advice).max())
            }
            AdmissionFailure::Error { .. } => {
                ConnectFailure::Fatal("接入失败，请检查身份码和服务端日志".into())
            }
        });
    }
    if status.as_u16() == 429 || status.is_server_error() {
        Err(ConnectFailure::Retry(advice))
    } else {
        Err(ConnectFailure::Fatal(format!(
            "HTTP 接入被拒绝或响应无效，HTTP {}",
            status.as_u16()
        )))
    }
}

// 示例将游标保留在进程内。需要跨客户端重启续传时，应与业务结果一起持久化。
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Ping {
        nonce: String,
    },
    Ready {
        room_id: i64,
        game_id: String,
        stream_id: String,
        seq: u64,
        resumed: bool,
        ack_window: u64,
    },
    Event {
        room_id: i64,
        stream_id: String,
        seq: u64,
        cmd: String,
        data: Value,
    },
    Status {
        state: String,
    },
    ResumeUnavailable {
        reason: String,
        earliest_after: Option<u64>,
        latest_seq: Option<u64>,
    },
    Error {
        code: String,
        retry_after_ms: Option<u64>,
    },
    Closed,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Retry { retry_after_ms: Option<u64> },
    Finished,
    Stopped,
}

type CursorState = Arc<Mutex<Option<EventCursor>>>;
struct Work {
    cursor: EventCursor,
    room_id: i64,
    cmd: String,
    data: Value,
}

async fn run_client(
    settings: &Settings,
    shutdown: impl Future<Output = std::io::Result<()>>,
) -> Result<(), String> {
    let mut shutdown = std::pin::pin!(shutdown);
    let mut delay = RETRY_INITIAL;
    let cursor: CursorState = Arc::new(Mutex::new(None));
    loop {
        tracing::info!("连接直播桥");
        let after = cursor.lock().unwrap().clone();
        let connected = tokio::select! {
            biased;
            result = shutdown.as_mut() => return interrupted(result),
            result = connect(settings, after) => result,
        };
        let mut stable = false;
        let suggested;
        match connected {
            Ok(mut socket) => {
                let mut ready_at = None;
                let outcome = listen(
                    &mut socket,
                    (!settings.is_http()).then_some(settings.code.as_str()),
                    &cursor,
                    &mut ready_at,
                    shutdown.as_mut(),
                )
                .await;
                stable = ready_at.is_some_and(|at: Instant| at.elapsed() >= STABLE_TIME);
                let _ = timeout(Duration::from_secs(2), async {
                    let _ = socket.close(None).await;
                    while let Some(Ok(message)) = socket.next().await {
                        if matches!(message, Message::Close(_)) {
                            let _ = socket.flush().await;
                            break;
                        }
                    }
                })
                .await;
                match outcome? {
                    Outcome::Finished | Outcome::Stopped => return Ok(()),
                    Outcome::Retry { retry_after_ms } => {
                        suggested = retry_after_ms;
                    }
                }
            }
            Err(ConnectFailure::Retry(retry_after_ms)) => {
                suggested = retry_after_ms;
                tracing::warn!(retry_after_ms = suggested, "接入失败或入口暂时繁忙");
            }
            Err(ConnectFailure::Fatal(message)) => return Err(message),
        }
        if stable {
            delay = RETRY_INITIAL;
        }
        let wait = retry_delay(delay, suggested, jitter());
        tracing::warn!(
            retry_secs = wait.as_secs_f64(),
            "下游断开，等待按已处理游标续传"
        );
        tokio::select! {
            biased;
            result = shutdown.as_mut() => return interrupted(result),
            _ = sleep(wait) => {}
        }
        delay = (delay * 2).min(RETRY_MAX);
    }
}

// 建议值设置下限，抖动只增加等待，不让大量客户端同时重试。
fn retry_delay(base: Duration, suggestion: Option<u64>, jitter_percent: u32) -> Duration {
    let suggested = Duration::from_millis(suggestion.unwrap_or(0));
    let base = base.max(suggested);
    // 不接受无法表示的远端期限，避免定时器溢出；仍保留已有游标。
    let base = base.min(Duration::from_secs(24 * 60 * 60));
    base.saturating_add(base.mul_f64(f64::from(jitter_percent.min(20)) / 100.0))
}

fn jitter() -> u32 {
    u32::from(uuid::Uuid::new_v4().as_bytes()[0]) % 21
}

fn interrupted(result: std::io::Result<()>) -> Result<(), String> {
    result.map_err(|_| "无法监听 Ctrl-C".to_owned())?;
    tracing::info!("收到中断，退出客户端");
    Ok(())
}

async fn process_events(
    mut work: mpsc::Receiver<Work>,
    cursor: CursorState,
    done: watch::Sender<Option<EventCursor>>,
) {
    while let Some(work) = work.recv().await {
        // 替换为自己的异步业务处理。只有处理成功才更新游标并允许发送 ACK。
        tracing::info!(room_id = work.room_id, cmd = %work.cmd, seq = work.cursor.seq, "收到直播事件");
        tracing::debug!(data = %work.data, "直播事件内容");
        *cursor.lock().unwrap() = Some(work.cursor.clone());
        done.send_replace(Some(work.cursor));
    }
}

async fn listen<F: Future<Output = std::io::Result<()>>>(
    socket: &mut Socket,
    code: Option<&str>,
    cursor: &CursorState,
    ready_at: &mut Option<Instant>,
    shutdown: Pin<&mut F>,
) -> Result<Outcome, String> {
    let after = cursor.lock().unwrap().clone();
    let mut subscribe = json!({"type":"subscribe","code":code,"ack":true,"ack_window":64});
    if let Some(after) = &after {
        subscribe["resume_after"] = json!(after);
    }
    let (work, pending) = mpsc::channel(64);
    let (done, mut processed) = watch::channel(None);
    let worker = tokio::spawn(process_events(pending, Arc::clone(cursor), done));
    let result = receive(
        socket,
        code.map(|_| subscribe),
        after,
        cursor,
        work,
        &mut processed,
        ready_at,
        shutdown,
    )
    .await;
    // 未处理完的队列交给下次续传；旧连接的 worker 不得在新连接后继续改游标。
    worker.abort();
    let _ = worker.await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn receive<F: Future<Output = std::io::Result<()>>>(
    socket: &mut Socket,
    subscribe: Option<Value>,
    after: Option<EventCursor>,
    cursor: &CursorState,
    work: mpsc::Sender<Work>,
    processed: &mut watch::Receiver<Option<EventCursor>>,
    ready_at: &mut Option<Instant>,
    mut shutdown: Pin<&mut F>,
) -> Result<Outcome, String> {
    if let Some(subscribe) = subscribe {
        let sent = tokio::select! {
            biased;
            result = shutdown.as_mut() => {
                interrupted(result)?;
                return Ok(Outcome::Stopped);
            }
            result = timeout(IO_TIMEOUT, socket.send(Message::text(subscribe.to_string()))) => result,
        };
        if !matches!(sent, Ok(Ok(()))) {
            return Ok(Outcome::Retry {
                retry_after_ms: None,
            });
        }
    }

    let mut last_incoming = Instant::now();
    let mut received: Option<EventCursor> = None;
    loop {
        let incoming = tokio::select! {
            biased;
            result = shutdown.as_mut() => {
                interrupted(result)?;
                return Ok(Outcome::Stopped);
            }
            changed = processed.changed() => {
                if changed.is_err() {
                    return Err("事件处理任务意外结束".into());
                }
                let ack = processed.borrow_and_update().clone().unwrap();
                let message = json!({"type":"ack","stream_id":ack.stream_id,"seq":ack.seq});
                let sent = tokio::select! {
                    biased;
                    result = shutdown.as_mut() => {
                        interrupted(result)?;
                        return Ok(Outcome::Stopped);
                    }
                    result = timeout(IO_TIMEOUT, socket.send(Message::text(message.to_string()))) => result,
                };
                if !matches!(sent, Ok(Ok(()))) {
                    return Ok(Outcome::Retry { retry_after_ms: None });
                }
                continue;
            }
            result = tokio::time::timeout_at(last_incoming + IDLE_TIMEOUT, socket.next()) => result,
        };
        if matches!(&incoming, Ok(Some(Ok(_)))) {
            last_incoming = Instant::now();
        }
        match incoming {
            Ok(Some(Ok(Message::Text(text)))) => {
                let message: ServerMessage =
                    serde_json::from_str(&text).map_err(|_| "桥接服务返回了无效 JSON 消息")?;
                match message {
                    ServerMessage::Ping { nonce } => {
                        if nonce.len() > 256 {
                            return Err("保活 nonce 过长".into());
                        }
                        let pong = serde_json::to_string(&ClientMessage::Pong { nonce }).unwrap();
                        let sent = tokio::select! {
                            biased;
                            result = shutdown.as_mut() => {
                                interrupted(result)?;
                                return Ok(Outcome::Stopped);
                            }
                            result = timeout(IO_TIMEOUT, socket.send(Message::text(pong))) => result,
                        };
                        if !matches!(sent, Ok(Ok(()))) {
                            return Ok(Outcome::Retry {
                                retry_after_ms: None,
                            });
                        }
                    }
                    ServerMessage::Ready {
                        room_id,
                        game_id,
                        stream_id,
                        seq,
                        resumed,
                        ack_window,
                    } => {
                        if received.is_some() || ack_window == 0 || ack_window > 64 {
                            return Err("无效 ready".into());
                        }
                        let base = EventCursor { stream_id, seq };
                        if resumed != after.is_some()
                            || after.as_ref().is_some_and(|old| old != &base)
                        {
                            return Err("服务端未从请求的游标续传".into());
                        }
                        *cursor.lock().unwrap() = Some(base.clone());
                        received = Some(base);
                        *ready_at = Some(Instant::now());
                        tracing::info!(room_id, %game_id, resumed, ack_window, "身份码订阅成功");
                    }
                    ServerMessage::Event {
                        room_id,
                        stream_id,
                        seq,
                        cmd,
                        data,
                    } => {
                        let position = received.as_mut().ok_or("桥接服务在 ready 前发送了事件")?;
                        if position.stream_id != stream_id {
                            return Err("事件流标识发生变化".into());
                        }
                        let completed = cursor.lock().unwrap().clone().unwrap();
                        if seq <= completed.seq {
                            // 同一流已经处理过的事件不重复执行业务，只重发累计 ACK。
                            let ack = json!({"type":"ack","stream_id":completed.stream_id,"seq":completed.seq});
                            if !matches!(
                                timeout(IO_TIMEOUT, socket.send(Message::text(ack.to_string())))
                                    .await,
                                Ok(Ok(()))
                            ) {
                                return Ok(Outcome::Retry {
                                    retry_after_ms: None,
                                });
                            }
                            continue;
                        }
                        if seq != position.seq + 1 {
                            return Err("事件序号不连续，停止以避免漏处理".into());
                        }
                        position.seq = seq;
                        // 有界队列满时重连续传，避免阻塞网络读取而错过 Ping。
                        if work
                            .try_send(Work {
                                cursor: position.clone(),
                                room_id,
                                cmd,
                                data,
                            })
                            .is_err()
                        {
                            return Ok(Outcome::Retry {
                                retry_after_ms: None,
                            });
                        }
                    }
                    ServerMessage::Status { state } => match state.as_str() {
                        "reconnecting" => tracing::warn!("上游正在恢复，保持下游订阅并等待"),
                        "connected" => tracing::info!("上游已恢复，继续接收事件"),
                        _ => tracing::debug!(%state, "忽略未知上游状态"),
                    },
                    ServerMessage::ResumeUnavailable {
                        reason,
                        earliest_after,
                        latest_seq,
                    } => {
                        tracing::error!(%reason, ?earliest_after, ?latest_seq, "续传不可用，不能完整恢复");
                        return Err("续传不可用，检查日志后决定是否重新从实时位置订阅".into());
                    }
                    ServerMessage::Error {
                        code,
                        retry_after_ms,
                    } => {
                        if is_retryable_code(&code) {
                            tracing::warn!(%code, ?retry_after_ms, "暂时拒绝接入，保留游标重试");
                            return Ok(Outcome::Retry { retry_after_ms });
                        }
                        tracing::error!(%code, "桥接服务报告接入错误");
                        return Err("接入失败，请检查身份码和服务端日志".into());
                    }
                    ServerMessage::Closed => {
                        tracing::info!("桥接会话结束");
                        return Ok(Outcome::Finished);
                    }
                    ServerMessage::Unknown => tracing::debug!("忽略未知服务端消息类型"),
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                // 自动 Pong 已排队，flush 驱动它；业务处理在另一个任务，不阻塞这里。
                let flushed = tokio::select! {
                    biased;
                    result = shutdown.as_mut() => {
                        interrupted(result)?;
                        return Ok(Outcome::Stopped);
                    }
                    result = timeout(IO_TIMEOUT, socket.flush()) => result,
                };
                if !matches!(flushed, Ok(Ok(()))) {
                    return Ok(Outcome::Retry {
                        retry_after_ms: None,
                    });
                }
            }
            Ok(Some(Ok(Message::Binary(_)))) => return Err("桥接服务应返回 JSON 文本帧".into()),
            Ok(Some(Ok(Message::Close(_)))) => {
                return Ok(Outcome::Retry {
                    retry_after_ms: None,
                });
            }
            Ok(Some(Ok(_))) => {}
            _ => {
                return Ok(Outcome::Retry {
                    retry_after_ms: None,
                });
            }
        }
    }
}

#[cfg(test)]
#[path = "../downstream/client_test.rs"]
mod client_test;
