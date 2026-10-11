//! 下游客户端示例：身份码订阅、事件与状态处理、断线后重新订阅。
//!
//! 先启动 `server`，再设置 `AUTH_CODE` 并运行本程序；无需平台 Access Key。
//! `BRIDGE_URL` 默认为 `ws://127.0.0.1:8080/ws`。网络断开按 2–10 秒退避重连；
//! `closed`、服务端 `error` 或无效业务消息结束程序。Ctrl-C 在连接和退避期间均有效。

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
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
        let request = url
            .as_str()
            .into_client_request()
            .map_err(|_| "BRIDGE_URL 必须是有效的 ws:// 或 wss:// 地址")?;
        if !matches!(request.uri().scheme_str(), Some("ws" | "wss")) {
            return Err("BRIDGE_URL 必须使用 ws:// 或 wss://".into());
        }
        Ok(Self { url, code })
    }
}

// 独立于服务端的 Rust 类型；未知消息类型和事件命令允许客户端向前兼容。
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerMessage {
    Ready {
        room_id: i64,
        game_id: String,
    },
    Event {
        room_id: i64,
        cmd: String,
        data: Value,
    },
    Status {
        state: String,
    },
    Lagged {
        skipped: u64,
    },
    Error {
        code: String,
    },
    Closed,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Retry,
    Finished,
    Stopped,
}

async fn run_client(
    settings: &Settings,
    shutdown: impl Future<Output = std::io::Result<()>>,
) -> Result<(), String> {
    let mut shutdown = std::pin::pin!(shutdown);
    let mut delay = RETRY_INITIAL;
    loop {
        tracing::info!("连接直播桥");
        let connected = tokio::select! {
            biased;
            result = shutdown.as_mut() => return interrupted(result),
            result = timeout(IO_TIMEOUT, connect_async(settings.url.as_str())) => result,
        };
        let mut ready_at = None;
        match connected {
            Ok(Ok((mut socket, _))) => {
                let outcome = listen(
                    &mut socket,
                    &settings.code,
                    &mut ready_at,
                    shutdown.as_mut(),
                )
                .await;
                // 有限等待关闭应答；异常连接也不会阻塞后续重连。
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
                    Outcome::Retry => {}
                }
            }
            Ok(Err(Error::Http(response)))
                if response.status().is_client_error() && response.status().as_u16() != 429 =>
            {
                return Err(format!(
                    "桥接服务拒绝握手，HTTP {}，请检查 BRIDGE_URL 或访问权限",
                    response.status().as_u16()
                ));
            }
            _ => tracing::warn!("握手失败或超时"),
        }
        // 短暂 ready 后立即断开不清零退避，避免反复故障时密集接入。
        if ready_at.is_some_and(|at: Instant| at.elapsed() >= STABLE_TIME) {
            delay = RETRY_INITIAL;
        }
        tracing::warn!(
            retry_secs = delay.as_secs(),
            "下游断开，等待重新连接并订阅；空窗事件不会补发"
        );
        tokio::select! {
            biased;
            result = shutdown.as_mut() => return interrupted(result),
            _ = sleep(delay) => {}
        }
        delay = (delay * 2).min(RETRY_MAX);
    }
}

fn interrupted(result: std::io::Result<()>) -> Result<(), String> {
    result.map_err(|_| "无法监听 Ctrl-C".to_owned())?;
    tracing::info!("收到中断，退出客户端");
    Ok(())
}

async fn listen<F: Future<Output = std::io::Result<()>>>(
    socket: &mut Socket,
    code: &str,
    ready_at: &mut Option<Instant>,
    mut shutdown: Pin<&mut F>,
) -> Result<Outcome, String> {
    let subscribe = json!({"type": "subscribe", "code": code});
    let sent = tokio::select! {
        biased;
        result = shutdown.as_mut() => { interrupted(result)?; return Ok(Outcome::Stopped); }
        result = timeout(IO_TIMEOUT, socket.send(Message::text(subscribe.to_string()))) => result,
    };
    if !matches!(sent, Ok(Ok(()))) {
        tracing::warn!("发送订阅失败或超时");
        return Ok(Outcome::Retry);
    }
    loop {
        let incoming = tokio::select! {
            biased;
            result = shutdown.as_mut() => { interrupted(result)?; return Ok(Outcome::Stopped); }
            result = timeout(IDLE_TIMEOUT, socket.next()) => result,
        };
        match incoming {
            Ok(Some(Ok(Message::Text(text)))) => {
                let message: ServerMessage = serde_json::from_str(&text)
                    .map_err(|_| "桥接服务返回了无效 JSON 消息".to_owned())?;
                if handle_message(message, ready_at)? {
                    return Ok(Outcome::Finished);
                }
            }
            Ok(Some(Ok(Message::Ping(_)))) => {
                // tungstenite 已排入相同负载的 Pong；flush 驱动应答，无需手动重复发送。
                let flushed = tokio::select! {
                    biased;
                    result = shutdown.as_mut() => { interrupted(result)?; return Ok(Outcome::Stopped); }
                    result = timeout(IO_TIMEOUT, socket.flush()) => result,
                };
                if !matches!(flushed, Ok(Ok(()))) {
                    return Ok(Outcome::Retry);
                }
            }
            Ok(Some(Ok(Message::Close(_)))) => {
                // 应答由统一清理驱动。仅收到关闭帧仍重连，业务终止由 closed/error 表示。
                return Ok(Outcome::Retry);
            }
            Ok(Some(Ok(Message::Binary(_)))) => return Err("桥接服务应返回 JSON 文本帧".into()),
            Ok(Some(Ok(_))) => {}
            _ => {
                tracing::warn!("读取下游失败、连接结束或 90 秒无入站活动");
                return Ok(Outcome::Retry);
            }
        }
    }
}

fn handle_message(message: ServerMessage, ready_at: &mut Option<Instant>) -> Result<bool, String> {
    match message {
        ServerMessage::Ready { room_id, game_id } => {
            ready_at.get_or_insert_with(Instant::now);
            tracing::info!(room_id, %game_id, "身份码订阅成功");
        }
        ServerMessage::Event { room_id, cmd, data } => {
            if ready_at.is_none() {
                return Err("桥接服务在 ready 前发送了事件".into());
            }
            tracing::info!(room_id, %cmd, "收到直播事件");
            // 在这里接入业务处理；避免长时间阻塞接收循环，以免延迟 Pong。
            tracing::debug!(%data, "直播事件内容");
        }
        ServerMessage::Status { state } => match state.as_str() {
            "reconnecting" => tracing::warn!("上游正在恢复，保持下游订阅并等待"),
            "connected" => tracing::info!("上游已恢复，继续接收事件"),
            _ => tracing::debug!(%state, "忽略未知上游状态"),
        },
        ServerMessage::Lagged { skipped } => {
            tracing::warn!(skipped, "消费落后，事件已丢失，继续接收保留的事件")
        }
        ServerMessage::Error { code } => {
            // 不自动重试接入错误，避免身份码无效时反复触发平台 start。
            tracing::error!(%code, "桥接服务报告错误，结束客户端");
            return Err("接入失败，请检查身份码和服务端日志".into());
        }
        ServerMessage::Closed => {
            tracing::info!("桥接会话结束");
            return Ok(true);
        }
        ServerMessage::Unknown => tracing::debug!("忽略未知服务端消息类型"),
    }
    Ok(false)
}

#[cfg(test)]
#[path = "../downstream/client_test.rs"]
mod client_test;
