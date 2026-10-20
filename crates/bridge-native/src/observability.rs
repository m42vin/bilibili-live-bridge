//! 独立管理监听器上的存活、就绪检查与固定标签指标，不调用真实平台探测。
//!
//! 管理入口只读取 Manager 状态，不占用业务连接名额；启动时业务监听器应已绑定。
//! 退出期间保留管理入口，直到下游服务完成清理或退出预算耗尽。

use std::{future::Future, io, time::Duration};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::session::Manager;

mod metrics;
pub(crate) use metrics::{Counters, Fault, Operation, Outcome, Snapshot};

const MAX_CONNECTIONS: usize = 32;
const MAX_HEADER_BYTES: usize = 8192;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

/// 返回当前管理器的 Prometheus 文本指标，不触发网络或日志淘汰。
///
/// 标签仅使用固定类别。不同组件分别取快照，不保证跨组件原子一致。
pub fn metrics(manager: &Manager) -> String {
    let mut text = manager.render_metrics();
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        text.push_str(&format!(
            "# TYPE bridge_runtime_alive_tasks gauge\nbridge_runtime_alive_tasks {}\n",
            runtime.metrics().num_alive_tasks()
        ));
    }
    text
}

/// 在已绑定的独立监听器上提供 GET /livez、/readyz 和 /metrics。
///
/// /livez 只表示管理入口能响应；/readyz 在管理器禁止接入后返回 503，
/// 上游暂时故障和资源暂满只由指标呈现。调用前应已绑定业务监听器并完成初始化。
/// 每次连接处理一个请求后关闭，最多 32 条并发、8 KiB 请求头、32 个头字段，
/// 读取与发送各最多等待两秒；并发满时关闭新连接，不排队。
/// 退出信号到达时中止管理请求，不关闭传入管理器。
///
/// # Errors
///
/// 监听失败时返回 I/O 错误，调用方应停止业务服务，避免失去运维入口后继续运行。
pub async fn serve(
    listener: TcpListener,
    manager: Manager,
    shutdown: impl Future<Output = ()>,
) -> io::Result<()> {
    let mut tasks = JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::error!(%error, "管理请求任务失败");
                    return Err(io::Error::other(error));
                }
            }
            incoming = listener.accept() => {
                let (stream, _) = incoming?;
                if tasks.len() < MAX_CONNECTIONS {
                    let manager = manager.clone();
                    tasks.spawn(async move { let _ = respond(stream, &manager).await; });
                }
            }
        }
    }
}

async fn respond(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    manager: &Manager,
) -> io::Result<()> {
    let request = timeout(IO_TIMEOUT, read_request(&mut stream)).await;
    let (status, body, content_type) = match request {
        Ok(Ok(bytes)) => route(&bytes, manager),
        Ok(Err(_)) => (
            "400 Bad Request",
            "bad request\n".into(),
            "text/plain; charset=utf-8",
        ),
        Err(_) => return Ok(()),
    };
    let allow = if status.starts_with("405") {
        "Allow: GET\r\n"
    } else {
        ""
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n{allow}\r\n{body}",
        body.len()
    );
    timeout(IO_TIMEOUT, stream.write_all(response.as_bytes())).await??;
    Ok(())
}

async fn read_request(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 || bytes.len() + count > MAX_HEADER_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "管理请求头不完整或过大",
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            return Ok(bytes);
        }
    }
}

fn route(bytes: &[u8], manager: &Manager) -> (&'static str, String, &'static str) {
    const TEXT: &str = "text/plain; charset=utf-8";
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut headers);
    if !matches!(request.parse(bytes), Ok(httparse::Status::Complete(_))) {
        return ("400 Bad Request", "bad request\n".into(), TEXT);
    }
    if request.method != Some("GET") {
        return (
            "405 Method Not Allowed",
            "method not allowed\n".into(),
            TEXT,
        );
    }
    match request.path {
        Some("/livez") => ("200 OK", "ok\n".into(), TEXT),
        Some("/readyz") if manager.is_shutting_down() => {
            ("503 Service Unavailable", "draining\n".into(), TEXT)
        }
        Some("/readyz") => ("200 OK", "ready\n".into(), TEXT),
        Some("/metrics") => (
            "200 OK",
            metrics(manager),
            "text/plain; version=0.0.4; charset=utf-8",
        ),
        _ => ("404 Not Found", "not found\n".into(), TEXT),
    }
}

#[cfg(test)]
#[path = "observability_test.rs"]
mod observability_test;
