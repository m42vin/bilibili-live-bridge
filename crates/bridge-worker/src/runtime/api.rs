//! 平台签名请求与上游握手，负责单次操作的超时和响应大小限制。

use bridge_core::open_live::{
    auth,
    error::{ErrorCode, ResponseError},
    model,
};
use serde::Serialize;
use serde_json::Value;
use worker::*;

use super::{app_id, deadline, error, now, random_id, read_body};

/// 对同一份序列化正文签名并发送，结束已失效场次视为清理完成。
pub(super) async fn execute(env: &Env, path: &str, body: &impl Serialize) -> Result<Value> {
    let body = serde_json::to_string(body)?;
    let signature = auth::sign(
        &env.secret("BILIBILI_ACCESS_KEY_ID")?.to_string(),
        &env.secret("BILIBILI_ACCESS_KEY_SECRET")?.to_string(),
        &body,
        now() / 1000,
        &random_id(),
    );

    let headers = Headers::new();
    for (key, value) in [
        ("Accept", "application/json"),
        ("Content-Type", "application/json"),
        (
            "x-bili-accesskeyid",
            &env.secret("BILIBILI_ACCESS_KEY_ID")?.to_string(),
        ),
        ("x-bili-content-md5", &signature.content_md5),
        ("x-bili-signature-method", auth::SIGNATURE_METHOD),
        ("x-bili-signature-version", auth::SIGNATURE_VERSION),
        ("x-bili-timestamp", &signature.timestamp),
        ("x-bili-signature-nonce", &signature.nonce),
        ("Authorization", &signature.authorization),
    ] {
        headers.set(key, value)?;
    }
    let req = Request::new_with_init(
        &format!("{}{path}", origin()),
        RequestInit::new()
            .with_method(Method::Post)
            .with_headers(headers)
            .with_redirect(RequestRedirect::Manual)
            .with_body(Some(body.into())),
    )?;

    let started = now();
    let mut stage = "fetch";
    let mut http_status = None;
    let mut platform_code = None;
    if path == "/v2/app/start" {
        console_log!("platform request started path={}", path);
    }
    let controller = AbortController::default();
    let result = deadline(10_000, async {
        let mut response = Fetch::Request(req)
            .send_with_signal(&controller.signal())
            .await?;
        http_status = Some(response.status_code());
        stage = "http_status";
        if response.status_code() != 200 {
            return Err(error("platform HTTP error"));
        }
        stage = "response_body";
        let bytes = read_body(response.stream()?, 1_048_576).await?;
        stage = "platform_response";
        match model::success_value(&bytes) {
            Ok(value) => Ok(value),
            Err(ResponseError::Platform { code, .. })
                if path == "/v2/app/end"
                    && matches!(code, ErrorCode::NotInGame | ErrorCode::HeartbeatExpired) =>
            {
                Ok(Value::Null)
            }
            Err(ResponseError::Platform { code, .. }) => {
                platform_code = Some(code.raw());
                Err(error("platform response error"))
            }
            Err(_) => Err(error("platform response error")),
        }
    })
    .await;
    // 成功后响应已读完；超时或失败时同时终止尚未完成的底层请求。
    controller.abort();
    if let Err(ref failure) = result {
        let timed_out =
            matches!(failure, Error::RustError(message) if message == "operation timed out");
        // 仅输出固定阶段、数值状态和耗时；底层错误及平台 message 可能含敏感输入。
        console_error!(
            "platform request failed path={} stage={} timed_out={} http_status={:?} platform_code={:?} elapsed_ms={}",
            path,
            stage,
            timed_out,
            http_status,
            platform_code,
            now().saturating_sub(started)
        );
    } else if path == "/v2/app/start" {
        console_log!(
            "platform request completed path={} elapsed_ms={}",
            path,
            now().saturating_sub(started)
        );
    }
    result
}

/// 结束已知场次；远端已结束或心跳失效也视为完成。
pub(super) async fn end(env: &Env, game: &str) -> Result<()> {
    execute(
        env,
        "/v2/app/end",
        &model::EndRequest {
            app_id: app_id(env)?,
            game_id: game,
        },
    )
    .await?;
    Ok(())
}

/// 为单个场次发送项目心跳，不在低层隐式重试。
pub(super) async fn heartbeat(env: &Env, game: &str) -> Result<()> {
    execute(
        env,
        "/v2/app/heartbeat",
        &model::HeartbeatRequest { game_id: game },
    )
    .await?;
    Ok(())
}

/// 为上游连接执行限时握手，失败时取消请求，避免遗留无人观察的连接。
pub(super) async fn connect(mut url: Url) -> Result<WebSocket> {
    let scheme = if url.scheme() == "wss" {
        "https"
    } else {
        "http"
    };
    url.set_scheme(scheme)
        .map_err(|_| error("invalid upstream URL"))?;
    let headers = Headers::new();
    headers.set("Upgrade", "websocket")?;
    let req = Request::new_with_init(
        url.as_str(),
        RequestInit::new()
            .with_headers(headers)
            .with_redirect(RequestRedirect::Manual),
    )?;

    let controller = AbortController::default();
    let result = deadline(10_000, async {
        Fetch::Request(req)
            .send_with_signal(&controller.signal())
            .await?
            .websocket()
            .ok_or_else(|| error("upstream refused upgrade"))
    })
    .await;
    if result.is_err() {
        controller.abort();
    }
    result
}

// 本地集成构建通过此单一注入点替换平台地址；正式构建不得设置该变量。
// 地址参与生产请求构造，无法仅放在测试模块中。
fn origin() -> &'static str {
    option_env!("BRIDGE_WORKER_TEST_API_ORIGIN").unwrap_or("https://live-open.biliapi.com")
}
