//! 对外 HTTP 路由与对象间调用的公共基础设施。
//! 入口只分发接入与 WebSocket 请求，场次所有权由对应对象维护。

use std::{future::Future, time::Duration};

use bridge_core::downstream::admission::{AdmissionFailure, AdmissionRequest};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use worker::wasm_bindgen::prelude::*;
use worker::*;

mod admission;
mod api;
mod room;

pub use admission::Admission;
pub use room::Room;

#[wasm_bindgen(inline_js = "export function random_id() { return crypto.randomUUID(); }")]
extern "C" {
    fn random_id() -> String;
}

/// 处理公网入口请求，失败时仅记录固定摘要，避免泄露请求中的凭证。
#[event(fetch)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    match route(req, env).await {
        Ok(r) => Ok(r),
        Err(_) => {
            console_error!("bridge request failed");
            failure(503, "server_busy")
        }
    }
}

async fn route(mut req: Request, env: Env) -> Result<Response> {
    let url = req.url()?;
    match (req.method(), url.path()) {
        (Method::Get, "/health") => Response::ok("ok"),
        (Method::Post, "/admit") => {
            let subscription = match json_body::<AdmissionRequest>(&mut req)
                .await
                .and_then(|s| s.validate().map_err(error))
            {
                Ok(s) => s,
                Err(_) => return failure(400, "invalid_request"),
            };
            let name = format!("{}:{}", app_id(&env)?, hash(&subscription.code));
            let body = serde_json::json!({
                "subscription": subscription,
                "origin": url.origin().ascii_serialization(),
            });
            call(&env, "ADMISSIONS", &name, "/admit", &body).await
        }
        (Method::Get, path) if path.starts_with("/ws/") => {
            let room = &path[4..];
            let Some(room_id) = room.parse::<i64>().ok().filter(|r| *r > 0) else {
                return failure(400, "invalid_request");
            };
            let name = format!("{}:{room_id}", app_id(&env)?);
            env.durable_object("ROOMS")?
                .get_by_name(&name)?
                .fetch_with_request(req)
                .await
        }
        _ => Response::error("not found", 404),
    }
}

fn now() -> u64 {
    Date::now().as_millis()
}

fn error(message: &str) -> Error {
    Error::RustError(message.into())
}

fn hash(code: &str) -> String {
    Sha256::digest(code.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

async fn deadline<T>(ms: u64, future: impl Future<Output = Result<T>>) -> Result<T> {
    futures_util::pin_mut!(future);
    match select(future, Box::pin(Delay::from(Duration::from_millis(ms)))).await {
        Either::Left((result, _)) => result,
        Either::Right(_) => Err(error("operation timed out")),
    }
}

async fn read_body(mut stream: ByteStream, max: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len().saturating_add(chunk.len()) > max {
            return Err(error("body too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn json_body<T: DeserializeOwned>(req: &mut Request) -> Result<T> {
    let bytes = deadline(10_000, read_body(req.stream()?, 8192)).await?;
    serde_json::from_slice(&bytes).map_err(|_| error("invalid JSON"))
}

fn failure(status: u16, code: &str) -> Result<Response> {
    let retry = matches!(status, 429 | 503);
    let mut response = Response::from_json(&AdmissionFailure::Error {
        code: code.into(),
        message: code.into(),
        retry_after_ms: retry.then_some(2000),
    })?
    .with_status(status);
    response.headers_mut().set("Cache-Control", "no-store")?;
    Ok(response)
}

fn unavailable(reason: &str, earliest: Option<u64>, tail: Option<u64>) -> Result<Response> {
    Response::from_json(&AdmissionFailure::ResumeUnavailable {
        reason: reason.into(),
        earliest_after: earliest,
        latest_seq: tail,
    })
    .map(|r| r.with_status(409))
}

async fn call(
    env: &Env,
    binding: &str,
    name: &str,
    path: &str,
    body: &impl Serialize,
) -> Result<Response> {
    let request = Request::new_with_init(
        &format!("https://internal{path}"),
        RequestInit::new()
            .with_method(Method::Post)
            .with_body(Some(serde_json::to_string(body)?.into())),
    )?;
    env.durable_object(binding)?
        .get_by_name(name)?
        .fetch_with_request(request)
        .await
}

fn app_id(env: &Env) -> Result<i64> {
    env.var("BILIBILI_APP_ID")?
        .to_string()
        .parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| error("invalid app id"))
}
