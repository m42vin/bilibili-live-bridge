//! 按身份码串行接入，并持久保存尚未移交给房间的场次清理责任。

use std::time::Duration;

use bridge_core::open_live::model::{StartRequest, start_result_from_value};
use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use worker::*;

use super::{
    AdmissionRequest, api, app_id, call, error, failure, hash, json_body, now, unavailable,
};

#[derive(Deserialize)]
struct Input {
    subscription: AdmissionRequest,
    origin: String,
}

#[derive(Serialize, Deserialize)]
struct Pending {
    room: String,
    game: String,
    candidate: Value,
}

/// 同一身份码的接入协调器，持久状态用于复用房间和补偿场次清理。
#[durable_object]
pub struct Admission {
    state: State,
    env: Env,
    // 跨存储和网络等待串行化接入与清理；fetch 忙时拒绝，alarm 等待已有操作完成。
    gate: Mutex<()>,
}

impl DurableObject for Admission {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            gate: Mutex::new(()),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let Some(_guard) = self.gate.try_lock() else {
            return failure(503, "server_busy");
        };
        let input: Input = json_body(&mut req).await?;
        let sub = input.subscription.validate().map_err(error)?;
        self.reconcile().await?;
        let code_hash = hash(&sub.code);

        let request = json!({
            "code_hash": code_hash,
            "ack": sub.ack,
            "ack_window": sub.ack_window,
            "resume_after": sub.resume_after,
            "origin": input.origin,
        });
        if let Some(room) = self.state.storage().get::<String>("room").await? {
            let response = call(&self.env, "ROOMS", &room, "/ticket", &request).await?;
            if response.status_code() != 410 {
                return Ok(response);
            }
        }
        if sub.resume_after.is_some() {
            return unavailable("stream_unavailable", None, None);
        }

        // start 失败时远端结果可能未知，先持久化冷却期，防止立即重试重复开场。
        let allowed = self
            .state
            .storage()
            .get::<u64>("start_after")
            .await?
            .unwrap_or(0);
        if now() < allowed {
            return failure(429, "rate_limited");
        }
        self.state
            .storage()
            .put("start_after", now() + 60_000)
            .await?;

        let candidate = match api::execute(
            &self.env,
            "/v2/app/start",
            &StartRequest {
                code: &sub.code,
                app_id: app_id(&self.env)?,
            },
        )
        .await
        {
            Ok(value) => value,
            Err(_) => return failure(502, "attach_failed"),
        };
        let started = start_result_from_value(candidate.clone())
            .map_err(|_| error("invalid start response"))?;
        let room = format!("{}:{}", app_id(&self.env)?, started.anchor().room_id);
        let pending = Pending {
            room: room.clone(),
            game: started.game_id().into(),
            candidate,
        };
        self.state
            .storage()
            .set_alarm(Duration::from_secs(10))
            .await?;
        self.state.storage().put("pending", &pending).await?;

        // Room 持久化所有权后才确认；响应丢失时通过 /owns 复核清理责任。
        let mut adopted = call(
            &self.env,
            "ROOMS",
            &room,
            "/prepare",
            &json!({"candidate":pending.candidate,"code_hash":code_hash}),
        )
        .await?;
        if adopted.status_code() != 200 {
            return Ok(adopted);
        }
        let result: Value = adopted.json().await?;
        if result["owned"] != true {
            api::end(&self.env, &pending.game).await?;
        }

        self.state.storage().put("room", &room).await?;
        self.state.storage().delete("pending").await?;
        self.state.storage().delete_alarm().await?;
        call(&self.env, "ROOMS", &room, "/ticket", &request).await
    }

    async fn alarm(&self) -> Result<Response> {
        let _guard = self.gate.lock().await;
        // I/O 前重新设置 alarm，避免平台有限次数重试耗尽后丢失清理机会。
        self.state
            .storage()
            .set_alarm(Duration::from_secs(10))
            .await?;
        self.reconcile().await?;
        Response::ok("ok")
    }
}

impl Admission {
    async fn reconcile(&self) -> Result<()> {
        if let Some(pending) = self.state.storage().get::<Pending>("pending").await? {
            let mut response = call(
                &self.env,
                "ROOMS",
                &pending.room,
                "/owns",
                &json!({"game":pending.game}),
            )
            .await?;
            if response.status_code() != 200 {
                return Err(error("ownership unknown"));
            }
            let value: Value = response.json().await?;
            if value["owned"] != true {
                api::end(&self.env, &pending.game).await?;
            } else {
                self.state.storage().put("room", &pending.room).await?;
            }
            self.state.storage().delete("pending").await?;
        }
        self.state.storage().delete_alarm().await?;
        Ok(())
    }
}
