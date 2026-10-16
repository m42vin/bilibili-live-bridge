//! 项目心跳分类、恢复期限与单请求调度；所有关闭仍交给会话清理路径。

use std::collections::HashSet;
use std::sync::{Arc, Weak};
use std::time::Duration;

pub use bridge_core::session::HeartbeatPolicy;
use bridge_core::session::failure::{self, ApiFailure, FailureKind};
use bridge_core::session::recovery::{HeartbeatDecision, HeartbeatOutcome};
use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};

use super::{Session, State, Stop, lock, until_idle};
use crate::open_live::api::{BatchHeartbeatResult, MAX_BATCH_HEARTBEAT, REQUEST_TIMEOUT};
use crate::open_live::error::ApiError;
use crate::session::platform::BoxFuture;
use crate::session::recovery::NativeMoment;

fn classify(error: &ApiError, batch_size: usize) -> FailureKind {
    let failure = match error {
        ApiError::Invalid { .. } => ApiFailure::Invalid,
        ApiError::Transport { .. } => ApiFailure::Transport,
        ApiError::Decode { .. } => ApiFailure::Decode,
        ApiError::Status { status } => ApiFailure::HttpStatus(*status),
        ApiError::Platform { code, .. } => ApiFailure::Platform(*code),
    };
    failure::classify(failure, batch_size)
}

fn jitter_percent() -> u32 {
    let random = uuid::Uuid::new_v4();
    let bytes = random.as_bytes();
    80 + u32::from(u16::from_be_bytes([bytes[0], bytes[1]])) % 41
}

#[derive(Clone)]
struct PendingBeat {
    game_id: String,
    session: Weak<Session>,
}

enum BatchOutcome {
    Response(Result<BatchHeartbeatResult, ApiError>),
    TimedOut,
}

struct Request {
    pending: Vec<PendingBeat>,
    future: BoxFuture<'static, BatchOutcome>,
}

fn request(state: &State, pending: Vec<PendingBeat>) -> Request {
    let platform = Arc::clone(&state.platform);
    let ids: Vec<String> = pending.iter().map(|beat| beat.game_id.clone()).collect();
    Request {
        pending,
        future: Box::pin(async move {
            match timeout(REQUEST_TIMEOUT, platform.batch_heartbeat(&ids)).await {
                Ok(response) => BatchOutcome::Response(response),
                Err(_) => BatchOutcome::TimedOut,
            }
        }),
    }
}

struct Closing {
    beat: PendingBeat,
    reason: &'static str,
    failures: u32,
}

fn close_sessions(closing: Vec<Closing>) {
    for close in closing {
        if let Some(session) = close.beat.session.upgrade() {
            tracing::warn!(
                game_id = %close.beat.game_id,
                room_id = ?session.room_id(),
                reason = close.reason,
                failures = close.failures,
                "项目心跳失效，关闭会话"
            );
            session.begin_close();
        }
    }
}

fn expire_recoveries(state: &State) {
    let now = Instant::now();
    let closing = {
        let mut beats = lock(&state.beats);
        let closing: Vec<_> = beats
            .iter()
            .filter(|(_, slot)| slot.recovery.expired(NativeMoment(now)))
            .map(|(game_id, slot)| Closing {
                beat: PendingBeat {
                    game_id: game_id.clone(),
                    session: slot.session.clone(),
                },
                reason: "recovery_budget_exhausted",
                failures: slot.recovery.failures(),
            })
            .collect();
        for close in &closing {
            beats.remove(&close.beat.game_id);
        }
        closing
    };
    close_sessions(closing);
}

fn finish_request(state: &State, pending: &[PendingBeat], outcome: BatchOutcome) {
    let now = Instant::now();
    let (kind, platform_code, http_status) = match &outcome {
        BatchOutcome::Response(Ok(result)) => {
            tracing::debug!(
                batch_size = pending.len(),
                failed_count = result.failed_game_ids().len(),
                "会话批量心跳完成"
            );
            (None, None, None)
        }
        BatchOutcome::Response(Err(error)) => (
            Some(classify(error, pending.len())),
            error.platform_code().map(|code| code.raw()),
            match error {
                ApiError::Status { status } => Some(*status),
                _ => None,
            },
        ),
        BatchOutcome::TimedOut => {
            tracing::warn!("批量项目心跳请求超时");
            (Some(FailureKind::Retryable), None, None)
        }
    };
    if let Some(kind) = kind {
        // 不记录平台 message 或请求正文；诊断只使用允许公开的分类、码和状态。
        tracing::warn!(
            batch_size = pending.len(),
            error_category = kind.label(),
            ?platform_code,
            ?http_status,
            "会话批量心跳失败"
        );
        match kind {
            FailureKind::AccessLimited => {
                tracing::warn!("平台访问限制可能来自权限或请求频率，降低心跳重试频率");
            }
            FailureKind::Unknown => {
                tracing::warn!(?platform_code, "未知平台心跳错误码，进入有限恢复");
            }
            _ => {}
        }
    }
    let failed: HashSet<&str> = match &outcome {
        BatchOutcome::Response(Ok(result)) => result
            .failed_game_ids()
            .iter()
            .map(String::as_str)
            .collect(),
        _ => HashSet::new(),
    };
    let jitter = jitter_percent();
    let mut closing = Vec::new();
    {
        let mut beats = lock(&state.beats);
        for beat in pending {
            let Some(slot) = beats.get_mut(&beat.game_id) else {
                continue;
            };
            if !Weak::ptr_eq(&slot.session, &beat.session) {
                continue;
            }
            slot.in_flight = false;
            let outcome = if failed.contains(beat.game_id.as_str()) {
                HeartbeatOutcome::FailedGame
            } else if let Some(kind) = kind {
                HeartbeatOutcome::Failure(kind)
            } else {
                HeartbeatOutcome::Confirmed
            };
            let reason = match slot.recovery.complete(NativeMoment(now), outcome, jitter) {
                HeartbeatDecision::Close(reason) => Some(reason.label()),
                HeartbeatDecision::Retry { delay, deadline } => {
                    slot.normal_due = None;
                    tracing::warn!(
                        game_id = %beat.game_id,
                        error_category = kind.expect("重试只可能来自请求失败").label(),
                        ?platform_code,
                        failures = slot.recovery.failures(),
                        retry_delay_secs = delay.as_secs_f64(),
                        remaining_budget_secs = deadline.0.duration_since(now).as_secs_f64(),
                        "项目心跳等待重试"
                    );
                    None
                }
                HeartbeatDecision::Confirmed { previous_failures } => {
                    if previous_failures > 0 {
                        tracing::info!(game_id = %beat.game_id, failures = previous_failures, "项目心跳已恢复");
                    }
                    None
                }
            };
            if let Some(reason) = reason {
                closing.push(Closing {
                    beat: beat.clone(),
                    reason,
                    failures: slot.recovery.failures(),
                });
            }
        }
        for close in &closing {
            beats.remove(&close.beat.game_id);
        }
    }
    close_sessions(closing);
    state.heartbeat_wake.notify_one();
}

fn mark_normal_due(state: &State) {
    let now = Instant::now();
    for slot in lock(&state.beats).values_mut() {
        if slot.recovery.retry_at().is_none() && !slot.in_flight {
            slot.normal_due.get_or_insert(now);
        }
    }
}

fn take_batch(state: &State) -> Vec<PendingBeat> {
    let now = Instant::now();
    let mut beats = lock(&state.beats);
    let mut normal = Vec::new();
    let mut retry = Vec::new();
    for (id, slot) in beats.iter() {
        if slot.in_flight {
            continue;
        }
        if let Some(at) = slot.recovery.retry_at().map(|at| at.0) {
            if at <= now {
                retry.push((slot.recovery.deadline().0, at, id.clone()));
            }
        } else if let Some(at) = slot.normal_due {
            normal.push((at, at, id.clone()));
        }
    }
    // 未发出的正常批次保留最初到期时间，下一轮 tick 不会让它们被新批次插队。
    let mut selected = if normal.is_empty() { retry } else { normal };
    selected.sort_unstable();
    selected
        .into_iter()
        .take(MAX_BATCH_HEARTBEAT)
        .map(|(_, _, id)| {
            let slot = beats.get_mut(&id).unwrap();
            slot.in_flight = true;
            slot.normal_due = None;
            PendingBeat {
                game_id: id,
                session: slot.session.clone(),
            }
        })
        .collect()
}

fn next_timer(state: &State, in_flight: bool) -> Option<Instant> {
    lock(&state.beats)
        .values()
        .filter_map(|slot| slot.recovery.next_deadline(in_flight).map(|at| at.0))
        .min()
}

fn request_is_live(state: &State, request: &Request) -> bool {
    let beats = lock(&state.beats);
    request.pending.iter().any(|beat| {
        beats
            .get(&beat.game_id)
            .is_some_and(|slot| Weak::ptr_eq(&slot.session, &beat.session))
    })
}

async fn receive(request: &mut Option<Request>) -> BatchOutcome {
    match request {
        Some(request) => request.future.as_mut().await,
        None => std::future::pending().await,
    }
}

struct Finished(Weak<State>);

impl Drop for Finished {
    fn drop(&mut self) {
        if let Some(state) = self.0.upgrade() {
            state.mark_heartbeat_finished();
        }
    }
}

/// 调度批量项目心跳及有限恢复；停止时取消在途请求并通知收尾。
pub(super) async fn run_heartbeats(state: Weak<State>, stop: Stop, every: Duration) {
    let _finished = Finished(state.clone());
    // 直接从第一轮正常截止时间开始，避免初始立即 tick 与唤醒注册发生竞争。
    let mut normal_at = Instant::now() + every;
    let mut ticker = interval_at(normal_at, every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut active: Option<Request> = None;
    loop {
        let Some(current) = state.upgrade() else {
            return;
        };
        if stop.is_cancelled() {
            return;
        }
        // 先登记通知，再采样状态。Owned 通知允许等待时释放管理器强引用。
        let wake = Arc::clone(&current.heartbeat_wake).notified_owned();
        let mut wake = std::pin::pin!(wake);
        wake.as_mut().enable();
        expire_recoveries(&current);
        // 重试计时器与正常 tick 同时到期时，先收取正常 tick 再选择批次。
        if Instant::now() >= normal_at {
            mark_normal_due(&current);
            normal_at = Instant::now() + every;
            // 时钟已到截止时间，即使 timer wheel 尚未发布就绪，也先处理正常批次。
            ticker.reset_at(normal_at);
        }
        if active
            .as_ref()
            .is_some_and(|request| !request_is_live(&current, request))
        {
            active = None;
        }
        if active.is_none() {
            let pending = take_batch(&current);
            if !pending.is_empty() {
                active = Some(request(&current, pending));
            }
        }
        let timer = next_timer(&current, active.is_some());
        let in_flight = active.is_some();
        drop(current);
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = until_idle(timer) => {}
            result = receive(&mut active), if in_flight => {
                let request = active.take().unwrap();
                let Some(current) = state.upgrade() else { return; };
                // 截止时刻收到结果也不能使已经耗尽恢复期限的场次复活。
                expire_recoveries(&current);
                finish_request(&current, &request.pending, result);
            }
            _ = ticker.tick() => {
                let Some(current) = state.upgrade() else { return; };
                mark_normal_due(&current);
                normal_at = Instant::now() + every;
                ticker.reset_at(normal_at);
            }
            _ = &mut wake => {}
        }
    }
}

#[cfg(test)]
#[path = "heartbeat_test.rs"]
mod heartbeat_test;
