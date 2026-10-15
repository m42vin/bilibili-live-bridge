//! 项目心跳分类、恢复期限与单请求调度；所有关闭仍交给会话清理路径。

use std::collections::HashSet;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::time::{Instant, MissedTickBehavior, interval_at, timeout};

use super::{Session, State, Stop, lock, until_idle};
use crate::open_live::api::{BatchHeartbeatResult, MAX_BATCH_HEARTBEAT, REQUEST_TIMEOUT};
use crate::open_live::error::ApiError;
use crate::session::platform::BoxFuture;

const DEFAULT_FAILURE_TIMEOUT: Duration = Duration::from_secs(45);

/// 项目心跳的有限恢复策略。
///
/// 默认从最后确认成功起保留 45 秒，失败后以 2 秒起、10 秒封顶的指数退避重试。
/// 退避加入 ±20% 抖动；访问限制的延迟至少为正常项目心跳间隔。
/// 首次登记作为初始成功时间；恢复期限仅在首次失败后启用。
///
/// 默认预算按最短 60 秒平台失效窗口，为请求响应延迟和安全余量留出 15 秒。
/// 不安全的组合仅警告，不修改参数；自定义参数不能保证在平台失效前恢复。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    /// 从最后确认成功起计算的恢复期限。默认 45 秒，必须非零且在时钟范围内。
    pub failure_timeout: Duration,
    /// 第一次普通重试的基础延迟。默认 2 秒，必须非零且在时钟范围内。
    pub retry_initial_delay: Duration,
    /// 普通退避延迟的上限，含抖动。默认 10 秒，必须非零且在时钟范围内。
    ///
    /// 访问限制的最终延迟可能超过此值，因为它至少等于正常心跳间隔。
    pub retry_max_delay: Duration,
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            failure_timeout: DEFAULT_FAILURE_TIMEOUT,
            retry_initial_delay: Duration::from_secs(2),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

impl HeartbeatPolicy {
    pub(super) fn validate(self) -> Result<(), &'static str> {
        for duration in [
            self.failure_timeout,
            self.retry_initial_delay,
            self.retry_max_delay,
        ] {
            if duration.is_zero() {
                return Err("项目心跳恢复策略时长必须大于 0");
            }
            if Instant::now().checked_add(duration).is_none() {
                return Err("项目心跳恢复策略时长超出时钟范围");
            }
        }
        Ok(())
    }

    pub(super) fn warn_if_unsafe(self, every: Duration) {
        if self.failure_timeout > DEFAULT_FAILURE_TIMEOUT {
            tracing::warn!(
                failure_timeout_secs = self.failure_timeout.as_secs_f64(),
                "项目心跳恢复期限超过建议的 45 秒，保留配置但无法保证最短平台失效窗口"
            );
        }
        if every.saturating_add(REQUEST_TIMEOUT) >= self.failure_timeout {
            tracing::warn!(
                interval_secs = every.as_secs_f64(),
                request_timeout_secs = REQUEST_TIMEOUT.as_secs_f64(),
                failure_timeout_secs = self.failure_timeout.as_secs_f64(),
                "项目心跳间隔与请求超时未给恢复期限留出余量，保留配置"
            );
        }
        if self.retry_initial_delay > self.retry_max_delay {
            tracing::warn!(
                retry_initial_secs = self.retry_initial_delay.as_secs_f64(),
                retry_max_secs = self.retry_max_delay.as_secs_f64(),
                "项目心跳初始退避超过上限，保留配置并按上限封顶"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    Permanent,
    Retryable,
    AccessLimited,
    AmbiguousExpired,
    Unknown,
}

impl FailureKind {
    fn label(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Retryable => "retryable",
            Self::AccessLimited => "access_limited",
            Self::AmbiguousExpired => "ambiguous_expired",
            Self::Unknown => "unknown",
        }
    }
}

fn classify(error: &ApiError, batch_size: usize) -> FailureKind {
    match error {
        ApiError::Invalid { .. } => FailureKind::Permanent,
        ApiError::Transport { .. } | ApiError::Decode { .. } => FailureKind::Retryable,
        ApiError::Status { status: 429 } => FailureKind::AccessLimited,
        ApiError::Status { status }
            if (400..500).contains(status) && !matches!(status, 408 | 425) =>
        {
            FailureKind::Permanent
        }
        ApiError::Status { .. } => FailureKind::Retryable,
        ApiError::Platform { code, .. } => match code.raw() {
            4003 | 4004 | 5000..=5002 => FailureKind::Retryable,
            4009 => FailureKind::AccessLimited,
            4000..=4002 | 4005..=4008 | 4010..=4013 | 5003..=5005 | 5011 | 7004 | 7005 | 8002 => {
                FailureKind::Permanent
            }
            7000 | 7003 if batch_size == 1 => FailureKind::Permanent,
            7000 | 7003 => FailureKind::AmbiguousExpired,
            _ => FailureKind::Unknown,
        },
    }
}

fn retry_delay(policy: HeartbeatPolicy, failures: u32, jitter_percent: u32) -> Duration {
    let factor = 1_u128
        .checked_shl(failures.saturating_sub(1))
        .unwrap_or(u128::MAX);
    let nanos = policy
        .retry_initial_delay
        .as_nanos()
        .saturating_mul(factor)
        .min(policy.retry_max_delay.as_nanos());
    let nanos = (nanos * u128::from(jitter_percent) / 100)
        .min(policy.retry_max_delay.as_nanos())
        .max(1);
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
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

fn recovery_deadline(state: &State, confirmed: Instant) -> Instant {
    // 极端时钟边界下尽早结束恢复，不能让后台任务因加法溢出而退出。
    confirmed
        .checked_add(state.heartbeat_policy.failure_timeout)
        .unwrap_or(confirmed)
}

fn expire_recoveries(state: &State) {
    let now = Instant::now();
    let closing = {
        let mut beats = lock(&state.beats);
        let closing: Vec<_> = beats
            .iter()
            .filter(|(_, slot)| {
                slot.retry_at.is_some() && recovery_deadline(state, slot.last_confirmed) <= now
            })
            .map(|(game_id, slot)| Closing {
                beat: PendingBeat {
                    game_id: game_id.clone(),
                    session: slot.session.clone(),
                },
                reason: "recovery_budget_exhausted",
                failures: slot.failures,
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
            let reason = if failed.contains(beat.game_id.as_str()) {
                Some("failed_game_id")
            } else if let Some(kind) = kind {
                slot.failures = slot.failures.saturating_add(1);
                let deadline = recovery_deadline(state, slot.last_confirmed);
                if kind == FailureKind::Permanent {
                    Some("permanent_error")
                } else if deadline <= now {
                    Some("recovery_budget_exhausted")
                } else {
                    let mut delay = retry_delay(state.heartbeat_policy, slot.failures, jitter);
                    if kind == FailureKind::AccessLimited {
                        delay = delay.max(state.app_heartbeat);
                    }
                    slot.retry_at = Some(now.checked_add(delay).unwrap_or(deadline));
                    slot.normal_due = None;
                    tracing::warn!(
                        game_id = %beat.game_id,
                        error_category = kind.label(),
                        ?platform_code,
                        failures = slot.failures,
                        retry_delay_secs = delay.as_secs_f64(),
                        remaining_budget_secs = deadline.duration_since(now).as_secs_f64(),
                        "项目心跳等待重试"
                    );
                    None
                }
            } else {
                if slot.failures > 0 {
                    tracing::info!(
                        game_id = %beat.game_id,
                        failures = slot.failures,
                        "项目心跳已恢复"
                    );
                }
                slot.last_confirmed = now;
                slot.failures = 0;
                slot.retry_at = None;
                None
            };
            if let Some(reason) = reason {
                closing.push(Closing {
                    beat: beat.clone(),
                    reason,
                    failures: slot.failures,
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
        if slot.retry_at.is_none() && !slot.in_flight {
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
        if let Some(at) = slot.retry_at {
            if at <= now {
                retry.push((
                    recovery_deadline(state, slot.last_confirmed),
                    at,
                    id.clone(),
                ));
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
        .filter_map(|slot| {
            slot.retry_at.map(|at| {
                let deadline = recovery_deadline(state, slot.last_confirmed);
                if in_flight {
                    deadline
                } else {
                    deadline.min(at)
                }
            })
        })
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

/// 测试用立即采样；生产请求只能由 run_heartbeats 的单请求调度器启动。
#[cfg(test)]
pub(super) async fn pump(state: &State) {
    let pending: Vec<PendingBeat> = {
        let mut beats = lock(&state.beats);
        beats
            .iter_mut()
            .filter(|(_, slot)| !slot.in_flight)
            .map(|(game_id, slot)| {
                slot.in_flight = true;
                slot.normal_due = None;
                PendingBeat {
                    game_id: game_id.clone(),
                    session: slot.session.clone(),
                }
            })
            .collect()
    };
    for chunk in pending.chunks(MAX_BATCH_HEARTBEAT) {
        let pending: Vec<_> = {
            let beats = lock(&state.beats);
            chunk
                .iter()
                .filter(|beat| {
                    beats
                        .get(&beat.game_id)
                        .is_some_and(|slot| Weak::ptr_eq(&slot.session, &beat.session))
                })
                .cloned()
                .collect()
        };
        if pending.is_empty() {
            continue;
        }
        let mut request = request(state, pending);
        let outcome = request.future.as_mut().await;
        expire_recoveries(state);
        finish_request(state, &request.pending, outcome);
    }
}

#[cfg(test)]
#[path = "heartbeat_test.rs"]
mod heartbeat_test;
