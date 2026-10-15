//! 会话上游的健康策略、有限恢复及状态快照。

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::time::{Instant, sleep, sleep_until, timeout_at};

use super::{Session, State, close_socket, until_idle};
use crate::open_live::api::StartResult;
use crate::open_live::ws::{HealthPolicy, LiveEvent, WsError};
use crate::session::platform::LiveSocket;

/// 原场次的官方连接恢复策略，不重新开启场次。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// 从首次故障起计算的恢复预算，默认 120 秒。
    pub failure_timeout: Duration,
    /// 第一轮失败后的基础退避，默认 2 秒。
    pub retry_initial_delay: Duration,
    /// 退避最终上限，含抖动，默认 10 秒。
    pub retry_max_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            failure_timeout: Duration::from_secs(120),
            retry_initial_delay: Duration::from_secs(2),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

/// 上游连接健康检查和原场次恢复策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamPolicy {
    /// 连接发送及有效入站活动期限。
    pub health: HealthPolicy,
    /// 恢复策略；None 表示断开后立即结束会话。默认启用。
    pub reconnect: Option<ReconnectPolicy>,
}

impl Default for UpstreamPolicy {
    fn default() -> Self {
        Self {
            health: HealthPolicy::default(),
            reconnect: Some(ReconnectPolicy::default()),
        }
    }
}

impl UpstreamPolicy {
    pub(crate) fn validate(self, heartbeat: Duration) -> Result<(), &'static str> {
        self.health.validate(heartbeat)?;
        if let Some(policy) = self.reconnect {
            for duration in [
                policy.failure_timeout,
                policy.retry_initial_delay,
                policy.retry_max_delay,
            ] {
                if duration.is_zero() {
                    return Err("上游恢复策略时长必须大于 0");
                }
                if Instant::now().checked_add(duration).is_none() {
                    return Err("上游恢复策略时长超出时钟范围");
                }
            }
            if policy.retry_initial_delay > policy.retry_max_delay {
                return Err("上游初始退避不得超过退避上限");
            }
        }
        Ok(())
    }
}

/// 当前上游状态。状态流是可合并中间变化的最新快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamStatus {
    /// 原场次的官方连接已经鉴权。
    Connected,
    /// 订阅保留，正在恢复原场次的连接。
    Reconnecting,
    /// 会话已经完成本地清理。
    Closed,
}

struct Recovery {
    deadline: Instant,
    failures: u32,
    attempts: u32,
}

pub(super) async fn run(
    state: Arc<State>,
    session: Arc<Session>,
    socket: Box<dyn LiveSocket>,
    events: broadcast::Sender<Arc<LiveEvent>>,
    started: StartResult,
) {
    let policy = state.upstream_policy;
    let mut socket = Some(socket);
    let mut connected_at = Instant::now();
    let mut recovery: Option<Recovery> = None;
    tracing::info!("直播间会话已建立");
    loop {
        let stable_at = connected_at + policy.health.activity_timeout;
        let result = active(&session, async {
            tokio::select! {
                biased;
                incoming = socket.as_mut().unwrap().recv() => Some(incoming),
                _ = sleep_until(stable_at), if recovery.is_some() => None,
            }
        })
        .await;
        let Some(result) = result else {
            break;
        };
        match result {
            None => {
                recovery = None;
                tracing::debug!("上游持续健康，清除恢复预算");
                continue;
            }
            Some(Ok(Some(event))) => {
                if recovery.is_some() && Instant::now() >= stable_at {
                    recovery = None;
                }
                let ends = event.ends_push();
                tracing::debug!(cmd = event.cmd(), "转发直播间事件");
                if state.log.append(&session.stream_id, &event).is_err() {
                    tracing::error!("无法记录事件，结束流以避免静默漏发");
                    break;
                }
                let _ = events.send(Arc::new(event));
                if ends {
                    tracing::info!("平台已停止互动推送，关闭会话");
                    break;
                }
                continue;
            }
            Some(Err(error)) => {
                tracing::warn!(room_id = started.anchor().room_id, game_id = %started.game_id(), %error, "官方长连接读取失败");
                if !retryable(&error) {
                    break;
                }
            }
            Some(Ok(None)) => {
                tracing::info!("官方长连接已关闭");
            }
        }
        let Some(reconnect) = policy.reconnect else {
            break;
        };
        let window = recovery.get_or_insert_with(|| Recovery {
            deadline: Instant::now() + reconnect.failure_timeout,
            failures: 0,
            attempts: 0,
        });
        if Instant::now() >= window.deadline {
            tracing::warn!(room_id = started.anchor().room_id, game_id = %started.game_id(), reason = "reconnect_deadline", "上游恢复预算已耗尽");
            drop(socket.take());
            break;
        }
        session.upstream.send_replace(UpstreamStatus::Reconnecting);
        let old = socket.take().unwrap();
        let endpoint = old.endpoint().map(str::to_owned);
        if !matches!(
            timeout_at(
                window.deadline,
                active(
                    &session,
                    close_socket(old, started.game_id(), started.anchor().room_id)
                )
            )
            .await,
            Ok(Some(()))
        ) {
            break;
        }
        let restored = restore(
            &state,
            &session,
            &started,
            endpoint.as_deref(),
            reconnect,
            window,
        )
        .await;
        let Some(restored) = restored else {
            break;
        };
        socket = Some(restored);
        connected_at = Instant::now();
        session.upstream.send_replace(UpstreamStatus::Connected);
        tracing::info!("原场次上游已恢复");
    }

    session.begin_close();
    if let Some(socket) = socket {
        close_socket(socket, started.game_id(), started.anchor().room_id).await;
    }
    if let Err(error) = state.platform.end(started.game_id()).await {
        tracing::warn!(room_id = started.anchor().room_id, game_id = %started.game_id(), %error, "关闭官方场次失败");
    }
    state.log.finish(&session.stream_id);
    state.unlink(&session);
    session.phase.send_replace(super::Phase::Closed);
    drop(events);
    session.upstream.send_replace(UpstreamStatus::Closed);
    tracing::info!("直播间会话已结束");
    session.mark_finished();
}

async fn restore(
    state: &State,
    session: &Session,
    started: &StartResult,
    endpoint: Option<&str>,
    policy: ReconnectPolicy,
    recovery: &mut Recovery,
) -> Option<Box<dyn LiveSocket>> {
    loop {
        if Instant::now() >= recovery.deadline || session.stop.is_cancelled() {
            return None;
        }
        recovery.attempts = recovery.attempts.saturating_add(1);
        tracing::info!(
            attempt = recovery.attempts,
            remaining_secs = recovery
                .deadline
                .saturating_duration_since(Instant::now())
                .as_secs_f64(),
            "尝试恢复官方长连接"
        );
        match timeout_at(
            recovery.deadline,
            active(session, state.platform.reconnect(started, endpoint)),
        )
        .await
        {
            Ok(Some(Ok(socket))) => {
                // timeout_at 可能先轮询已就绪的建连 future，再检查期限。
                if Instant::now() >= recovery.deadline || session.stop.is_cancelled() {
                    drop(socket);
                    return None;
                }
                return Some(socket);
            }
            Ok(Some(Err(error))) => {
                tracing::warn!(room_id = started.anchor().room_id, game_id = %started.game_id(), attempt = recovery.attempts, %error, "恢复官方长连接失败");
                if !retryable(&error) {
                    return None;
                }
            }
            _ => {
                tracing::warn!(room_id = started.anchor().room_id, game_id = %started.game_id(), reason = "reconnect_deadline", "上游恢复已终止");
                return None;
            }
        }
        recovery.failures = recovery.failures.saturating_add(1);
        let delay = retry_delay(policy, recovery.failures, jitter());
        tracing::debug!(
            attempt = recovery.attempts,
            delay_secs = delay.as_secs_f64(),
            "上游恢复退避"
        );
        if !matches!(
            timeout_at(recovery.deadline, active(session, sleep(delay))).await,
            Ok(Some(()))
        ) {
            return None;
        }
    }
}

fn retryable(error: &WsError) -> bool {
    matches!(error, WsError::Transport { .. } | WsError::Connect { .. })
}

fn retry_delay(policy: ReconnectPolicy, failures: u32, jitter_percent: u32) -> Duration {
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

fn jitter() -> u32 {
    let random = uuid::Uuid::new_v4();
    80 + u32::from(u16::from_be_bytes([
        random.as_bytes()[0],
        random.as_bytes()[1],
    ])) % 41
}

/// 保留操作 future 跨越闲置变更；停止和闲置期限始终优先。
async fn active<T>(session: &Session, future: impl Future<Output = T>) -> Option<T> {
    let mut future = std::pin::pin!(future);
    let mut idle = session.idle.subscribe();
    loop {
        let deadline = *idle.borrow_and_update();
        tokio::select! {
            biased;
            _ = session.stop.cancelled() => return None,
            _ = idle.changed() => {},
            _ = until_idle(deadline) => { if session.expire_idle() { return None; } },
            result = &mut future => return Some(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validation_and_jitter_bounds() {
        let policy = UpstreamPolicy::default();
        assert_eq!(
            policy.reconnect.unwrap().failure_timeout,
            Duration::from_secs(120)
        );
        assert!(policy.validate(Duration::from_secs(20)).is_ok());
        for failures in [1, 2, 3, 4, u32::MAX] {
            for jitter in [80, 100, 120] {
                let delay = retry_delay(policy.reconnect.unwrap(), failures, jitter);
                assert!(!delay.is_zero() && delay <= Duration::from_secs(10));
            }
        }
        let invalid = UpstreamPolicy {
            reconnect: Some(ReconnectPolicy {
                retry_initial_delay: Duration::from_secs(11),
                ..ReconnectPolicy::default()
            }),
            ..policy
        };
        assert!(invalid.validate(Duration::from_secs(20)).is_err());
    }
}
