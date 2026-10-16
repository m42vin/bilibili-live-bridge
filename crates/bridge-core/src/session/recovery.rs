//! 单场心跳与上游连接恢复状态。调用方显式提供单调时间，不执行 I/O。
//!
//! 所有输入时间必须来自同一时间轴且按事件处理顺序单调不减。状态不是跨进程
//! 恢复快照；重启后的时间基准与持久化恢复由运行时另行定义。
//! 运行时必须串行驱动每个状态，保证同一请求/尝试的反馈只应用一次，并过滤旧场次结果。

use std::time::Duration;

use super::failure::FailureKind;
use super::{HeartbeatPolicy, UpstreamPolicy};

/// 时间点所需的纯运算；实现不得读取当前时间。
///
/// `checked_add` 必须反映运行时实际可表示的范围。默认使用 Duration 作为逻辑时间。
pub trait Moment: Copy + Ord {
    /// 加上时长，超出可表示范围返回 None。
    fn checked_add(self, duration: Duration) -> Option<Self>;

    /// 非负时间差。
    fn saturating_duration_since(self, earlier: Self) -> Duration;
}

impl Moment for Duration {
    fn checked_add(self, duration: Duration) -> Option<Self> {
        Duration::checked_add(self, duration)
    }

    fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.saturating_sub(earlier)
    }
}

/// 单个场次的项目心跳反馈。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatOutcome {
    /// 本场次确认成功。
    Confirmed,
    /// 成功响应的 failed_game_ids 包含本场次。
    FailedGame,
    /// 整个请求失败，已按批次大小分类。
    Failure(FailureKind),
}

/// 心跳恢复结束原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatClose {
    /// 平台明确列为失败场次。
    FailedGame,
    /// 不可重试的请求错误。
    Permanent,
    /// 最后成功时间起的恢复预算已耗尽。
    Exhausted,
}

impl HeartbeatClose {
    /// 保留原生日志使用的原因名称。
    pub fn label(self) -> &'static str {
        match self {
            Self::FailedGame => "failed_game_id",
            Self::Permanent => "permanent_error",
            Self::Exhausted => "recovery_budget_exhausted",
        }
    }
}

/// 反馈处理后的动作，执行仍由运行时负责。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatDecision<T> {
    /// 成功并重置恢复；包含重置前的失败次数。
    Confirmed {
        /// 重置前的连续失败次数，用于恢复日志。
        previous_failures: u32,
    },
    /// 安排重试；截止时间可以早于下次重试时间。
    Retry {
        /// 本次退避时长。
        delay: Duration,
        /// 当前恢复窗口的绝对截止时间。
        deadline: T,
    },
    /// 结束本场次；终止状态不会因迟到响应复活。
    Close(HeartbeatClose),
}

/// 单个已注册场次的心跳恢复状态；组批和正常周期不属于此状态。
#[derive(Debug, Clone)]
pub struct HeartbeatRecovery<T: Moment = Duration> {
    policy: HeartbeatPolicy,
    every: Duration,
    last_confirmed: T,
    failures: u32,
    retry_at: Option<T>,
    closed: Option<HeartbeatClose>,
}

impl<T: Moment> HeartbeatRecovery<T> {
    /// 注册时刻作为首次确认时间；调用方应先校验策略。
    pub fn new(policy: HeartbeatPolicy, every: Duration, registered: T) -> Self {
        Self {
            policy,
            every,
            last_confirmed: registered,
            failures: 0,
            retry_at: None,
            closed: None,
        }
    }

    /// 最后成功确认时间。
    pub fn last_confirmed(&self) -> T {
        self.last_confirmed
    }

    /// 连续失败次数。
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// 下一次重试时间；请求在途时仍保留，用于独立检查预算。
    pub fn retry_at(&self) -> Option<T> {
        self.retry_at
    }

    /// 潜在恢复截止时间；未失败时此预算尚未启用。
    pub fn deadline(&self) -> T {
        self.last_confirmed
            .checked_add(self.policy.failure_timeout)
            .unwrap_or(self.last_confirmed)
    }

    /// 当前是否应因恢复预算耗尽结束；正常状态不会因没有新请求而过期。
    pub fn expired(&self, now: T) -> bool {
        self.retry_at.is_some() && self.deadline() <= now
    }

    /// 下次应唤醒的恢复时间；全局有请求在途时仅等待预算截止。
    pub fn next_deadline(&self, request_in_flight: bool) -> Option<T> {
        self.retry_at.map(|at| {
            if request_in_flight {
                self.deadline()
            } else {
                self.deadline().min(at)
            }
        })
    }

    /// 标记预算耗尽；可重复调用，且不允许后续成功响应恢复终止场次。
    pub fn expire(&mut self, now: T) -> Option<HeartbeatClose> {
        if self.closed.is_none() && self.expired(now) {
            self.closed = Some(HeartbeatClose::Exhausted);
        }
        self.closed
    }

    /// 应用当前请求结果。调用方须先验证场次归属，过滤已被替换的请求。
    pub fn complete(
        &mut self,
        now: T,
        outcome: HeartbeatOutcome,
        jitter: u32,
    ) -> HeartbeatDecision<T> {
        if let Some(reason) = self.expire(now) {
            return HeartbeatDecision::Close(reason);
        }
        let reason = match outcome {
            HeartbeatOutcome::Confirmed => {
                let previous_failures = self.failures;
                self.last_confirmed = now;
                self.failures = 0;
                self.retry_at = None;
                return HeartbeatDecision::Confirmed { previous_failures };
            }
            HeartbeatOutcome::FailedGame => HeartbeatClose::FailedGame,
            HeartbeatOutcome::Failure(kind) => {
                self.failures = self.failures.saturating_add(1);
                let deadline = self.deadline();
                if kind == FailureKind::Permanent {
                    HeartbeatClose::Permanent
                } else if deadline <= now {
                    HeartbeatClose::Exhausted
                } else {
                    let delay =
                        self.policy
                            .retry_delay_for(kind, self.every, self.failures, jitter);
                    self.retry_at = Some(now.checked_add(delay).unwrap_or(deadline));
                    return HeartbeatDecision::Retry { delay, deadline };
                }
            }
        };
        self.closed = Some(reason);
        HeartbeatDecision::Close(reason)
    }
}

#[derive(Debug, Clone)]
struct Window<T> {
    deadline: T,
    failures: u32,
    attempts: u32,
}

/// 原场次的上游恢复状态。短暂连接成功保留窗口，持续健康后才清除。
#[derive(Debug, Clone)]
pub struct UpstreamRecovery<T: Moment = Duration> {
    policy: UpstreamPolicy,
    connected_at: T,
    window: Option<Window<T>>,
    retry_at: Option<T>,
    connected: bool,
}

impl<T: Moment> UpstreamRecovery<T> {
    /// 从已鉴权连接开始；调用方须先校验策略。
    pub fn new(policy: UpstreamPolicy, connected_at: T) -> Self {
        Self {
            policy,
            connected_at,
            window: None,
            retry_at: None,
            connected: true,
        }
    }

    /// 当前恢复窗口的截止时间，包括旧连接释放、建连和退避。
    pub fn deadline(&self) -> Option<T> {
        self.window.as_ref().map(|w| w.deadline)
    }

    /// 当前窗口已发起的连接尝试数。
    pub fn attempts(&self) -> u32 {
        self.window.as_ref().map_or(0, |w| w.attempts)
    }

    /// 当前窗口的连接失败数，短暂成功不清零。
    pub fn failures(&self) -> u32 {
        self.window.as_ref().map_or(0, |w| w.failures)
    }

    /// 下次连接尝试的时间。
    pub fn retry_at(&self) -> Option<T> {
        self.retry_at
    }

    /// 连接持续健康到该时刻才允许重置预算。
    pub fn stable_at(&self) -> Option<T> {
        if self.connected && self.window.is_some() {
            Some(
                self.connected_at
                    .checked_add(self.policy.health.activity_timeout)
                    .unwrap_or(self.connected_at),
            )
        } else {
            None
        }
    }

    /// 处理健康计时器或有效事件；断线事件不能调用此方法抢先刷新预算。
    pub fn observe_healthy(&mut self, now: T) -> bool {
        if self.stable_at().is_some_and(|at| now >= at) {
            self.window = None;
            self.retry_at = None;
            true
        } else {
            false
        }
    }

    /// 断线后开始或继续窗口；恢复禁用或预算已耗尽返回 false。
    pub fn disconnected(&mut self, now: T) -> bool {
        self.connected = false;
        self.retry_at = None;
        let Some(policy) = self.policy.reconnect else {
            return false;
        };
        let window = self.window.get_or_insert_with(|| Window {
            deadline: now.checked_add(policy.failure_timeout).unwrap_or(now),
            failures: 0,
            attempts: 0,
        });
        now < window.deadline
    }

    /// 检查恢复预算，等于截止时间也视为超时。
    pub fn expired(&self, now: T) -> bool {
        self.deadline().is_some_and(|at| now >= at)
    }

    /// 在实际发起建连前登记尝试；截止或退避未到期时不允许发起。
    pub fn begin_attempt(&mut self, now: T) -> Option<u32> {
        if self.connected || self.expired(now) || self.retry_at.is_some_and(|at| now < at) {
            return None;
        }
        let window = self.window.as_mut()?;
        window.attempts = window.attempts.saturating_add(1);
        self.retry_at = None;
        Some(window.attempts)
    }

    /// 连接可重试失败后的退避；永久错误由调用方结束会话。
    pub fn attempt_failed(&mut self, now: T, jitter: u32) -> Option<Duration> {
        let policy = self.policy.reconnect?;
        let window = self.window.as_mut()?;
        window.failures = window.failures.saturating_add(1);
        let delay = policy.retry_delay(window.failures, jitter);
        self.retry_at = Some(now.checked_add(delay).unwrap_or(window.deadline));
        Some(delay)
    }

    /// 接受恢复连接；迟于或等于预算截止的成功必须由调用方丢弃 socket。
    pub fn restored(&mut self, now: T) -> bool {
        if self.window.is_none() || self.expired(now) {
            return false;
        }
        self.connected = true;
        self.connected_at = now;
        self.retry_at = None;
        true
    }
}

#[cfg(test)]
#[path = "recovery_test.rs"]
mod recovery_test;
