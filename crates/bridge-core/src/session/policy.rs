//! 与运行时无关的健康、心跳和重连策略。
use std::time::Duration;

/// 官方长连接的发送与有效入站活动期限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthPolicy {
    /// 单次发送的总期限，包含准备和 flush；默认 5 秒。
    pub write_timeout: Duration,
    /// 最后有效事件或官方心跳回复后的存活期限；默认 60 秒。
    pub activity_timeout: Duration,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            write_timeout: Duration::from_secs(5),
            activity_timeout: Duration::from_secs(60),
        }
    }
}

impl HealthPolicy {
    /// 检查非零时长以及存活期限大于心跳间隔加发送期限。
    pub fn validate(self, heartbeat: Duration) -> Result<(), &'static str> {
        for duration in [heartbeat, self.write_timeout, self.activity_timeout] {
            if duration.is_zero() {
                return Err("长连接心跳与健康策略时长必须大于 0");
            }
        }
        if heartbeat
            .checked_add(self.write_timeout)
            .is_none_or(|minimum| self.activity_timeout <= minimum)
        {
            return Err("长连接存活期限必须大于心跳间隔加发送期限");
        }
        Ok(())
    }
}

const DEFAULT_FAILURE_TIMEOUT: Duration = Duration::from_secs(45);

/// 项目心跳的有限恢复策略。
///
/// 默认从最后确认成功起保留 45 秒，失败后以 2 秒起、10 秒封顶的指数退避重试。
/// 退避加入 ±20% 抖动；访问限制的延迟至少为正常项目心跳间隔。
/// 首次登记作为初始成功时间；恢复期限仅在首次失败后启用。
///
/// 默认预算按最短 60 秒平台失效窗口，为请求响应延迟和安全余量留出 15 秒。
/// 不安全的组合由 diagnostics 返回提示，运行时可记录警告；不修改参数。
/// 自定义参数不能保证在平台失效前恢复。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    /// 从最后确认成功起计算的恢复期限。默认 45 秒，必须非零；运行时另行检查时钟范围。
    pub failure_timeout: Duration,
    /// 第一次普通重试的基础延迟。默认 2 秒，必须非零；运行时另行检查时钟范围。
    pub retry_initial_delay: Duration,
    /// 普通退避延迟的上限，含抖动。默认 10 秒，必须非零；运行时另行检查时钟范围。
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
    /// 检查非零时长；时钟可表示范围由运行时检查。
    pub fn validate(self) -> Result<(), &'static str> {
        for duration in [
            self.failure_timeout,
            self.retry_initial_delay,
            self.retry_max_delay,
        ] {
            if duration.is_zero() {
                return Err("项目心跳恢复策略时长必须大于 0");
            }
        }
        Ok(())
    }
}

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
    /// 检查健康规则及恢复时长关系，不读取时钟。
    pub fn validate(self, heartbeat: Duration) -> Result<(), &'static str> {
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
            }
            if policy.retry_initial_delay > policy.retry_max_delay {
                return Err("上游初始退避不得超过退避上限");
            }
        }
        Ok(())
    }
}

/// 项目心跳配置的风险；仅提示，不改变配置或拒绝启动。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatDiagnostic {
    /// 恢复预算超过建议的 45 秒。
    FailureTimeoutExceedsRecommended,
    /// 心跳间隔加请求超时未给恢复留下余量。
    InsufficientRecoveryMargin,
    /// 初始退避超过上限，计算时仍按上限封顶。
    InitialDelayExceedsMaximum,
}
impl HeartbeatPolicy {
    /// 返回所有风险；请求超时由运行时提供。
    pub fn diagnostics(
        self,
        every: Duration,
        request_timeout: Duration,
    ) -> Vec<HeartbeatDiagnostic> {
        let mut result = Vec::new();
        if self.failure_timeout > DEFAULT_FAILURE_TIMEOUT {
            result.push(HeartbeatDiagnostic::FailureTimeoutExceedsRecommended);
        }
        if every.saturating_add(request_timeout) >= self.failure_timeout {
            result.push(HeartbeatDiagnostic::InsufficientRecoveryMargin);
        }
        if self.retry_initial_delay > self.retry_max_delay {
            result.push(HeartbeatDiagnostic::InitialDelayExceedsMaximum);
        }
        result
    }
    /// 指数退避，最终封顶且至少为 1 纳秒。
    /// 调用方应先校验策略，并提供 80–120 的抖动百分比；失败次数 0 按首次失败处理。
    pub fn retry_delay(self, failures: u32, jitter_percent: u32) -> Duration {
        retry_delay(
            self.retry_initial_delay,
            self.retry_max_delay,
            failures,
            jitter_percent,
        )
    }
    /// 访问限制至少等待一个正常心跳周期；此下限可以超过普通退避上限。
    pub fn retry_delay_for(
        self,
        kind: super::failure::FailureKind,
        every: Duration,
        failures: u32,
        jitter_percent: u32,
    ) -> Duration {
        let delay = self.retry_delay(failures, jitter_percent);
        if kind == super::failure::FailureKind::AccessLimited {
            delay.max(every)
        } else {
            delay
        }
    }
}
impl ReconnectPolicy {
    /// 指数退避，不会生成随机数。
    /// 调用方应先通过 UpstreamPolicy 校验策略，并提供 80–120 的抖动百分比。
    pub fn retry_delay(self, failures: u32, jitter_percent: u32) -> Duration {
        retry_delay(
            self.retry_initial_delay,
            self.retry_max_delay,
            failures,
            jitter_percent,
        )
    }
}
fn retry_delay(
    initial: Duration,
    maximum: Duration,
    failures: u32,
    jitter_percent: u32,
) -> Duration {
    let factor = 1_u128
        .checked_shl(failures.saturating_sub(1))
        .unwrap_or(u128::MAX);
    let nanos = initial
        .as_nanos()
        .saturating_mul(factor)
        .min(maximum.as_nanos());
    let nanos = (nanos.saturating_mul(u128::from(jitter_percent)) / 100)
        .min(maximum.as_nanos())
        .max(1);
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
}

#[cfg(test)]
#[path = "policy_test.rs"]
mod policy_test;
