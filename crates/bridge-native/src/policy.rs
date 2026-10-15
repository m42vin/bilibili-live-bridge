//! 策略的原生时钟检查和诊断输出；业务规则由 core 决定。
use crate::open_live::api::REQUEST_TIMEOUT;
use bridge_core::session::{HealthPolicy, HeartbeatDiagnostic, HeartbeatPolicy, UpstreamPolicy};
use std::time::Duration;
use tokio::time::Instant;

pub(crate) fn validate_health(
    policy: HealthPolicy,
    heartbeat: Duration,
) -> Result<(), &'static str> {
    policy.validate(heartbeat)?;
    for duration in [heartbeat, policy.write_timeout, policy.activity_timeout] {
        if Instant::now().checked_add(duration).is_none() {
            return Err("长连接健康策略时长超出时钟范围");
        }
    }
    Ok(())
}
pub(crate) fn validate_heartbeat(policy: HeartbeatPolicy) -> Result<(), &'static str> {
    policy.validate()?;
    for duration in [
        policy.failure_timeout,
        policy.retry_initial_delay,
        policy.retry_max_delay,
    ] {
        if Instant::now().checked_add(duration).is_none() {
            return Err("项目心跳恢复策略时长超出时钟范围");
        }
    }
    Ok(())
}
pub(crate) fn validate_upstream(
    policy: UpstreamPolicy,
    heartbeat: Duration,
) -> Result<(), &'static str> {
    policy.validate(heartbeat)?;
    validate_health(policy.health, heartbeat)?;
    if let Some(policy) = policy.reconnect {
        for duration in [
            policy.failure_timeout,
            policy.retry_initial_delay,
            policy.retry_max_delay,
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err("上游恢复策略时长超出时钟范围");
            }
        }
    }
    Ok(())
}
pub(crate) fn warn_heartbeat(policy: HeartbeatPolicy, every: Duration) {
    let diagnostics = policy.diagnostics(every, REQUEST_TIMEOUT);
    if diagnostics.contains(&HeartbeatDiagnostic::FailureTimeoutExceedsRecommended) {
        tracing::warn!(
            failure_timeout_secs = policy.failure_timeout.as_secs_f64(),
            "项目心跳恢复期限超过建议的 45 秒，保留配置但无法保证最短平台失效窗口"
        );
    }
    if diagnostics.contains(&HeartbeatDiagnostic::InsufficientRecoveryMargin) {
        tracing::warn!(
            interval_secs = every.as_secs_f64(),
            request_timeout_secs = REQUEST_TIMEOUT.as_secs_f64(),
            failure_timeout_secs = policy.failure_timeout.as_secs_f64(),
            "项目心跳间隔与请求超时未给恢复期限留出余量，保留配置"
        );
    }
    if diagnostics.contains(&HeartbeatDiagnostic::InitialDelayExceedsMaximum) {
        tracing::warn!(
            retry_initial_secs = policy.retry_initial_delay.as_secs_f64(),
            retry_max_secs = policy.retry_max_delay.as_secs_f64(),
            "项目心跳初始退避超过上限，保留配置并按上限封顶"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bridge_core::session::ReconnectPolicy;
    #[test]
    fn rejects_unrepresentable_upstream_deadlines() {
        for field in 0..3 {
            let mut reconnect = ReconnectPolicy::default();
            match field {
                0 => reconnect.failure_timeout = Duration::MAX,
                1 => {
                    reconnect.retry_initial_delay = Duration::MAX;
                    reconnect.retry_max_delay = Duration::MAX;
                }
                _ => reconnect.retry_max_delay = Duration::MAX,
            }
            let policy = UpstreamPolicy {
                reconnect: Some(reconnect),
                ..Default::default()
            };
            assert!(policy.validate(Duration::from_secs(20)).is_ok());
            assert!(validate_upstream(policy, Duration::from_secs(20)).is_err());
        }
    }
}
