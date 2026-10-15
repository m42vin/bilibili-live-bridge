//! 官方长连接运行期间的期限。

use std::time::Duration;

use tokio::time::Instant;

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
    pub(crate) fn validate(self, heartbeat: Duration) -> Result<(), &'static str> {
        for duration in [heartbeat, self.write_timeout, self.activity_timeout] {
            if duration.is_zero() {
                return Err("长连接心跳与健康策略时长必须大于 0");
            }
            if Instant::now().checked_add(duration).is_none() {
                return Err("长连接健康策略时长超出时钟范围");
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
