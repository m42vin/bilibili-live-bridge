//! 资源准入策略与显式时间令牌桶；不依赖运行时、时钟或网络。

use std::time::Duration;

/// 一类令牌桶的速率与突发容量，单位由调用方规定为次数或字节。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RatePolicy {
    /// 每秒补充的令牌数量。
    pub per_sec: u64,
    /// 满桶允许的突发数量。
    pub burst: u64,
}

/// 共享资源、发送与开场冷却限制；工程默认值不代表平台官方额度。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourcePolicy {
    /// 下游连接任务总数，包含握手及等待订阅。
    pub max_connections: usize,
    /// 同时执行的 HTTP 升级数量。
    pub max_handshakes: usize,
    /// 正在等待 Manager 接入结果的请求数。
    pub max_pending_attaches: usize,
    /// 正在开场、建连、鉴权或失败清理的任务数。
    pub max_starting_sessions: usize,
    /// 建立中、活动和清理中的场次数。
    pub max_sessions: usize,
    /// 实际平台 start 请求的速率限制，失败也消耗令牌。
    pub start: RatePolicy,
    /// 每个房间的活动订阅上限，包含进程内订阅。
    pub max_subscribers_per_room: usize,
    /// 每条连接入站业务消息的速率限制，包含 ACK。
    pub client_message: RatePolicy,
    /// 全部下游连接共享的序列化及待 flush JSON 字节上限。
    pub max_send_bytes: usize,
    /// 单连接单条序列化 JSON 字节上限，不包含 WebSocket 帧头。
    pub max_connection_send_bytes: usize,
    /// 每连接历史回放的字节速率及突发容量。
    pub replay: RatePolicy,
    /// 实际 start 失败后，同码再次 start 的冷却时间。
    pub failed_code_cooldown: Duration,
    /// 冷却缓存的总条目上限，包含正在 start 的预留条目。
    pub max_failed_codes: usize,
}

impl Default for ResourcePolicy {
    fn default() -> Self {
        Self {
            max_connections: 512,
            max_handshakes: 32,
            max_pending_attaches: 64,
            max_starting_sessions: 4,
            max_sessions: 64,
            start: RatePolicy {
                per_sec: 1,
                burst: 2,
            },
            max_subscribers_per_room: 32,
            client_message: RatePolicy {
                per_sec: 1000,
                burst: 2000,
            },
            max_send_bytes: 32 << 20,
            max_connection_send_bytes: 1 << 20,
            replay: RatePolicy {
                per_sec: 1 << 20,
                burst: 1 << 20,
            },
            failed_code_cooldown: Duration::from_secs(30),
            max_failed_codes: 1024,
        }
    }
}

impl ResourcePolicy {
    /// 校验正值、跨平台计数范围以及明确矛盾的并发上限。
    pub fn validate(self) -> Result<(), &'static str> {
        for n in [
            self.max_connections,
            self.max_handshakes,
            self.max_pending_attaches,
            self.max_starting_sessions,
            self.max_sessions,
            self.max_subscribers_per_room,
            self.max_send_bytes,
            self.max_connection_send_bytes,
            self.max_failed_codes,
        ] {
            if n == 0 || n as u128 > u32::MAX as u128 {
                return Err("资源上限必须为 1..=u32::MAX 的整数");
            }
        }
        for rate in [self.start, self.client_message, self.replay] {
            rate.validate()?;
        }
        if self.max_handshakes > self.max_connections {
            return Err("握手上限不能超过连接上限");
        }
        if self.max_starting_sessions > self.max_sessions {
            return Err("开场上限不能超过会话上限");
        }
        if self.max_connection_send_bytes > self.max_send_bytes {
            return Err("单连接发送字节上限不能超过共享发送上限");
        }
        if self.replay.burst < self.max_connection_send_bytes as u64 {
            return Err("回放突发字节不能小于单连接发送字节上限");
        }
        if self.failed_code_cooldown.is_zero()
            || self.failed_code_cooldown.as_secs() > u32::MAX as u64
        {
            return Err("失败身份码冷却必须为有效正时长");
        }
        Ok(())
    }
}

impl RatePolicy {
    /// 校验速率和容量，避免固定精度计算及运行时转换溢出。
    pub fn validate(self) -> Result<(), &'static str> {
        if self.per_sec == 0
            || self.burst == 0
            || self.per_sec > u32::MAX as u64
            || self.burst > u32::MAX as u64
        {
            return Err("速率和突发容量必须为 1..=u32::MAX 的整数");
        }
        Ok(())
    }
}

/// 可观测的资源类别，适合固定基数的日志和计数器标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Resource {
    /// 下游连接。
    Connection,
    /// 下游握手。
    Handshake,
    /// 待接入请求。
    PendingAttach,
    /// 开场任务。
    StartingSession,
    /// 上游会话。
    Session,
    /// 实际 start 频率。
    StartRate,
    /// 房间订阅数。
    Subscriber,
    /// 连接业务消息频率。
    ClientMessage,
    /// 下游序列化及待发送字节。
    SendingBytes,
    /// start 失败冷却及其有界缓存。
    FailedCode,
}

impl Resource {
    /// 固定日志标签，不包含身份码或其他凭据。
    pub const fn label(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Handshake => "handshake",
            Self::PendingAttach => "pending_attach",
            Self::StartingSession => "starting_session",
            Self::Session => "session",
            Self::StartRate => "start_rate",
            Self::Subscriber => "subscriber",
            Self::ClientMessage => "client_message",
            Self::SendingBytes => "sending_bytes",
            Self::FailedCode => "failed_code",
        }
    }
}

/// 可重试的资源拒绝原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    /// 触发拒绝的资源。
    pub resource: Resource,
    /// 重试建议；不保证届时资源可用。
    pub retry_after_ms: Option<u64>,
}

impl Rejection {
    /// 稳定的客户端错误码。
    pub const fn code(self) -> &'static str {
        match self.resource {
            Resource::StartRate | Resource::ClientMessage | Resource::FailedCode => "rate_limited",
            Resource::Subscriber => "subscription_limit",
            _ => "server_busy",
        }
    }
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.code(), self.resource.label())
    }
}

impl std::error::Error for Rejection {}

/// 固定纳秒精度令牌桶；调用方提供同一单调时间原点的 elapsed。
#[derive(Debug)]
pub struct TokenBucket {
    policy: RatePolicy,
    credit: u128,
    updated: Duration,
}

impl TokenBucket {
    /// 创建满桶；无内部时钟。
    pub fn new(policy: RatePolicy, now: Duration) -> Result<Self, &'static str> {
        policy.validate()?;
        Ok(Self {
            policy,
            credit: policy.burst as u128 * 1_000_000_000,
            updated: now,
        })
    }

    /// 消耗一个令牌，或返回最少还需等待的毫秒数。时间倒退不补充额度。
    pub fn take(&mut self, now: Duration) -> Result<(), u64> {
        self.take_amount(1, now)
    }

    /// 按显式时间一次计费多个令牌；超过突发容量时返回 u64::MAX，不消耗额度。
    pub fn take_amount(&mut self, amount: u64, now: Duration) -> Result<(), u64> {
        if amount > self.policy.burst {
            return Err(u64::MAX);
        }
        if now > self.updated {
            let added = (now - self.updated)
                .as_nanos()
                .saturating_mul(self.policy.per_sec as u128);
            self.credit = self
                .credit
                .saturating_add(added)
                .min(self.policy.burst as u128 * 1_000_000_000);
            self.updated = now;
        }
        let required = u128::from(amount) * 1_000_000_000;
        if self.credit >= required {
            self.credit -= required;
            return Ok(());
        }
        let nanos = (required - self.credit).div_ceil(self.policy.per_sec as u128);
        Err(nanos.div_ceil(1_000_000).min(u64::MAX as u128) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bucket_restores_fractional_tokens_caps_burst_and_ignores_backwards_time() {
        let mut b = TokenBucket::new(
            RatePolicy {
                per_sec: 2,
                burst: 2,
            },
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(b.take(Duration::ZERO), Ok(()));
        assert_eq!(b.take(Duration::ZERO), Ok(()));
        assert_eq!(b.take(Duration::from_millis(250)), Err(250));
        assert_eq!(b.take(Duration::from_millis(500)), Ok(()));
        assert_eq!(b.take(Duration::ZERO), Err(500));
        for _ in 0..2 {
            assert_eq!(b.take(Duration::MAX), Ok(()));
        }
        assert_eq!(b.take(Duration::MAX), Err(500));
    }

    #[test]
    fn validates_limits_and_conflicts() {
        assert!(ResourcePolicy::default().validate().is_ok());
        for p in [
            ResourcePolicy {
                max_connections: 0,
                ..Default::default()
            },
            ResourcePolicy {
                max_handshakes: 513,
                ..Default::default()
            },
            ResourcePolicy {
                max_sessions: 1,
                ..Default::default()
            },
            ResourcePolicy {
                start: RatePolicy {
                    per_sec: u64::MAX,
                    burst: 1,
                },
                ..Default::default()
            },
        ] {
            assert!(p.validate().is_err());
        }
    }

    #[test]
    fn byte_charges_are_atomic_fractional_and_reject_impossible_bursts() {
        let mut bucket = TokenBucket::new(
            RatePolicy {
                per_sec: 10,
                burst: 20,
            },
            Duration::ZERO,
        )
        .unwrap();
        bucket.take_amount(15, Duration::ZERO).unwrap();
        assert_eq!(bucket.take_amount(21, Duration::ZERO), Err(u64::MAX));
        assert_eq!(bucket.take_amount(10, Duration::ZERO), Err(500));
        assert_eq!(bucket.take_amount(10, Duration::from_millis(499)), Err(1));
        bucket.take_amount(10, Duration::from_millis(500)).unwrap();
        bucket.take_amount(0, Duration::ZERO).unwrap();
        assert_eq!(bucket.take_amount(1, Duration::ZERO), Err(100));
    }
}
