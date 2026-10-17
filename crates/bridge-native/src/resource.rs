//! 原生共享资源预算、所有权名额和运行时计数。拒绝不排队，不消耗心跳或清理额度。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bridge_core::resource::TokenBucket;
pub use bridge_core::resource::{RatePolicy, Rejection, Resource, ResourcePolicy};
use sha2::{Digest, Sha256};
use tokio::time::Instant;

/// 当前占用与累计计数；拒绝数组按 Resource 的枚举顺序索引。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceSnapshot {
    /// 连接、握手、待接入、开场和会话的当前占用。
    pub occupied: [usize; 5],
    /// 已发出的 start 调用量，包含失败请求。
    pub start_calls: u64,
    /// 当前仍持有活动会话守卫的订阅总数。
    pub subscribers: usize,
    /// 按资源类别累计的拒绝数量。
    pub rejected: [u64; 10],
    /// 正在序列化或等待 flush 的 JSON 预留字节。
    pub sending_bytes: usize,
    /// 冷却缓存实际条目数，包含在途预留及惰性清理前的过期条目。
    pub failed_code_entries: usize,
}

struct State {
    stats: ResourceSnapshot,
    start: TokenBucket,
    rooms: HashMap<i64, usize>,
    failed_codes: HashMap<[u8; 32], Option<Instant>>,
}

struct Inner {
    policy: ResourcePolicy,
    origin: Instant,
    state: Mutex<State>,
    // 冻结消息计费时钟必须影响实际采样，故保留仅测试编译的覆盖值。
    #[cfg(test)]
    message_elapsed: Mutex<Option<Duration>>,
}

/// 同一 server、Manager 克隆和进程内接入共用的资源预算。
#[derive(Clone)]
pub struct ResourceBudget(Arc<Inner>);

impl ResourceBudget {
    /// 创建预算，校验纯策略；所有名额均快速尝试获取，不排队。
    pub fn new(policy: ResourcePolicy) -> Result<Self, &'static str> {
        policy.validate()?;
        if Instant::now()
            .checked_add(policy.failed_code_cooldown)
            .is_none()
        {
            return Err("失败身份码冷却超出时钟范围");
        }
        Ok(Self(Arc::new(Inner {
            policy,
            origin: Instant::now(),
            #[cfg(test)]
            message_elapsed: Mutex::new(None),
            state: Mutex::new(State {
                stats: ResourceSnapshot::default(),
                start: TokenBucket::new(policy.start, Duration::ZERO)?,
                rooms: HashMap::new(),
                failed_codes: HashMap::new(),
            }),
        })))
    }

    /// 当前策略。
    pub fn policy(&self) -> ResourcePolicy {
        self.0.policy
    }

    /// 获取当前占用和累计计数的快照。
    pub fn snapshot(&self) -> ResourceSnapshot {
        let state = self.0.state.lock().unwrap();
        let mut stats = state.stats.clone();
        stats.failed_code_entries = state.failed_codes.len();
        stats
    }

    /// 立即尝试取得并发资源名额；拒绝时不排队。
    pub(crate) fn acquire(&self, resource: Resource) -> Result<Permit, Rejection> {
        let limit = match resource {
            Resource::Connection => self.0.policy.max_connections,
            Resource::Handshake => self.0.policy.max_handshakes,
            Resource::PendingAttach => self.0.policy.max_pending_attaches,
            Resource::StartingSession => self.0.policy.max_starting_sessions,
            Resource::Session => self.0.policy.max_sessions,
            _ => unreachable!("非并发资源"),
        };
        let mut state = self.0.state.lock().unwrap();
        if state.stats.occupied[resource as usize] >= limit {
            let rejection = Rejection {
                resource,
                retry_after_ms: Some(1000),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        state.stats.occupied[resource as usize] += 1;
        tracing::debug!(
            resource = resource.label(),
            occupied = state.stats.occupied[resource as usize],
            "资源名额已取得"
        );
        Ok(Permit {
            budget: self.clone(),
            resource,
        })
    }

    /// 取得房间订阅名额，由返回的守卫负责释放。
    pub(crate) fn subscribe(&self, room: i64) -> Result<RoomPermit, Rejection> {
        let mut state = self.0.state.lock().unwrap();
        if state.rooms.get(&room).copied().unwrap_or(0) >= self.0.policy.max_subscribers_per_room {
            let rejection = Rejection {
                resource: Resource::Subscriber,
                retry_after_ms: Some(1000),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        *state.rooms.entry(room).or_default() += 1;
        state.stats.subscribers += 1;
        tracing::debug!(subscribers = state.stats.subscribers, "活动订阅名额已取得");
        Ok(RoomPermit {
            budget: self.clone(),
            room,
        })
    }

    /// 消耗一次实际 start 调用的频率额度，并更新统计。
    pub(crate) fn start_code(&self, code: &str) -> Result<StartAttempt, Rejection> {
        let key: [u8; 32] = Sha256::digest(code.as_bytes()).into();
        let mut state = self.0.state.lock().unwrap();
        let now = Instant::now();
        state
            .failed_codes
            .retain(|_, until| until.is_none_or(|until| until > now));
        if let Some(until) = state.failed_codes.get(&key) {
            let rejection = Rejection {
                resource: if until.is_some() {
                    Resource::FailedCode
                } else {
                    Resource::StartingSession
                },
                retry_after_ms: Some(until.map_or(1000, |until| remaining_ms(until, now))),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        if state.failed_codes.len() >= self.0.policy.max_failed_codes {
            let retry = state
                .failed_codes
                .values()
                .flatten()
                .map(|until| remaining_ms(*until, now))
                .min()
                .unwrap_or(1000);
            let rejection = Rejection {
                resource: Resource::FailedCode,
                retry_after_ms: Some(retry),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        if let Err(ms) = state.start.take(self.0.origin.elapsed()) {
            let rejection = Rejection {
                resource: Resource::StartRate,
                retry_after_ms: Some(ms),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        state.stats.start_calls = state.stats.start_calls.saturating_add(1);
        state.failed_codes.insert(key, None);
        tracing::debug!(
            start_calls = state.stats.start_calls,
            "平台 start 额度已消耗"
        );
        Ok(StartAttempt {
            budget: self.clone(),
            key,
            pending: true,
        })
    }

    /// 序列化前预留单连接完整上限；完成后按实际长度缩减，额度不足立即拒绝。
    pub(crate) fn reserve_send(&self) -> Result<SendPermit, Rejection> {
        let bytes = self.0.policy.max_connection_send_bytes;
        let mut state = self.0.state.lock().unwrap();
        if bytes
            > self
                .0
                .policy
                .max_send_bytes
                .saturating_sub(state.stats.sending_bytes)
        {
            let rejection = Rejection {
                resource: Resource::SendingBytes,
                retry_after_ms: Some(1000),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        state.stats.sending_bytes += bytes;
        Ok(SendPermit {
            budget: self.clone(),
            bytes,
        })
    }

    /// 记录资源拒绝，并附带可选的毫秒重试建议。
    pub(crate) fn reject(&self, resource: Resource, retry_after_ms: Option<u64>) -> Rejection {
        let rejection = Rejection {
            resource,
            retry_after_ms,
        };
        record(&mut self.0.state.lock().unwrap().stats, rejection);
        rejection
    }

    /// 为单条连接创建独立的业务消息令牌桶。
    pub(crate) fn messages(&self) -> MessageBudget {
        MessageBudget {
            budget: self.clone(),
            origin: Instant::now(),
            bucket: TokenBucket::new(self.0.policy.client_message, Duration::ZERO)
                .expect("策略已校验"),
        }
    }
}

fn remaining_ms(until: Instant, now: Instant) -> u64 {
    until
        .saturating_duration_since(now)
        .as_nanos()
        .div_ceil(1_000_000)
        .min(u64::MAX as u128) as u64
}

/// 一次实际 start 的缓存预留，只有明确失败才启用冷却。
pub(crate) struct StartAttempt {
    budget: ResourceBudget,
    key: [u8; 32],
    pending: bool,
}

impl StartAttempt {
    /// 从收到失败结果时起冷却；已有会话复用与续传不会调用此方法。
    pub(crate) fn failed(mut self) {
        let mut state = self.budget.0.state.lock().unwrap();
        state.failed_codes.insert(
            self.key,
            Some(Instant::now() + self.budget.0.policy.failed_code_cooldown),
        );
        // 失败条目由缓存期限管理；旧守卫不能在到期后删除同码的新预留。
        self.pending = false;
    }
}

impl Drop for StartAttempt {
    fn drop(&mut self) {
        if !self.pending {
            return;
        }
        let mut state = self.budget.0.state.lock().unwrap();
        if state
            .failed_codes
            .get(&self.key)
            .is_some_and(Option::is_none)
        {
            state.failed_codes.remove(&self.key);
        }
    }
}

/// 在途 JSON 字节的所有权守卫，flush 完成或写端销毁后归还额度。
pub(crate) struct SendPermit {
    budget: ResourceBudget,
    bytes: usize,
}

impl SendPermit {
    /// 序列化结束后归还未使用的预留字节；不允许扩大预留。
    pub(crate) fn shrink(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes, "序列化输出不能超过预留上限");
        self.budget.0.state.lock().unwrap().stats.sending_bytes -= self.bytes - bytes;
        self.bytes = bytes;
    }
}

impl Drop for SendPermit {
    fn drop(&mut self) {
        self.budget.0.state.lock().unwrap().stats.sending_bytes -= self.bytes;
    }
}

fn record(stats: &mut ResourceSnapshot, rejection: Rejection) {
    let n = &mut stats.rejected[rejection.resource as usize];
    *n = n.saturating_add(1);
    tracing::warn!(
        resource = rejection.resource.label(),
        code = rejection.code(),
        retry_after_ms = rejection.retry_after_ms,
        rejections = *n,
        "资源准入被拒绝"
    );
}

/// 持有一份并发资源名额，析构时归还原预算。
pub(crate) struct Permit {
    budget: ResourceBudget,
    resource: Resource,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.budget.0.state.lock().unwrap();
        state.stats.occupied[self.resource as usize] -= 1;
        tracing::debug!(
            resource = self.resource.label(),
            occupied = state.stats.occupied[self.resource as usize],
            "资源名额已释放"
        );
    }
}

/// 单条连接的业务消息频率预算，拒绝计入共享统计。
pub(crate) struct MessageBudget {
    budget: ResourceBudget,
    origin: Instant,
    bucket: TokenBucket,
}

impl MessageBudget {
    /// 消耗一条业务消息的令牌；不足时返回重试建议。
    pub(crate) fn take(&mut self) -> Result<(), Rejection> {
        let now = self.origin.elapsed();
        #[cfg(test)]
        let now = self.budget.0.message_elapsed.lock().unwrap().unwrap_or(now);
        self.bucket
            .take(now)
            .map_err(|ms| self.budget.reject(Resource::ClientMessage, Some(ms)))
    }
}

/// 持有一个房间订阅名额，析构时释放订阅计数。
pub(crate) struct RoomPermit {
    budget: ResourceBudget,
    room: i64,
}

impl Drop for RoomPermit {
    fn drop(&mut self) {
        let mut state = self.budget.0.state.lock().unwrap();
        let count = state.rooms.get_mut(&self.room).expect("订阅名额已登记");
        *count -= 1;
        if *count == 0 {
            state.rooms.remove(&self.room);
        }
        state.stats.subscribers -= 1;
        tracing::debug!(subscribers = state.stats.subscribers, "活动订阅名额已释放");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl ResourceBudget {
        /// 单独验证 start 速率，不保留任何失败身份码缓存。
        fn start(&self) -> Result<(), Rejection> {
            self.start_code("test-code").map(drop)
        }
        /// 为非资源专项测试创建宽松预算，避免无关额度遮挡断言。
        pub(crate) fn for_tests() -> Self {
            Self::new(ResourcePolicy {
                max_connections: 8192,
                max_handshakes: 8192,
                max_pending_attaches: 8192,
                max_starting_sessions: 8192,
                max_sessions: 8192,
                max_subscribers_per_room: 8192,
                failed_code_cooldown: Duration::from_nanos(1),
                start: RatePolicy {
                    per_sec: 100_000,
                    burst: 100_000,
                },
                ..Default::default()
            })
            .unwrap()
        }

        /// 冻结消息计费时钟，避免网络调度延迟补充测试令牌。
        pub(crate) fn freeze_message_clock(&self) {
            *self.0.message_elapsed.lock().unwrap() = Some(Duration::ZERO);
        }
    }

    #[test]
    fn guards_and_shared_clones_release_once_and_track_rejections() {
        let budget = ResourceBudget::new(ResourcePolicy {
            max_connections: 1,
            max_handshakes: 1,
            ..Default::default()
        })
        .unwrap();
        let permit = budget.acquire(Resource::Connection).unwrap();
        assert_eq!(
            budget
                .clone()
                .acquire(Resource::Connection)
                .err()
                .unwrap()
                .code(),
            "server_busy"
        );
        assert_eq!(budget.snapshot().occupied[0], 1);
        drop(permit);
        assert_eq!(budget.snapshot().occupied[0], 0);
        assert_eq!(budget.snapshot().rejected[Resource::Connection as usize], 1);
    }

    #[tokio::test(start_paused = true)]
    async fn actual_start_quota_and_message_quota_replenish_without_affecting_control_frames() {
        let budget = ResourceBudget::new(ResourcePolicy {
            start: RatePolicy {
                per_sec: 1,
                burst: 1,
            },
            client_message: RatePolicy {
                per_sec: 2,
                burst: 2,
            },
            ..Default::default()
        })
        .unwrap();
        budget.start().unwrap();
        assert_eq!(budget.start().unwrap_err().retry_after_ms, Some(1000));
        let mut messages = budget.messages();
        messages.take().unwrap();
        messages.take().unwrap();
        assert_eq!(messages.take().unwrap_err().code(), "rate_limited");
        tokio::time::advance(Duration::from_millis(500)).await;
        messages.take().unwrap();
        assert!(budget.start().is_err());
        tokio::time::advance(Duration::from_millis(500)).await;
        budget.start().unwrap();
        assert_eq!(budget.snapshot().start_calls, 2);
        assert_eq!(budget.snapshot().occupied, [0; 5]);
    }
}
