//! 原生共享资源预算、所有权名额和运行时计数。拒绝不排队，不消耗心跳或清理额度。
use bridge_core::resource::TokenBucket;
pub use bridge_core::resource::{RatePolicy, Rejection, Resource, ResourcePolicy};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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
    pub rejected: [u64; 8],
}
struct State {
    stats: ResourceSnapshot,
    start: TokenBucket,
    rooms: HashMap<i64, usize>,
}
struct Inner {
    policy: ResourcePolicy,
    origin: Instant,
    state: Mutex<State>,
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
        Ok(Self(Arc::new(Inner {
            policy,
            origin: Instant::now(),
            #[cfg(test)]
            message_elapsed: Mutex::new(None),
            state: Mutex::new(State {
                stats: ResourceSnapshot::default(),
                start: TokenBucket::new(policy.start, Duration::ZERO)?,
                rooms: HashMap::new(),
            }),
        })))
    }
    /// 当前策略。
    pub fn policy(&self) -> ResourcePolicy {
        self.0.policy
    }
    /// 获取当前占用和累计计数的快照。
    pub fn snapshot(&self) -> ResourceSnapshot {
        self.0.state.lock().unwrap().stats.clone()
    }
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
    pub(crate) fn start(&self) -> Result<(), Rejection> {
        let mut state = self.0.state.lock().unwrap();
        if let Err(ms) = state.start.take(self.0.origin.elapsed()) {
            let rejection = Rejection {
                resource: Resource::StartRate,
                retry_after_ms: Some(ms),
            };
            record(&mut state.stats, rejection);
            return Err(rejection);
        }
        state.stats.start_calls = state.stats.start_calls.saturating_add(1);
        tracing::debug!(
            start_calls = state.stats.start_calls,
            "平台 start 额度已消耗"
        );
        Ok(())
    }
    pub(crate) fn reject(&self, resource: Resource, retry_after_ms: Option<u64>) -> Rejection {
        let rejection = Rejection {
            resource,
            retry_after_ms,
        };
        record(&mut self.0.state.lock().unwrap().stats, rejection);
        rejection
    }
    pub(crate) fn messages(&self) -> MessageBudget {
        MessageBudget {
            budget: self.clone(),
            origin: Instant::now(),
            bucket: TokenBucket::new(self.0.policy.client_message, Duration::ZERO)
                .expect("策略已校验"),
        }
    }
    #[cfg(test)]
    pub(crate) fn freeze_message_clock(&self) {
        *self.0.message_elapsed.lock().unwrap() = Some(Duration::ZERO);
    }
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::new(ResourcePolicy {
            max_connections: 8192,
            max_handshakes: 8192,
            max_pending_attaches: 8192,
            max_starting_sessions: 8192,
            max_sessions: 8192,
            max_subscribers_per_room: 8192,
            start: RatePolicy {
                per_sec: 100_000,
                burst: 100_000,
            },
            ..Default::default()
        })
        .unwrap()
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
pub(crate) struct MessageBudget {
    budget: ResourceBudget,
    origin: Instant,
    bucket: TokenBucket,
}
impl MessageBudget {
    pub(crate) fn take(&mut self) -> Result<(), Rejection> {
        let now = self.origin.elapsed();
        #[cfg(test)]
        let now = self.budget.0.message_elapsed.lock().unwrap().unwrap_or(now);
        self.bucket
            .take(now)
            .map_err(|ms| self.budget.reject(Resource::ClientMessage, Some(ms)))
    }
}

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
