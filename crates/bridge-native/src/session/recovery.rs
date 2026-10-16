//! Tokio 单调时间的纯运算适配，当前时间仅由原生调用方采样。

use std::time::Duration;

use bridge_core::session::recovery::Moment;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
/// 包装 Tokio 单调时间，仅向共享状态提供纯时间运算。
pub(super) struct NativeMoment(pub(super) Instant);

impl Moment for NativeMoment {
    fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }

    fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}
