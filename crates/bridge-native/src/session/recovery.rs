//! Tokio 单调时间的纯运算适配，当前时间仅由原生调用方采样。
use bridge_core::session::recovery::Moment;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct NativeMoment(pub(super) Instant);
impl Moment for NativeMoment {
    fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }
    fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.0.saturating_duration_since(earlier.0)
    }
}
