//! 会话的纯策略；异步调度和恢复状态暂由运行时管理。
pub mod failure;
pub mod policy;
pub use policy::{
    HealthPolicy, HeartbeatDiagnostic, HeartbeatPolicy, ReconnectPolicy, UpstreamPolicy,
};
