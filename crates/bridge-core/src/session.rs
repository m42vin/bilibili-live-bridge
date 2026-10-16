//! 会话的纯策略和恢复状态；异步调度由运行时管理。

pub mod failure;
pub mod policy;
pub use policy::{
    HealthPolicy, HeartbeatDiagnostic, HeartbeatPolicy, ReconnectPolicy, UpstreamPolicy,
};

pub mod recovery;

pub mod lifecycle;
