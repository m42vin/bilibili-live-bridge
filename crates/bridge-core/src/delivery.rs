//! 与存储实现无关的交付模型。

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// 客户端最后连续处理完成的事件位置。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EventCursor {
    /// 进程内一次事件流的随机标识，原场次上游恢复时保持不变。
    pub stream_id: String,
    /// 桥侧序号；0 表示尚未处理第一条事件。
    pub seq: u64,
}

/// 无法继续完整读取的原因和当前保留范围。不会静默跳过事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResumeUnavailable {
    /// stream_unavailable、cursor_expired 或 cursor_ahead。
    pub reason: &'static str,
    /// 仍可续传的最小 resume_after.seq；流不可用时为空。
    pub earliest_after: Option<u64>,
    /// 桥已记录的最大序号；流不可用时为空。
    pub latest_seq: Option<u64>,
}

impl fmt::Display for ResumeUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "无法续传：{}", self.reason)
    }
}

impl std::error::Error for ResumeUnavailable {}

/// 已序列化、可共享的事件记录。
#[derive(Debug)]
pub struct RecordedEvent {
    /// 桥侧序号。
    pub seq: u64,
    /// 官方命令名。
    pub cmd: String,
    /// 已知事件序列化的数据，或未知命令原始数据。
    pub data: Box<RawValue>,
}
