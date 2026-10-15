//! 从运行时无关的失败摘要判断恢复方式。
use crate::open_live::error::ErrorCode;

/// HTTP 请求失败的最小摘要；原始错误由运行时保留用于日志。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiFailure {
    /// 参数或响应字段无效。
    Invalid,
    /// 网络传输失败或请求超时。
    Transport,
    /// 响应解析失败。
    Decode,
    /// 非成功 HTTP 状态码。
    HttpStatus(u16),
    /// 平台业务错误码。
    Platform(ErrorCode),
}
/// 官方连接失败的最小摘要。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamFailure {
    /// 本地参数无效。
    Invalid,
    /// 候选地址未能完成握手。
    Connect,
    /// 网络读写失败。
    Transport,
    /// 包协议错误或鉴权被拒绝。
    Protocol,
    /// 事件解码失败。
    Decode,
}
impl UpstreamFailure {
    /// 仅传输和候选地址连接失败允许恢复原场次。
    pub fn retryable(self) -> bool {
        matches!(self, Self::Transport | Self::Connect)
    }
}

/// 项目心跳错误的恢复分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// 永久错误，结束对应会话。
    Permanent,
    /// 可以在预算内重试。
    Retryable,
    /// 访问受限，需要降低频率。
    AccessLimited,
    /// 批量请求过期但无法定位具体场次。
    AmbiguousExpired,
    /// 未知平台错误，按有限恢复处理。
    Unknown,
}

impl FailureKind {
    /// 稳定的日志分类名称。
    pub fn label(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Retryable => "retryable",
            Self::AccessLimited => "access_limited",
            Self::AmbiguousExpired => "ambiguous_expired",
            Self::Unknown => "unknown",
        }
    }
}

/// 按错误类别和请求批次大小判断恢复方式。
pub fn classify(error: ApiFailure, batch_size: usize) -> FailureKind {
    match error {
        ApiFailure::Invalid => FailureKind::Permanent,
        ApiFailure::Transport | ApiFailure::Decode => FailureKind::Retryable,
        ApiFailure::HttpStatus(429) => FailureKind::AccessLimited,
        ApiFailure::HttpStatus(status)
            if (400..500).contains(&status) && !matches!(status, 408 | 425) =>
        {
            FailureKind::Permanent
        }
        ApiFailure::HttpStatus(_) => FailureKind::Retryable,
        ApiFailure::Platform(code) => match code.raw() {
            4003 | 4004 | 5000..=5002 => FailureKind::Retryable,
            4009 => FailureKind::AccessLimited,
            4000..=4002 | 4005..=4008 | 4010..=4013 | 5003..=5005 | 5011 | 7004 | 7005 | 8002 => {
                FailureKind::Permanent
            }
            7000 | 7003 if batch_size == 1 => FailureKind::Permanent,
            7000 | 7003 => FailureKind::AmbiguousExpired,
            _ => FailureKind::Unknown,
        },
    }
}

#[cfg(test)]
#[path = "failure_test.rs"]
mod failure_test;
