//! 二进制包及事件 JSON 的解析错误。
use std::fmt;

/// 纯协议解析错误，不包含连接或传输错误。
#[derive(Debug)]
pub enum ProtocolError {
    /// 参数不满足协议约束。
    Invalid {
        /// 错误说明，不包含包正文。
        message: String,
    },
    /// 二进制包不符合协议。
    Protocol {
        /// 错误说明，不包含包正文。
        message: String,
    },
    /// 业务 JSON 无法解析。
    Decode {
        /// 错误说明，不包含包正文。
        message: String,
    },
}
impl ProtocolError {
    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }
    pub(crate) fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: message.into(),
        }
    }
}
impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { message } | Self::Protocol { message } | Self::Decode { message } => {
                f.write_str(message)
            }
        }
    }
}
impl std::error::Error for ProtocolError {}
