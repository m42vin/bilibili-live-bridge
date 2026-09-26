//! 官方 WebSocket 的失败类型。
//!
//! 这里不放 Access Key，也不把 `auth_body` 写进错误文本。

use std::fmt;

/// 连接或读取官方长连接失败。
#[derive(Debug)]
pub enum WsError {
    /// 还没发起连接，参数不满足协议约束。
    Invalid {
        /// 哪一项不满足约束。不含密钥或鉴权正文。
        message: String,
    },
    /// 给出的地址都没能完成 WebSocket 握手和鉴权。
    Connect {
        /// 每个地址的失败原因。
        message: String,
    },
    /// 读写 WebSocket 失败。
    Transport {
        /// 失败发生在哪一步。
        message: &'static str,
        /// tungstenite 或 IO 返回的原因。
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// 二进制包不符合长链协议。
    Protocol {
        /// 协议哪里对不上。不含包体。
        message: String,
    },
    /// 业务 JSON 无法解析。
    Decode {
        /// serde 的解析说明。不含推送正文。
        message: String,
    },
}

impl WsError {
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }

    pub(super) fn connect(message: impl Into<String>) -> Self {
        Self::Connect {
            message: message.into(),
        }
    }

    pub(super) fn transport(
        message: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Transport {
            message,
            source: Box::new(source),
        }
    }

    pub(super) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    pub(super) fn decode(message: impl Into<String>) -> Self {
        Self::Decode {
            message: message.into(),
        }
    }
}

impl fmt::Display for WsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { message }
            | Self::Connect { message }
            | Self::Protocol { message }
            | Self::Decode { message } => formatter.write_str(message),
            Self::Transport { message, source } => write!(formatter, "{message}：{source}"),
        }
    }
}

impl std::error::Error for WsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}
