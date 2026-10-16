//! 开放平台应用 API 的失败类型。
//!
//! 业务错误码与统一鉴权文档一致。HTTP 状态仍是 200 时，看响应体里的 `code`。

use std::fmt;

/// 调用应用 API 失败。
#[derive(Debug)]
pub enum ApiError {
    /// 参数不满足约束，或成功响应缺少必要的场次信息。
    Invalid {
        /// 参数或响应哪里不满足约束。不含密钥、身份码或鉴权正文。
        message: String,
    },
    /// HTTP 客户端或传输失败。
    Transport {
        /// 失败发生在哪一步。
        message: &'static str,
        /// reqwest 返回的原因。
        source: Box<reqwest::Error>,
    },
    /// 响应不是约定的 JSON 信封，或结果结构无法反序列化。
    Decode {
        /// serde 的解析说明。不含响应正文。
        message: String,
    },
    /// HTTP 状态不是成功。业务错误仍会以 200 和 [`ApiError::Platform`] 返回。
    Status {
        /// HTTP 状态码。
        status: u16,
    },
    /// 开放平台业务错误，响应体里的 `code` 不是 0。
    Platform {
        /// 平台错误码。
        code: ErrorCode,
        /// 平台返回的说明。
        message: String,
        /// 平台返回的请求 ID。
        request_id: Option<String>,
    },
}

impl ApiError {
    /// 构造参数或响应校验错误，说明由调用方提供。
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }

    /// 保留 HTTP 传输错误及其发生阶段。
    pub(super) fn transport(message: &'static str, source: reqwest::Error) -> Self {
        Self::Transport {
            message,
            source: Box::new(source),
        }
    }

    /// 开放平台业务错误码。传输、参数和解析错误返回 `None`。
    #[must_use]
    pub const fn platform_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Platform { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { message } | Self::Decode { message } => {
                write!(formatter, "{message}")
            }
            Self::Transport { message, source } => write!(formatter, "{message}：{source}"),
            Self::Status { status } => write!(formatter, "开放平台返回 HTTP {status}"),
            Self::Platform {
                code,
                message,
                request_id,
            } => {
                write!(formatter, "开放平台返回 {}：{message}", code.raw())?;
                if let Some(request_id) = request_id {
                    write!(formatter, "（request_id: {request_id}）")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

pub use bridge_core::open_live::error::ErrorCode;

impl From<bridge_core::open_live::error::ResponseError> for ApiError {
    fn from(error: bridge_core::open_live::error::ResponseError) -> Self {
        use bridge_core::open_live::error::ResponseError;
        match error {
            ResponseError::Invalid { message } => Self::Invalid { message },
            ResponseError::Decode { message } => Self::Decode { message },
            ResponseError::Platform {
                code,
                message,
                request_id,
            } => Self::Platform {
                code,
                message,
                request_id,
            },
        }
    }
}
