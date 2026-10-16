//! 会话接入失败。
//!
//! 这里保留调用方能处理的原因。Access Key、签名和 `auth_body` 不会出现在这些文本里。

use std::fmt;

use crate::open_live::error::{ApiError, ErrorCode};
use crate::open_live::ws::WsError;

/// 接入或维持一场会话失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    /// 共享资源准入拒绝，可按建议延迟重试。
    Resource(crate::resource::Rejection),
    /// 身份码为空，或心跳间隔无效。
    Invalid {
        /// 哪一项不满足约束。
        message: String,
    },
    /// `/v2/app/start` 被拒绝或没有完成。
    Start {
        /// 失败说明。
        message: String,
        /// 开放平台业务错误码。传输失败时为空。
        code: Option<ErrorCode>,
    },
    /// 官方长连接没有建立。`start` 已经成功时会尝试关闭这场 `game_id`。
    Connect {
        /// 长连接失败说明。
        message: String,
        /// 关闭场次也失败时的说明。
        end_message: Option<String>,
    },
    /// 这场会话在接入完成前结束，或事件通道已关闭。
    Closed,
    /// [`crate::session::Manager::shutdown`] 已经开始，不再接受新的接入。
    ShuttingDown,
}

impl SessionError {
    /// 构造会话参数错误，说明由调用方提供。
    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }

    /// 保存开场失败的说明和可选平台业务码。
    pub(super) fn from_start(error: ApiError) -> Self {
        Self::Start {
            code: error.platform_code(),
            message: error.to_string(),
        }
    }

    /// 保存建连失败，并附带场次结束失败的说明。
    pub(super) fn from_connect(error: WsError, end: Result<(), ApiError>) -> Self {
        let end_message = match end {
            Ok(()) => None,
            Err(error) => Some(error.to_string()),
        };
        Self::Connect {
            message: error.to_string(),
            end_message,
        }
    }

    /// `/v2/app/start` 返回的开放平台业务错误码。
    #[must_use]
    pub const fn platform_code(&self) -> Option<ErrorCode> {
        match self {
            Self::Start { code, .. } => *code,
            _ => None,
        }
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { message } | Self::Start { message, .. } => formatter.write_str(message),
            Self::Connect {
                message,
                end_message,
            } => match end_message {
                Some(end_message) => write!(formatter, "{message}；关闭场次失败：{end_message}"),
                None => formatter.write_str(message),
            },
            Self::Resource(error) => error.fmt(formatter),
            Self::Closed => formatter.write_str("这场会话已经结束"),
            Self::ShuttingDown => formatter.write_str("直播桥正在关闭"),
        }
    }
}

impl std::error::Error for SessionError {}
