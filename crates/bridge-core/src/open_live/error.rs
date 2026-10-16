//! 平台业务错误码与响应校验错误。

use std::fmt;

/// 响应解析、参数校验或平台业务失败，不包含 HTTP 传输错误。
#[derive(Debug)]
pub enum ResponseError {
    /// 参数或响应缺少必要字段。
    Invalid {
        /// 错误说明，不包含请求凭据。
        message: String,
    },
    /// 响应或事件 JSON 无法解析。
    Decode {
        /// 错误说明，不包含请求凭据。
        message: String,
    },
    /// 平台返回了非零业务错误码。
    Platform {
        /// 平台业务错误码。
        code: ErrorCode,
        /// 错误说明，不包含请求凭据。
        message: String,
        /// 平台响应中的请求标识。
        request_id: Option<String>,
    },
}

impl ResponseError {
    /// 构造参数或响应校验错误，说明由调用方提供。
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: message.into(),
        }
    }

    /// 将解析失败说明保存为响应解码错误。
    pub(crate) fn decode(error: impl fmt::Display) -> Self {
        Self::Decode {
            message: error.to_string(),
        }
    }
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid { message } | Self::Decode { message } => f.write_str(message),
            Self::Platform {
                code,
                message,
                request_id,
            } => {
                write!(f, "开放平台返回 {}：{message}", code.raw())?;
                if let Some(id) = request_id {
                    write!(f, "（request_id: {id}）")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ResponseError {}

/// 开放平台业务错误码，编号与统一鉴权文档一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// 参数错误。
    BadParams,
    /// 应用无效。
    InvalidApp,
    /// 签名异常。
    BadSignature,
    /// 请求过期。
    ExpiredRequest,
    /// 重复请求。
    Replay,
    /// 签名 method 异常。
    BadSignatureMethod,
    /// 版本异常。
    BadVersion,
    /// IP 白名单限制。
    IpWhitelist,
    /// 权限异常。
    PermissionDenied,
    /// 接口访问限制。
    RateLimited,
    /// 接口不存在。
    NotFound,
    /// Content-Type 不是 `application/json`。
    BadContentType,
    /// MD5 校验失败。
    BadContentMd5,
    /// Accept 不是 `application/json`。
    BadAccept,
    /// 服务异常。
    ServiceUnavailable,
    /// 请求超时。
    Timeout,
    /// 内部错误。
    Internal,
    /// 配置错误。
    BadConfig,
    /// 房间白名单限制。未上架应用只能连接开发者自己的直播间。
    RoomWhitelist,
    /// 房间黑名单限制。
    RoomBlacklist,
    /// 应用权限限制。
    AppPermission,
    /// 验证码错误。
    BadCaptcha,
    /// 手机号码错误。
    BadPhoneNumber,
    /// 验证码已过期。
    CaptchaExpired,
    /// 验证码频率限制。
    CaptchaRateLimited,
    /// 房间号不能为空。
    EmptyRoomId,
    /// 没有查询到房间。
    RoomNotFound,
    /// 主播信息为空。
    EmptyAnchor,
    /// 互玩游戏关闭失败。
    EndGameFailed,
    /// 插件关闭失败。
    EndPluginFailed,
    /// 直播工具关闭失败。
    EndToolFailed,
    /// 当前房间未进行互动游戏。
    NotInGame,
    /// 上个游戏正在结算。
    Cooldown,
    /// 当前房间正在进行游戏。
    RoomBusy,
    /// 心跳过期，或 `game_id` 已关闭。
    HeartbeatExpired,
    /// 批量心跳超过 200 条。
    BatchTooLarge,
    /// 批量心跳的 `game_id` 重复。
    DuplicateGameId,
    /// 身份码错误。
    IdentityCode,
    /// 插件重复开启。
    PluginAlreadyStarted,
    /// 无道具投放权限。
    NoGiftPermission,
    /// 同一个应用在单个直播间最多 5 条连接。
    ConnectionLimit,
    /// 项目无权限访问。
    ProjectForbidden,
    /// 文档未列出的返回码。
    Other(i64),
}

impl ErrorCode {
    /// 把平台返回的整数转成错误码，未识别的值保留在 [`ErrorCode::Other`]。
    #[must_use]
    pub const fn from_raw(code: i64) -> Self {
        match code {
            4000 => Self::BadParams,
            4001 => Self::InvalidApp,
            4002 => Self::BadSignature,
            4003 => Self::ExpiredRequest,
            4004 => Self::Replay,
            4005 => Self::BadSignatureMethod,
            4006 => Self::BadVersion,
            4007 => Self::IpWhitelist,
            4008 => Self::PermissionDenied,
            4009 => Self::RateLimited,
            4010 => Self::NotFound,
            4011 => Self::BadContentType,
            4012 => Self::BadContentMd5,
            4013 => Self::BadAccept,
            5000 => Self::ServiceUnavailable,
            5001 => Self::Timeout,
            5002 => Self::Internal,
            5003 => Self::BadConfig,
            5004 => Self::RoomWhitelist,
            5005 => Self::RoomBlacklist,
            5011 => Self::AppPermission,
            6000 => Self::BadCaptcha,
            6001 => Self::BadPhoneNumber,
            6002 => Self::CaptchaExpired,
            6003 => Self::CaptchaRateLimited,
            6010 => Self::EmptyRoomId,
            6011 => Self::RoomNotFound,
            6012 => Self::EmptyAnchor,
            6013 => Self::EndGameFailed,
            6014 => Self::EndPluginFailed,
            6015 => Self::EndToolFailed,
            7000 => Self::NotInGame,
            7001 => Self::Cooldown,
            7002 => Self::RoomBusy,
            7003 => Self::HeartbeatExpired,
            7004 => Self::BatchTooLarge,
            7005 => Self::DuplicateGameId,
            7007 => Self::IdentityCode,
            7008 => Self::PluginAlreadyStarted,
            7009 => Self::NoGiftPermission,
            7010 => Self::ConnectionLimit,
            8002 => Self::ProjectForbidden,
            other => Self::Other(other),
        }
    }

    /// 平台返回体里的整数错误码。
    #[must_use]
    pub const fn raw(self) -> i64 {
        match self {
            Self::BadParams => 4000,
            Self::InvalidApp => 4001,
            Self::BadSignature => 4002,
            Self::ExpiredRequest => 4003,
            Self::Replay => 4004,
            Self::BadSignatureMethod => 4005,
            Self::BadVersion => 4006,
            Self::IpWhitelist => 4007,
            Self::PermissionDenied => 4008,
            Self::RateLimited => 4009,
            Self::NotFound => 4010,
            Self::BadContentType => 4011,
            Self::BadContentMd5 => 4012,
            Self::BadAccept => 4013,
            Self::ServiceUnavailable => 5000,
            Self::Timeout => 5001,
            Self::Internal => 5002,
            Self::BadConfig => 5003,
            Self::RoomWhitelist => 5004,
            Self::RoomBlacklist => 5005,
            Self::AppPermission => 5011,
            Self::BadCaptcha => 6000,
            Self::BadPhoneNumber => 6001,
            Self::CaptchaExpired => 6002,
            Self::CaptchaRateLimited => 6003,
            Self::EmptyRoomId => 6010,
            Self::RoomNotFound => 6011,
            Self::EmptyAnchor => 6012,
            Self::EndGameFailed => 6013,
            Self::EndPluginFailed => 6014,
            Self::EndToolFailed => 6015,
            Self::NotInGame => 7000,
            Self::Cooldown => 7001,
            Self::RoomBusy => 7002,
            Self::HeartbeatExpired => 7003,
            Self::BatchTooLarge => 7004,
            Self::DuplicateGameId => 7005,
            Self::IdentityCode => 7007,
            Self::PluginAlreadyStarted => 7008,
            Self::NoGiftPermission => 7009,
            Self::ConnectionLimit => 7010,
            Self::ProjectForbidden => 8002,
            Self::Other(code) => code,
        }
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod error_test;
