//! 应用 API 的请求、响应模型与纯解析校验。

use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::{ErrorCode, ResponseError};

/// 单次批量心跳上限。
pub const MAX_BATCH_HEARTBEAT: usize = 200;

/// `/v2/app/start` 成功后的一场互动。
///
/// 保存场次、官方连接鉴权信息及主播资料，不持有网络连接。
/// `Debug` 隐去鉴权正文；丢弃此值不调用场次结束 API。
#[derive(Clone, PartialEq, Eq)]
pub struct StartResult {
    game_id: String,
    auth_body: String,
    wss_link: Vec<String>,
    anchor: Anchor,
}

impl StartResult {
    /// 场次 ID。项目心跳和场次结束 API 使用这个值。
    #[must_use]
    pub fn game_id(&self) -> &str {
        &self.game_id
    }

    /// 官方 WebSocket 鉴权包正文。原样放进长连接的第一个包。
    /// 此内容属于连接凭据，不应记录到日志或转发给订阅者。
    #[must_use]
    pub fn auth_body(&self) -> &str {
        &self.auth_body
    }

    /// 官方 WebSocket 地址，按平台返回的可用顺序排列。
    ///
    /// 运行时连接适配器在建连时按此顺序尝试地址。
    /// 鉴权被明确拒绝时停止尝试；低层 Connection 不自行重连；会话管理器在恢复时优先切换集群。
    #[must_use]
    pub fn wss_link(&self) -> &[String] {
        &self.wss_link
    }

    /// 授权这场互动的主播。
    #[must_use]
    pub fn anchor(&self) -> &Anchor {
        &self.anchor
    }

    /// 从运行时提供的字段构造结果；调用方负责保证字段有效。
    pub fn from_parts(
        game_id: impl Into<String>,
        auth_body: impl Into<String>,
        wss_link: Vec<String>,
        anchor: Anchor,
    ) -> Self {
        Self {
            game_id: game_id.into(),
            auth_body: auth_body.into(),
            wss_link,
            anchor,
        }
    }
}

impl fmt::Debug for StartResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartResult")
            .field("game_id", &self.game_id)
            .field("auth_body", &"<redacted>")
            .field("wss_link", &self.wss_link)
            .field("anchor", &self.anchor)
            .finish()
    }
}

/// 主播信息。`open_id` 是这个应用下的用户标识，`uid` 仍由平台返回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    /// 主播房间号。同一个项目下用它对应一场上游连接。
    pub room_id: i64,
    /// 主播昵称。
    pub uname: String,
    /// 主播头像地址。
    pub uface: String,
    /// 主播 uid。
    pub uid: i64,
    /// 主播在这个应用下的唯一标识。
    pub open_id: String,
    /// 主播在这个开发者下的唯一标识。未开通时为空字符串。
    pub union_id: String,
}

/// `/v2/app/batchHeartbeat` 的成功结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchHeartbeatResult {
    failed_game_ids: Vec<String>,
}

impl BatchHeartbeatResult {
    /// 这一批里心跳失败的场次。为空表示全部成功。
    #[must_use]
    pub fn failed_game_ids(&self) -> &[String] {
        &self.failed_game_ids
    }

    /// 这一批是否全部成功。
    #[must_use]
    pub fn all_succeeded(&self) -> bool {
        self.failed_game_ids.is_empty()
    }

    /// 从失败场次列表构造批量心跳结果。
    pub fn from_failed(failed_game_ids: Vec<String>) -> Self {
        Self { failed_game_ids }
    }
}

/// 开启互动场次的请求正文。
#[derive(Serialize)]
pub struct StartRequest<'a> {
    /// 主播身份码；错误消息中为错误分类。
    pub code: &'a str,
    /// 开放平台项目 ID。
    pub app_id: i64,
}

/// 结束互动场次的请求正文。
#[derive(Serialize)]
pub struct EndRequest<'a> {
    /// 开放平台项目 ID。
    pub app_id: i64,
    /// 平台互动场次标识。
    pub game_id: &'a str,
}

/// 单场项目心跳请求正文。
#[derive(Serialize)]
pub struct HeartbeatRequest<'a> {
    /// 平台互动场次标识。
    pub game_id: &'a str,
}

/// 批量项目心跳请求正文。
#[derive(Serialize)]
pub struct BatchHeartbeatRequest<'a> {
    /// 待发送项目心跳的场次列表。
    pub game_ids: &'a [String],
}

#[derive(Deserialize)]
struct Envelope {
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Deserialize)]
struct StartBody {
    game_info: GameInfoBody,
    websocket_info: WebsocketInfoBody,
    anchor_info: AnchorBody,
}

#[derive(Deserialize)]
struct GameInfoBody {
    game_id: String,
}

#[derive(Deserialize)]
struct WebsocketInfoBody {
    auth_body: String,
    #[serde(default)]
    wss_link: Vec<String>,
}

#[derive(Deserialize)]
struct AnchorBody {
    room_id: i64,
    #[serde(default)]
    uname: String,
    #[serde(default)]
    uface: String,
    #[serde(default)]
    uid: i64,
    #[serde(default)]
    open_id: String,
    #[serde(default)]
    union_id: String,
}

#[derive(Deserialize)]
struct BatchBody {
    /// 平台成功时也会把这个数组写成 JSON `null`。
    #[serde(default, deserialize_with = "empty_if_null")]
    failed_game_ids: Vec<String>,
}

fn empty_if_null<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<Vec<String>>::deserialize(deserializer)?.unwrap_or_default())
}

/// 解析响应信封，返回成功 data，非零业务码转为平台错误。
pub fn success_value(bytes: &[u8]) -> Result<Value, ResponseError> {
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(ResponseError::decode)?;
    if envelope.code != 0 {
        let request_id = envelope
            .request_id
            .filter(|request_id| !request_id.is_empty());
        return Err(ResponseError::Platform {
            code: ErrorCode::from_raw(envelope.code),
            message: envelope.message,
            request_id,
        });
    }
    Ok(envelope.data.unwrap_or(Value::Null))
}

/// 解析并校验开启结果；拒绝缺失场次、鉴权、地址和房间信息。
pub fn start_result_from_value(value: Value) -> Result<StartResult, ResponseError> {
    if value.is_null() {
        return Err(ResponseError::invalid("开放平台响应缺少 data"));
    }
    let body: StartBody = serde_json::from_value(value).map_err(ResponseError::decode)?;
    let game_id = body.game_info.game_id;
    if game_id.is_empty() {
        return Err(ResponseError::invalid("开放平台响应缺少 game_id"));
    }
    if body.websocket_info.auth_body.is_empty() {
        return Err(ResponseError::invalid("开放平台响应缺少 auth_body"));
    }
    let wss_link: Vec<String> = body
        .websocket_info
        .wss_link
        .into_iter()
        .filter(|link| !link.is_empty())
        .collect();
    if wss_link.is_empty() {
        return Err(ResponseError::invalid("开放平台响应缺少 wss_link"));
    }
    if body.anchor_info.room_id <= 0 {
        return Err(ResponseError::invalid("开放平台响应缺少房间号"));
    }
    Ok(StartResult {
        game_id,
        auth_body: body.websocket_info.auth_body,
        wss_link,
        anchor: Anchor {
            room_id: body.anchor_info.room_id,
            uname: body.anchor_info.uname,
            uface: body.anchor_info.uface,
            uid: body.anchor_info.uid,
            open_id: body.anchor_info.open_id,
            union_id: body.anchor_info.union_id,
        },
    })
}

/// 解析批量心跳结果，将 null 失败列表视为空列表。
pub fn batch_result_from_value(value: Value) -> Result<BatchHeartbeatResult, ResponseError> {
    let body = match value {
        Value::Null => BatchBody {
            failed_game_ids: Vec::new(),
        },
        other => serde_json::from_value(other).map_err(ResponseError::decode)?,
    };
    Ok(BatchHeartbeatResult {
        failed_game_ids: body
            .failed_game_ids
            .into_iter()
            .filter(|game_id| !game_id.is_empty())
            .collect(),
    })
}

/// 去掉场次 ID 首尾空白，拒绝空值、重复项及超出批量上限。
pub fn normalize_game_ids(
    game_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<String>, ResponseError> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for game_id in game_ids {
        let game_id = required_text(game_id.as_ref(), "game_id 不能为空")?;
        if !seen.insert(game_id.clone()) {
            return Err(ResponseError::invalid("game_ids 存在重复"));
        }
        normalized.push(game_id);
        if normalized.len() > MAX_BATCH_HEARTBEAT {
            return Err(ResponseError::invalid("game_ids 单次不能超过 200 个"));
        }
    }
    if normalized.is_empty() {
        return Err(ResponseError::invalid("game_ids 不能为空"));
    }
    Ok(normalized)
}

/// 去掉首尾空白，空值使用调用方提供的说明返回错误。
pub fn required_text(
    value: impl AsRef<str>,
    empty_message: &'static str,
) -> Result<String, ResponseError> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        return Err(ResponseError::invalid(empty_message));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
#[path = "model_test.rs"]
mod model_test;
