//! 开放平台应用 API。
//!
//! 覆盖 `/v2/app/start`、`/v2/app/heartbeat`、`/v2/app/batchHeartbeat` 和 `/v2/app/end`。
//! 每次请求都按统一鉴权对这份 JSON 正文签名。Access Key 只留在 [`Client`] 里。

use std::collections::HashSet;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::Url;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::auth::{self, SIGNATURE_METHOD, SIGNATURE_VERSION, Signature};
use super::error::{ApiError, ErrorCode};
use crate::config::Config;

/// 正式环境域名。所有应用 API 都发到这里，请求本身不带查询参数。
pub const API_ORIGIN: &str = "https://live-open.biliapi.com";
/// 单次批量心跳允许的 `game_id` 数量。超过后开放平台返回 7004。
pub const MAX_BATCH_HEARTBEAT: usize = 200;

const START_PATH: &str = "/v2/app/start";
const END_PATH: &str = "/v2/app/end";
const HEARTBEAT_PATH: &str = "/v2/app/heartbeat";
const BATCH_HEARTBEAT_PATH: &str = "/v2/app/batchHeartbeat";

/// 对开放平台发起已签名的应用 API 请求。
///
/// 用 [`Client::from_config`] 或 [`Client::new`] 创建。`Clone` 共享同一个 HTTP 连接池。
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    access_key_id: String,
    access_key_secret: String,
    app_id: i64,
    origin: String,
}

impl Client {
    /// 用 Access Key 和项目 ID 创建客户端，请求发往 [`API_ORIGIN`]。
    #[must_use = "创建失败时需要处理 ApiError"]
    pub fn new(
        access_key_id: impl AsRef<str>,
        access_key_secret: impl AsRef<str>,
        app_id: i64,
    ) -> Result<Self, ApiError> {
        Self::with_origin(access_key_id, access_key_secret, app_id, API_ORIGIN)
    }

    /// 用进程配置创建客户端。
    #[must_use = "创建失败时需要处理 ApiError"]
    pub fn from_config(config: &Config) -> Result<Self, ApiError> {
        Self::new(
            config.access_key_id(),
            config.access_key_secret(),
            config.app_id(),
        )
    }

    /// 指定开放平台源站。集成测试用来指向本地桩。
    pub(crate) fn with_origin(
        access_key_id: impl AsRef<str>,
        access_key_secret: impl AsRef<str>,
        app_id: i64,
        origin: &str,
    ) -> Result<Self, ApiError> {
        let access_key_id = required_header_value(access_key_id, "Access Key Id 不能为空")?;
        let access_key_secret = required_text(access_key_secret, "Access Key Secret 不能为空")?;
        if app_id <= 0 {
            return Err(ApiError::invalid("app_id 必须是大于 0 的 i64"));
        }
        let origin = normalize_origin(origin)?;
        let http = reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|source| ApiError::transport("创建 HTTP 客户端失败", source))?;

        Ok(Self {
            http,
            access_key_id,
            access_key_secret,
            app_id,
            origin,
        })
    }

    /// 项目 ID。
    #[must_use]
    pub const fn app_id(&self) -> i64 {
        self.app_id
    }

    /// 用主播身份码开启一场互动。
    ///
    /// 成功后用 [`crate::open_live::ws::Connection::from_start`] 连接返回的 `wss_link`，
    /// 并在结束时调用 [`Client::end`]。项目心跳仍然走 [`Client::heartbeat`]。
    #[must_use = "开启失败时需要处理 ApiError"]
    pub async fn start(&self, code: &str) -> Result<StartResult, ApiError> {
        let code = required_text(code, "身份码不能为空")?;
        let value = self
            .execute(
                START_PATH,
                &StartRequest {
                    code: &code,
                    app_id: self.app_id,
                },
            )
            .await?;
        start_result_from_value(value)
    }

    /// 关闭一场互动，并同步下线互动道具。
    #[must_use = "关闭失败时需要处理 ApiError"]
    pub async fn end(&self, game_id: &str) -> Result<(), ApiError> {
        let game_id = required_text(game_id, "game_id 不能为空")?;
        self.execute(
            END_PATH,
            &EndRequest {
                app_id: self.app_id,
                game_id: &game_id,
            },
        )
        .await?;
        Ok(())
    }

    /// 维持一场互动。开放平台建议每 20 秒调用一次。
    ///
    /// 互动玩法超过 60 秒、插件和工具超过 180 秒没有项目心跳时，平台会关闭这场 `game_id`。
    #[must_use = "心跳失败时需要处理 ApiError"]
    pub async fn heartbeat(&self, game_id: &str) -> Result<(), ApiError> {
        let game_id = required_text(game_id, "game_id 不能为空")?;
        self.execute(HEARTBEAT_PATH, &HeartbeatRequest { game_id: &game_id })
            .await?;
        Ok(())
    }

    /// 一次为多场互动发送心跳。`game_ids` 去重后数量须在 1 到 [`MAX_BATCH_HEARTBEAT`] 之间。
    ///
    /// 调用本身成功时，仍然可能有一部分 `game_id` 失败，结果在 [`BatchHeartbeatResult::failed_game_ids`]。
    #[must_use = "批量心跳失败时需要处理 ApiError"]
    pub async fn batch_heartbeat(
        &self,
        game_ids: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<BatchHeartbeatResult, ApiError> {
        let game_ids = normalize_game_ids(game_ids)?;
        let value = self
            .execute(
                BATCH_HEARTBEAT_PATH,
                &BatchHeartbeatRequest {
                    game_ids: &game_ids,
                },
            )
            .await?;
        batch_result_from_value(value)
    }

    async fn execute(&self, path: &str, body: &impl Serialize) -> Result<Value, ApiError> {
        let body = serde_json::to_string(body)
            .map_err(|error| ApiError::invalid(format!("构造请求体失败：{error}")))?;
        let signed = auth::sign(
            &self.access_key_id,
            &self.access_key_secret,
            &body,
            unix_timestamp()?,
            &nonce(),
        );
        let headers = signed_headers(&self.access_key_id, &signed)?;
        let url = format!("{}{path}", self.origin);
        let response = self
            .http
            .post(&url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|source| ApiError::transport("请求开放平台失败", source))?;
        let status = response.status();
        if !status.is_success() {
            return Err(ApiError::Status {
                status: status.as_u16(),
            });
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|source| ApiError::transport("读取开放平台响应失败", source))?;
        success_value(&bytes)
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("access_key_id", &self.access_key_id)
            .field("access_key_secret", &"<redacted>")
            .field("app_id", &self.app_id)
            .field("origin", &self.origin)
            .finish()
    }
}

/// `/v2/app/start` 成功后的一场互动。
#[derive(Clone, PartialEq, Eq)]
pub struct StartResult {
    game_id: String,
    auth_body: String,
    wss_link: Vec<String>,
    anchor: Anchor,
}

impl StartResult {
    /// 场次 ID。项目心跳和 [`Client::end`] 使用这个值。
    #[must_use]
    pub fn game_id(&self) -> &str {
        &self.game_id
    }

    /// 官方 WebSocket 鉴权包正文。原样放进长连接的第一个包。
    #[must_use]
    pub fn auth_body(&self) -> &str {
        &self.auth_body
    }

    /// 官方 WebSocket 地址，按平台返回的可用顺序排列。
    ///
    /// 先连第一个，连接失败再换后面的集群。
    #[must_use]
    pub fn wss_link(&self) -> &[String] {
        &self.wss_link
    }

    /// 授权这场互动的主播。
    #[must_use]
    pub fn anchor(&self) -> &Anchor {
        &self.anchor
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
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

    #[cfg(test)]
    pub(crate) fn from_failed(failed_game_ids: Vec<String>) -> Self {
        Self { failed_game_ids }
    }
}

#[derive(Serialize)]
struct StartRequest<'a> {
    code: &'a str,
    app_id: i64,
}

#[derive(Serialize)]
struct EndRequest<'a> {
    app_id: i64,
    game_id: &'a str,
}

#[derive(Serialize)]
struct HeartbeatRequest<'a> {
    game_id: &'a str,
}

#[derive(Serialize)]
struct BatchHeartbeatRequest<'a> {
    game_ids: &'a [String],
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
    #[serde(default)]
    failed_game_ids: Vec<String>,
}

fn signed_headers(access_key_id: &str, signed: &Signature) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    insert_header(&mut headers, "x-bili-accesskeyid", access_key_id, false)?;
    insert_header(
        &mut headers,
        "x-bili-content-md5",
        &signed.content_md5,
        false,
    )?;
    insert_header(
        &mut headers,
        "x-bili-signature-method",
        SIGNATURE_METHOD,
        false,
    )?;
    insert_header(&mut headers, "x-bili-signature-nonce", &signed.nonce, false)?;
    insert_header(
        &mut headers,
        "x-bili-signature-version",
        SIGNATURE_VERSION,
        false,
    )?;
    insert_header(&mut headers, "x-bili-timestamp", &signed.timestamp, false)?;
    insert_header(
        &mut headers,
        AUTHORIZATION.as_str(),
        &signed.authorization,
        true,
    )?;
    Ok(headers)
}

fn insert_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
    sensitive: bool,
) -> Result<(), ApiError> {
    let name = HeaderName::from_static(name);
    let mut value =
        HeaderValue::from_str(value).map_err(|_| ApiError::invalid("请求头包含非法字符"))?;
    if sensitive {
        value.set_sensitive(true);
    }
    headers.insert(name, value);
    Ok(())
}

fn success_value(bytes: &[u8]) -> Result<Value, ApiError> {
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(ApiError::decode)?;
    if envelope.code != 0 {
        let request_id = envelope
            .request_id
            .filter(|request_id| !request_id.is_empty());
        return Err(ApiError::Platform {
            code: ErrorCode::from_raw(envelope.code),
            message: envelope.message,
            request_id,
        });
    }
    Ok(envelope.data.unwrap_or(Value::Null))
}

fn start_result_from_value(value: Value) -> Result<StartResult, ApiError> {
    if value.is_null() {
        return Err(ApiError::invalid("开放平台响应缺少 data"));
    }
    let body: StartBody = serde_json::from_value(value).map_err(ApiError::decode)?;
    let game_id = body.game_info.game_id;
    if game_id.is_empty() {
        return Err(ApiError::invalid("开放平台响应缺少 game_id"));
    }
    if body.websocket_info.auth_body.is_empty() {
        return Err(ApiError::invalid("开放平台响应缺少 auth_body"));
    }
    let wss_link: Vec<String> = body
        .websocket_info
        .wss_link
        .into_iter()
        .filter(|link| !link.is_empty())
        .collect();
    if wss_link.is_empty() {
        return Err(ApiError::invalid("开放平台响应缺少 wss_link"));
    }
    if body.anchor_info.room_id <= 0 {
        return Err(ApiError::invalid("开放平台响应缺少房间号"));
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

fn batch_result_from_value(value: Value) -> Result<BatchHeartbeatResult, ApiError> {
    let body = match value {
        Value::Null => BatchBody {
            failed_game_ids: Vec::new(),
        },
        other => serde_json::from_value(other).map_err(ApiError::decode)?,
    };
    Ok(BatchHeartbeatResult {
        failed_game_ids: body.failed_game_ids,
    })
}

fn normalize_game_ids(
    game_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<String>, ApiError> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for game_id in game_ids {
        let game_id = required_text(game_id.as_ref(), "game_id 不能为空")?;
        if !seen.insert(game_id.clone()) {
            return Err(ApiError::invalid("game_ids 存在重复"));
        }
        normalized.push(game_id);
        if normalized.len() > MAX_BATCH_HEARTBEAT {
            return Err(ApiError::invalid("game_ids 单次不能超过 200 个"));
        }
    }
    if normalized.is_empty() {
        return Err(ApiError::invalid("game_ids 不能为空"));
    }
    Ok(normalized)
}

fn required_text(value: impl AsRef<str>, empty_message: &'static str) -> Result<String, ApiError> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        return Err(ApiError::invalid(empty_message));
    }
    Ok(value.to_owned())
}

fn required_header_value(
    value: impl AsRef<str>,
    empty_message: &'static str,
) -> Result<String, ApiError> {
    let value = required_text(value, empty_message)?;
    if HeaderValue::from_str(&value).is_err() {
        return Err(ApiError::invalid("Access Key Id 包含非法字符"));
    }
    Ok(value)
}

fn normalize_origin(origin: &str) -> Result<String, ApiError> {
    let url = Url::parse(origin.trim()).map_err(|_| ApiError::invalid("开放平台地址无效"))?;
    if url.scheme() != "https" && url.scheme() != "http" {
        return Err(ApiError::invalid("开放平台地址必须是 http 或 https"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(ApiError::invalid("开放平台地址不能带查询参数"));
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err(ApiError::invalid("开放平台地址不能带路径"));
    }
    Ok(url.origin().ascii_serialization())
}

fn unix_timestamp() -> Result<u64, ApiError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| ApiError::invalid("系统时间不可用"))
}

fn nonce() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
#[path = "api_test.rs"]
mod api_test;
