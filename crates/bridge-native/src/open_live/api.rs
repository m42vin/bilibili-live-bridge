//! 开放平台应用 API。
//!
//! 覆盖 `/v2/app/start`、`/v2/app/heartbeat`、`/v2/app/batchHeartbeat` 和 `/v2/app/end`。
//! 每次请求都按统一鉴权对实际发送的 JSON 正文签名，客户端保存签名凭据。
//! HTTP 状态错误与平台业务错误分别由 [`ApiError::Status`] 和 [`ApiError::Platform`] 表达。

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bridge_core::open_live::model::{
    self, BatchHeartbeatRequest, EndRequest, HeartbeatRequest, StartRequest,
};
pub use bridge_core::open_live::model::{Anchor, BatchHeartbeatResult, StartResult};
use reqwest::Url;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::auth::{self, SIGNATURE_METHOD, SIGNATURE_VERSION, Signature};
use super::error::ApiError;
use crate::config::Config;

/// 正式环境域名。所有应用 API 都发到这里，请求本身不带查询参数。
pub const API_ORIGIN: &str = "https://live-open.biliapi.com";
/// 单次批量心跳允许的 `game_id` 数量。超过后开放平台返回 7004。
pub use bridge_core::open_live::model::MAX_BATCH_HEARTBEAT;
/// 请求总超时；会话恢复预算与这个上限保持一致。
pub(crate) const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const START_PATH: &str = "/v2/app/start";
const END_PATH: &str = "/v2/app/end";
const HEARTBEAT_PATH: &str = "/v2/app/heartbeat";
const BATCH_HEARTBEAT_PATH: &str = "/v2/app/batchHeartbeat";

/// 对开放平台发起已签名的应用 API 请求。
///
/// 用 [`Client::from_config`] 或 [`Client::new`] 创建。`Clone` 共享同一个 HTTP 连接池。
/// 请求总超时为 10 秒、连接超时为 5 秒，不跟随重定向，也不使用系统代理。
/// 此类型不启动心跳任务，不会在析构时关闭已开启的场次。
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
    ///
    /// # Errors
    ///
    /// 凭据去掉首尾空白后为空、Access Key Id 不能作为请求头或 `app_id <= 0` 时
    /// 返回 [`ApiError::Invalid`]；HTTP 客户端初始化失败时返回 [`ApiError::Transport`]。
    #[must_use = "创建失败时需要处理 ApiError"]
    pub fn new(
        access_key_id: impl AsRef<str>,
        access_key_secret: impl AsRef<str>,
        app_id: i64,
    ) -> Result<Self, ApiError> {
        Self::with_origin(access_key_id, access_key_secret, app_id, API_ORIGIN)
    }

    /// 用进程配置创建客户端。
    ///
    /// # Errors
    ///
    /// 与 [`Client::new`] 相同，不在此处读取或修改进程环境变量。
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
                "bilibili-live-bridge",
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(REQUEST_TIMEOUT)
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
    /// 并在结束时调用 [`Client::end`]。项目心跳使用 [`Client::heartbeat`] 或
    /// [`Client::batch_heartbeat`]；丢弃返回值不会自动结束场次。
    ///
    /// # Errors
    ///
    /// 身份码去掉首尾空白后为空、响应缺少必要的场次信息、HTTP 请求失败、
    /// 响应无法解析或平台拒绝开启时返回 [`ApiError`]。
    #[must_use = "开启失败时需要处理 ApiError"]
    #[tracing::instrument(skip_all, fields(app_id = self.app_id))]
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
        let started = start_result_from_value(value)?;
        tracing::info!(
            game_id = started.game_id(),
            room_id = started.anchor().room_id,
            "已开启官方场次"
        );
        Ok(started)
    }

    /// 关闭一场互动，并同步下线互动道具。
    ///
    /// # Errors
    ///
    /// `game_id` 去掉首尾空白后为空，或 HTTP 请求、响应解析和平台处理失败时返回错误。
    #[must_use = "关闭失败时需要处理 ApiError"]
    #[tracing::instrument(skip_all, fields(game_id = game_id))]
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
        tracing::info!("已关闭官方场次");
        Ok(())
    }

    /// 维持一场互动。开放平台建议每 20 秒调用一次。
    ///
    /// 互动玩法超过 60 秒、插件和工具超过 180 秒没有项目心跳时，平台会关闭这场 `game_id`。
    /// 本方法只发送一次请求，不启动定时任务。
    ///
    /// # Errors
    ///
    /// `game_id` 去掉首尾空白后为空，或 HTTP 请求、响应解析和平台处理失败时返回错误。
    #[must_use = "心跳失败时需要处理 ApiError"]
    pub async fn heartbeat(&self, game_id: &str) -> Result<(), ApiError> {
        let game_id = required_text(game_id, "game_id 不能为空")?;
        self.execute(HEARTBEAT_PATH, &HeartbeatRequest { game_id: &game_id })
            .await?;
        tracing::debug!(%game_id, "项目心跳成功");
        Ok(())
    }

    /// 一次为多场互动发送心跳。`game_ids` 数量须在 1 到 [`MAX_BATCH_HEARTBEAT`] 之间。
    ///
    /// 每个 ID 去掉首尾空白后必须非空且互不重复；本方法不会自动去重或拆分请求。
    /// 调用本身成功时，仍然可能有一部分 `game_id` 失败，结果在 [`BatchHeartbeatResult::failed_game_ids`]。
    ///
    /// # Errors
    ///
    /// 列表为空、包含空白或重复 ID、数量超过上限时，在发送请求前返回
    /// [`ApiError::Invalid`]；HTTP 请求、响应解析或平台业务失败时也返回错误。
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
        let result = batch_result_from_value(value)?;
        tracing::debug!(
            batch_size = game_ids.len(),
            failed_count = result.failed_game_ids().len(),
            "批量项目心跳完成"
        );
        Ok(result)
    }

    #[tracing::instrument(
        name = "open_live_request",
        skip_all,
        fields(path = path, app_id = self.app_id)
    )]
    async fn execute(&self, path: &str, body: &impl Serialize) -> Result<Value, ApiError> {
        self.send_request(path, body).await.inspect_err(|error| {
            tracing::warn!(path, app_id = self.app_id, %error, "开放平台请求失败");
        })
    }

    async fn send_request(&self, path: &str, body: &impl Serialize) -> Result<Value, ApiError> {
        tracing::debug!("发送开放平台请求");
        let body = serde_json::to_string(body)
            .map_err(|error| ApiError::invalid(format!("构造请求体失败：{error}")))?;
        // 签名与发送必须使用同一份序列化结果，重新序列化可能改变正文的 MD5。
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
        tracing::debug!(status = status.as_u16(), "收到开放平台响应");
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

fn success_value(bytes: &[u8]) -> Result<Value, ApiError> {
    model::success_value(bytes).map_err(Into::into)
}

fn start_result_from_value(value: Value) -> Result<StartResult, ApiError> {
    model::start_result_from_value(value).map_err(Into::into)
}

fn batch_result_from_value(value: Value) -> Result<BatchHeartbeatResult, ApiError> {
    model::batch_result_from_value(value).map_err(Into::into)
}

fn normalize_game_ids(
    game_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<Vec<String>, ApiError> {
    model::normalize_game_ids(game_ids).map_err(Into::into)
}

fn required_text(value: impl AsRef<str>, message: &'static str) -> Result<String, ApiError> {
    model::required_text(value, message).map_err(Into::into)
}

#[cfg(test)]
#[path = "api_test.rs"]
mod api_test;
