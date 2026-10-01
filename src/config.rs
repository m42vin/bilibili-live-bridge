//! 从进程环境变量加载的桥配置。
//!
//! Access Key 由配置传给开放平台 HTTP 客户端用于签名。这里不自动读取 `.env`；
//! `RUST_LOG` 由 [`crate::logging::init`] 读取，示例程序的 `AUTH_CODE` 由各入口读取。

use std::env::VarError;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::open_live::ws::HealthPolicy;
use crate::session::{HeartbeatPolicy, ReconnectPolicy, UpstreamPolicy};

/// 开放平台 Access Key Id。
pub const ENV_ACCESS_KEY_ID: &str = "BILIBILI_ACCESS_KEY_ID";
/// 开放平台 Access Key Secret。签名只用这份密钥，不能写进日志或下游协议。
pub const ENV_ACCESS_KEY_SECRET: &str = "BILIBILI_ACCESS_KEY_SECRET";
/// 项目 ID，对应开放平台的 `app_id`，类型是 i64。
pub const ENV_APP_ID: &str = "BILIBILI_APP_ID";
/// 下游 WebSocket 服务的监听地址。未设置时采用本机回环地址。
pub const ENV_LISTEN: &str = "BRIDGE_LISTEN";
/// 官方 WebSocket 心跳间隔，单位秒。
pub const ENV_WEBSOCKET_HEARTBEAT_SECS: &str = "BRIDGE_WEBSOCKET_HEARTBEAT_SECS";
/// 项目心跳间隔，单位秒。
pub const ENV_APP_HEARTBEAT_SECS: &str = "BRIDGE_APP_HEARTBEAT_SECS";
/// 项目心跳恢复期限，从最后确认成功起算，单位秒。
pub const ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS: &str =
    "BRIDGE_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS";
/// 项目心跳初始退避，单位毫秒。
pub const ENV_APP_HEARTBEAT_RETRY_INITIAL_MS: &str = "BRIDGE_APP_HEARTBEAT_RETRY_INITIAL_MS";
/// 项目心跳普通退避上限，单位毫秒。
pub const ENV_APP_HEARTBEAT_RETRY_MAX_MS: &str = "BRIDGE_APP_HEARTBEAT_RETRY_MAX_MS";

/// 官方长连接单次发送期限，单位秒。
pub const ENV_WEBSOCKET_WRITE_TIMEOUT_SECS: &str = "BRIDGE_WEBSOCKET_WRITE_TIMEOUT_SECS";
/// 官方长连接有效入站活动期限，单位秒。
pub const ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS: &str = "BRIDGE_WEBSOCKET_ACTIVITY_TIMEOUT_SECS";
/// 原场次上游恢复预算，单位秒；0 禁用恢复。
pub const ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS: &str = "BRIDGE_UPSTREAM_RECONNECT_TIMEOUT_SECS";
/// 上游恢复初始退避，单位毫秒。
pub const ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS: &str =
    "BRIDGE_UPSTREAM_RECONNECT_RETRY_INITIAL_MS";
/// 上游恢复退避上限，单位毫秒。
pub const ENV_UPSTREAM_RECONNECT_RETRY_MAX_MS: &str = "BRIDGE_UPSTREAM_RECONNECT_RETRY_MAX_MS";

const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
/// 官方文档推荐的心跳间隔。长连接约 30 秒收不到心跳会断开，互动玩法约 60 秒没有项目心跳会关闭场次。
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(20);
const WEBSOCKET_HEARTBEAT_MAX_SECS: u64 = 30;
const APP_HEARTBEAT_MAX_SECS: u64 = 60;

/// 直播桥进程配置。
///
/// 用 [`Config::from_env`] 加载，只读取进程环境变量，不自动加载 `.env`。
/// `Debug` 会隐去 Access Key Secret。
pub struct Config {
    access_key_id: String,
    access_key_secret: String,
    app_id: i64,
    listen: SocketAddr,
    websocket_heartbeat: Duration,
    app_heartbeat: Duration,
    heartbeat_policy: HeartbeatPolicy,
    upstream_policy: UpstreamPolicy,
}

impl Config {
    /// 从进程环境变量加载配置。
    ///
    /// 必填：`BILIBILI_ACCESS_KEY_ID`、`BILIBILI_ACCESS_KEY_SECRET`、`BILIBILI_APP_ID`。
    /// 可选：`BRIDGE_LISTEN`（默认 `127.0.0.1:8080`）、
    /// `BRIDGE_WEBSOCKET_HEARTBEAT_SECS` 与 `BRIDGE_APP_HEARTBEAT_SECS`（默认都是 20）。
    /// WebSocket 心跳必须小于 30 秒，项目心跳必须小于 60 秒。
    /// 项目心跳恢复期限默认 45 秒，初始退避默认 2000 毫秒、上限 10000 毫秒；
    /// 分别通过 BRIDGE_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS、
    /// BRIDGE_APP_HEARTBEAT_RETRY_INITIAL_MS、BRIDGE_APP_HEARTBEAT_RETRY_MAX_MS 配置。
    /// 项目心跳时间组合不安全时由管理器警告，配置值保持原样。
    /// 上游发送/活动期限默认 5/60 秒，恢复预算默认 120 秒（0 禁用），退避 2000/10000 毫秒。
    /// 活动期限必须大于长连接心跳间隔加发送期限，初始退避不得超过上限。
    /// 读取的文本去掉首尾空白；空白的可选变量使用默认值。
    ///
    /// # Errors
    ///
    /// 必填变量缺失或为空时返回 [`ConfigError::Missing`]；值不是 Unicode、
    /// 项目 ID 不为正整数、地址无法解析或间隔超出范围时返回 [`ConfigError::Invalid`]。
    #[must_use = "加载失败时需要处理 ConfigError"]
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_reader(|key| std::env::var(key))
    }

    fn from_reader(read: impl Fn(&str) -> Result<String, VarError>) -> Result<Self, ConfigError> {
        let access_key_id = required(&read, ENV_ACCESS_KEY_ID)?;
        let access_key_secret = required(&read, ENV_ACCESS_KEY_SECRET)?;
        let app_id = parse_app_id(&required(&read, ENV_APP_ID)?)?;
        let listen = match optional(&read, ENV_LISTEN)? {
            Some(value) => parse_listen(&value)?,
            None => DEFAULT_LISTEN,
        };
        let websocket_heartbeat = match optional(&read, ENV_WEBSOCKET_HEARTBEAT_SECS)? {
            Some(value) => parse_interval(
                &value,
                ENV_WEBSOCKET_HEARTBEAT_SECS,
                WEBSOCKET_HEARTBEAT_MAX_SECS,
            )?,
            None => DEFAULT_HEARTBEAT,
        };
        let app_heartbeat = match optional(&read, ENV_APP_HEARTBEAT_SECS)? {
            Some(value) => parse_interval(&value, ENV_APP_HEARTBEAT_SECS, APP_HEARTBEAT_MAX_SECS)?,
            None => DEFAULT_HEARTBEAT,
        };
        let defaults = HeartbeatPolicy::default();
        let heartbeat_policy = HeartbeatPolicy {
            failure_timeout: policy_duration(
                &read,
                ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS,
                defaults.failure_timeout,
                Duration::from_secs,
            )?,
            retry_initial_delay: policy_duration(
                &read,
                ENV_APP_HEARTBEAT_RETRY_INITIAL_MS,
                defaults.retry_initial_delay,
                Duration::from_millis,
            )?,
            retry_max_delay: policy_duration(
                &read,
                ENV_APP_HEARTBEAT_RETRY_MAX_MS,
                defaults.retry_max_delay,
                Duration::from_millis,
            )?,
        };

        let defaults = UpstreamPolicy::default();
        let health = HealthPolicy {
            write_timeout: policy_duration(
                &read,
                ENV_WEBSOCKET_WRITE_TIMEOUT_SECS,
                defaults.health.write_timeout,
                Duration::from_secs,
            )?,
            activity_timeout: policy_duration(
                &read,
                ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS,
                defaults.health.activity_timeout,
                Duration::from_secs,
            )?,
        };
        health
            .validate(websocket_heartbeat)
            .map_err(|message| ConfigError::Invalid {
                var: ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS,
                message: message.to_owned(),
            })?;
        let reconnect = match optional(&read, ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS)? {
            Some(value) if value.parse::<u64>().ok() == Some(0) => None,
            _ => {
                let defaults = ReconnectPolicy::default();
                Some(ReconnectPolicy {
                    failure_timeout: policy_duration(
                        &read,
                        ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS,
                        defaults.failure_timeout,
                        Duration::from_secs,
                    )?,
                    retry_initial_delay: policy_duration(
                        &read,
                        ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS,
                        defaults.retry_initial_delay,
                        Duration::from_millis,
                    )?,
                    retry_max_delay: policy_duration(
                        &read,
                        ENV_UPSTREAM_RECONNECT_RETRY_MAX_MS,
                        defaults.retry_max_delay,
                        Duration::from_millis,
                    )?,
                })
            }
        };
        let upstream_policy = UpstreamPolicy { health, reconnect };
        upstream_policy
            .validate(websocket_heartbeat)
            .map_err(|message| ConfigError::Invalid {
                var: ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS,
                message: message.to_owned(),
            })?;

        Ok(Self {
            access_key_id,
            access_key_secret,
            app_id,
            listen,
            websocket_heartbeat,
            app_heartbeat,
            heartbeat_policy,
            upstream_policy,
        })
    }

    /// 开放平台 Access Key Id，对应请求头 `x-bili-accesskeyid`。
    #[must_use]
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// 开放平台 Access Key Secret。只用于 HMAC-SHA256 签名。
    #[must_use]
    pub fn access_key_secret(&self) -> &str {
        &self.access_key_secret
    }

    /// 项目 ID。开放平台要求使用 i64，不能放进 32 位整数。
    #[must_use]
    pub const fn app_id(&self) -> i64 {
        self.app_id
    }

    /// 下游监听地址。`server` 在此地址提供 `/ws`；读取此值不会启动服务。
    #[must_use]
    pub const fn listen(&self) -> SocketAddr {
        self.listen
    }

    /// 官方 WebSocket 心跳发送间隔。
    #[must_use]
    pub const fn websocket_heartbeat(&self) -> Duration {
        self.websocket_heartbeat
    }

    /// 项目心跳发送间隔。会话管理器使用批量 API，低层调用方也可用于单场心跳。
    #[must_use]
    pub const fn app_heartbeat(&self) -> Duration {
        self.app_heartbeat
    }

    /// 项目心跳恢复策略。管理器构造时只警告不安全的时间组合，不修改原值。
    #[must_use]
    pub const fn heartbeat_policy(&self) -> HeartbeatPolicy {
        self.heartbeat_policy
    }
    /// 官方连接健康检查及原场次恢复策略。
    #[must_use]
    pub const fn upstream_policy(&self) -> UpstreamPolicy {
        self.upstream_policy
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("access_key_id", &self.access_key_id)
            .field("access_key_secret", &"<redacted>")
            .field("app_id", &self.app_id)
            .field("listen", &self.listen)
            .field("websocket_heartbeat", &self.websocket_heartbeat)
            .field("app_heartbeat", &self.app_heartbeat)
            .field("heartbeat_policy", &self.heartbeat_policy)
            .field("upstream_policy", &self.upstream_policy)
            .finish()
    }
}

/// 环境变量缺失或无法解析。
#[derive(Debug)]
pub enum ConfigError {
    /// 必填环境变量不存在，或去掉空白后为空。
    Missing {
        /// 环境变量名。
        var: &'static str,
    },
    /// 环境变量存在，但值不符合约束。
    Invalid {
        /// 环境变量名。
        var: &'static str,
        /// 值为什么不能用。不含密钥原文。
        message: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { var } => write!(formatter, "缺少必填环境变量 {var}"),
            Self::Invalid { var, message } => {
                write!(formatter, "环境变量 {var} 无效：{message}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

fn required(
    read: &impl Fn(&str) -> Result<String, VarError>,
    var: &'static str,
) -> Result<String, ConfigError> {
    optional(read, var)?.ok_or(ConfigError::Missing { var })
}

fn optional(
    read: &impl Fn(&str) -> Result<String, VarError>,
    var: &'static str,
) -> Result<Option<String>, ConfigError> {
    match read(var) {
        Ok(value) => {
            let value = value.trim();
            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value.to_owned()))
            }
        }
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(ConfigError::Invalid {
            var,
            message: "不是合法的 Unicode".to_owned(),
        }),
    }
}

fn policy_duration(
    read: &impl Fn(&str) -> Result<String, VarError>,
    key: &'static str,
    default: Duration,
    convert: impl Fn(u64) -> Duration,
) -> Result<Duration, ConfigError> {
    let Some(value) = optional(read, key)? else {
        return Ok(default);
    };
    let amount = value
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| ConfigError::Invalid {
            var: key,
            message: "必须是大于 0 的整数".to_owned(),
        })?;
    let duration = convert(amount);
    if std::time::Instant::now().checked_add(duration).is_none() {
        return Err(ConfigError::Invalid {
            var: key,
            message: "时长超出时钟范围".to_owned(),
        });
    }
    Ok(duration)
}

fn parse_app_id(value: &str) -> Result<i64, ConfigError> {
    let app_id = value.parse::<i64>().ok().filter(|app_id| *app_id > 0);
    app_id.ok_or_else(|| ConfigError::Invalid {
        var: ENV_APP_ID,
        message: "必须是大于 0 且能放进 i64 的整数".to_owned(),
    })
}

fn parse_listen(value: &str) -> Result<SocketAddr, ConfigError> {
    value.parse().map_err(|error| ConfigError::Invalid {
        var: ENV_LISTEN,
        message: format!("不是合法的套接字地址：{error}"),
    })
}

fn parse_interval(value: &str, var: &'static str, max_secs: u64) -> Result<Duration, ConfigError> {
    let interval = value
        .parse::<u64>()
        .ok()
        .filter(|secs| (1..max_secs).contains(secs))
        .map(Duration::from_secs);
    interval.ok_or_else(|| ConfigError::Invalid {
        var,
        message: format!("必须是小于 {max_secs} 的正整数秒"),
    })
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
