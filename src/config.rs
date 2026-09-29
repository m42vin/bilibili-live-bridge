//! 从进程环境变量加载的桥配置。
//!
//! Access Key 由配置传给开放平台 HTTP 客户端用于签名。这里不自动读取 `.env`；
//! `RUST_LOG` 由 [`crate::logging::init`] 读取，示例程序的 `AUTH_CODE` 由各入口读取。

use std::env::VarError;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// 开放平台 Access Key Id。
pub const ENV_ACCESS_KEY_ID: &str = "BILIBILI_ACCESS_KEY_ID";
/// 开放平台 Access Key Secret。签名只用这份密钥，不能写进日志或下游协议。
pub const ENV_ACCESS_KEY_SECRET: &str = "BILIBILI_ACCESS_KEY_SECRET";
/// 项目 ID，对应开放平台的 `app_id`，类型是 i64。
pub const ENV_APP_ID: &str = "BILIBILI_APP_ID";
/// 预留的直播桥监听地址。未设置时采用本机回环地址；当前没有实现监听服务。
pub const ENV_LISTEN: &str = "BRIDGE_LISTEN";
/// 官方 WebSocket 心跳间隔，单位秒。
pub const ENV_WEBSOCKET_HEARTBEAT_SECS: &str = "BRIDGE_WEBSOCKET_HEARTBEAT_SECS";
/// 项目心跳间隔，单位秒。
pub const ENV_APP_HEARTBEAT_SECS: &str = "BRIDGE_APP_HEARTBEAT_SECS";

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
}

impl Config {
    /// 从进程环境变量加载配置。
    ///
    /// 必填：`BILIBILI_ACCESS_KEY_ID`、`BILIBILI_ACCESS_KEY_SECRET`、`BILIBILI_APP_ID`。
    /// 可选：`BRIDGE_LISTEN`（默认 `127.0.0.1:8080`）、
    /// `BRIDGE_WEBSOCKET_HEARTBEAT_SECS` 与 `BRIDGE_APP_HEARTBEAT_SECS`（默认都是 20）。
    /// WebSocket 心跳必须小于 30 秒，项目心跳必须小于 60 秒。
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

        Ok(Self {
            access_key_id,
            access_key_secret,
            app_id,
            listen,
            websocket_heartbeat,
            app_heartbeat,
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

    /// 预留的下游监听地址。读取此值不会启动服务。
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
