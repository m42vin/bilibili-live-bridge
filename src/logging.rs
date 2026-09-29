//! 基于 tracing 的进程日志。
//!
//! 二进制入口在读取业务配置前调用 [`init`]。库本身只产生事件，调用方可以安装自己的 subscriber。

use std::env::VarError;
use std::io::{self, IsTerminal};

use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{LevelFilter, ParseError};

/// 初始化全局日志，输出到 stderr，包含时间、级别、模块和 span 上下文。
///
/// `RUST_LOG` 未设置或为空时使用 `info`；支持 `bilibili_live_bridge=debug` 等模块过滤。
/// 只有 stderr 连接终端时启用 ANSI 颜色。默认功能还会接收依赖通过 `log` 产生的日志。
///
/// # Errors
///
/// `RUST_LOG` 无法解析或全局 subscriber 已被安装时返回错误。每个进程只应调用一次。
pub fn init() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directives = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(value) => value,
        Err(VarError::NotPresent) => String::new(),
        Err(error) => return Err(error.into()),
    };
    let filter = filter(&directives).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("RUST_LOG 无效：{error}"),
        )
    })?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .with_ansi(io::stderr().is_terminal())
        .try_init()
}

fn filter(directives: &str) -> Result<EnvFilter, ParseError> {
    EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse(directives)
}

#[cfg(test)]
#[path = "logging_test.rs"]
pub(crate) mod logging_test;
