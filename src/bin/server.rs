//! 预留的外部客户端服务入口。
//!
//! 当前只初始化日志并提示未实现，不绑定监听地址或提供接入协议。

// Copyright (c) 2026 Marvine
//
// Licensed under the Apache License, Version 2.0 <LICENCE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENCE-MIT or https://opensource.org/license/mit>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bilibili_live_bridge::logging::init()?;
    // TODO: Not implemented yet
    tracing::warn!("server 入口尚未实现监听服务");
    Ok(())
}
