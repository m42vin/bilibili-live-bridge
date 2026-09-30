//! 面向本机客户端的 WebSocket 服务入口，最后一个客户端离开后保留上游 30 秒。

// Copyright (c) 2026 Marvine
//
// Licensed under the Apache License, Version 2.0 <LICENCE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENCE-MIT or https://opensource.org/license/mit>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::time::Duration;

use bilibili_live_bridge::{
    config::Config,
    downstream, logging,
    open_live::api::Client,
    session::{Manager, ManagerOptions},
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logging::init()?;
    let config = Config::from_env()?;
    let manager = Manager::with_options(
        Client::from_config(&config)?,
        config.websocket_heartbeat(),
        config.app_heartbeat(),
        ManagerOptions {
            idle_grace: Duration::from_secs(30),
        },
    )?;
    let listener = TcpListener::bind(config.listen()).await?;
    tracing::info!(listen = %listener.local_addr()?, path = "/ws", "直播桥已开始监听");
    downstream::serve(listener, manager, shutdown_signal()).await?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    if let Ok(mut terminate) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result { tracing::warn!(%error, "等待 Ctrl-C 失败"); }
            }
            _ = terminate.recv() => {}
        }
        return;
    }
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "等待 Ctrl-C 失败");
    }
}
