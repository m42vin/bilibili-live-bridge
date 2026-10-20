//! WebSocket 服务入口，最后一个客户端离开后保留上游 30 秒，维护退出受总预算限制。

// Copyright (c) 2026 Marvine
//
// Licensed under the Apache License, Version 2.0 <LICENCE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENCE-MIT or https://opensource.org/license/mit>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{future::Future, io};

use bilibili_live_bridge::{config::Config, logging, server};
use tokio::runtime::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    logging::init()?;
    run_runtime(run())?
}

fn run_runtime<F: Future>(future: F) -> io::Result<F::Output> {
    let runtime = Runtime::new()?;
    let result = runtime.block_on(future);
    // 服务已按总预算收尾；不能让运行时析构继续无限等待 DNS 等阻塞辅助任务。
    // 此入口随即退出进程，不将后台关闭策略用于嵌入式库调用方。
    runtime.shutdown_background();
    Ok(result)
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    server::run(Config::from_env()?, shutdown_signal()).await
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

#[cfg(test)]
#[path = "../downstream/server_test.rs"]
mod server_test;
