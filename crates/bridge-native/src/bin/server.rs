//! WebSocket 服务入口，最后一个客户端离开后保留上游 30 秒，维护退出受总预算限制。

// Copyright (c) 2026 Marvine
//
// Licensed under the Apache License, Version 2.0 <LICENCE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENCE-MIT or https://opensource.org/license/mit>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::{future::Future, io, time::Duration};

use bilibili_live_bridge::{
    config::Config,
    downstream, logging, observability,
    open_live::api::Client,
    resource::ResourceBudget,
    session::{Manager, ManagerOptions},
};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

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
    let config = Config::from_env()?;
    let budget = ResourceBudget::new(config.resource_policy())?;
    let manager = Manager::with_policies_and_delivery(
        Client::from_config(&config)?,
        config.websocket_heartbeat(),
        config.app_heartbeat(),
        ManagerOptions {
            idle_grace: Duration::from_secs(30),
        },
        config.heartbeat_policy(),
        config.upstream_policy(),
        config.delivery_policy(),
    )?
    .with_resource_budget(budget)?;
    let listener = TcpListener::bind(config.listen()).await?;
    let admin_listener = TcpListener::bind(config.admin_listen()).await?;
    tracing::info!(listen = %listener.local_addr()?, path = "/ws", "直播桥已开始监听");
    tracing::info!(listen = %admin_listener.local_addr()?, "管理入口已开始监听");
    serve_with_admin(
        listener,
        admin_listener,
        manager,
        shutdown_signal(),
        config.shutdown_timeout(),
    )
    .await?;
    Ok(())
}

async fn serve_with_admin(
    listener: TcpListener,
    admin_listener: TcpListener,
    manager: Manager,
    shutdown: impl Future<Output = ()>,
    shutdown_timeout: Duration,
) -> io::Result<()> {
    let admin_manager = manager.clone();
    let (stop_admin, admin_stopped) = oneshot::channel();
    let (failed, admin_failed) = oneshot::channel();
    let admin = tokio::spawn(async move {
        let result = observability::serve(admin_listener, admin_manager, async {
            let _ = admin_stopped.await;
        })
        .await;
        if let Err(error) = &result {
            tracing::error!(%error, "管理入口停止，触发业务维护退出");
        }
        // 监听失败或请求任务 panic 都触发业务退出，避免失去探针后继续接入。
        let _ = failed.send(());
        result
    });
    let result = downstream::serve_with_shutdown_timeout(
        listener,
        manager,
        async {
            tokio::select! {
                _ = shutdown => {},
                _ = admin_failed => {},
            }
        },
        shutdown_timeout,
    )
    .await;
    let _ = stop_admin.send(());
    let admin_result = admin.await.map_err(io::Error::other)?;
    result.and(admin_result)
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
