//! 服务装配与业务、管理监听器的共同生命周期。

use std::{future::Future, io, time::Duration};

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::{
    config::Config,
    downstream, observability,
    open_live::api::Client,
    resource::ResourceBudget,
    session::{Manager, ManagerOptions},
};

/// 按配置启动服务，直到退出信号或监听失败，沿用配置的退出总预算。
///
/// 应通过退出信号结束并等待收尾，不以丢弃此 future 代替清理。
///
/// # Errors
/// 配置策略、平台客户端或监听器初始化失败，以及服务收尾失败时返回错误。
pub async fn run(
    config: Config,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let api = Client::from_config(&config)?;
    run_with_client(
        config,
        api,
        ManagerOptions {
            idle_grace: Duration::from_secs(30),
        },
        shutdown,
    )
    .await
}

async fn run_with_client(
    config: Config,
    api: Client,
    options: ManagerOptions,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let budget = ResourceBudget::new(config.resource_policy())?;
    let manager = Manager::with_policies_and_delivery(
        api,
        config.websocket_heartbeat(),
        config.app_heartbeat(),
        options,
        config.heartbeat_policy(),
        config.upstream_policy(),
        config.delivery_policy(),
    )?
    .with_resource_budget(budget)?;
    let listener = TcpListener::bind(config.listen()).await?;
    let admin = TcpListener::bind(config.admin_listen()).await?;
    tracing::info!(listen = %listener.local_addr()?, path = "/ws", "直播桥已开始监听");
    tracing::info!(listen = %admin.local_addr()?, "管理入口已开始监听");
    serve(
        listener,
        admin,
        manager,
        shutdown,
        config.shutdown_timeout(),
    )
    .await?;
    Ok(())
}

/// 驱动已绑定的业务和管理监听器，业务清理期间保留探针，收尾后停止管理请求。
///
/// # Errors
/// 任一监听或清理失败时返回 I/O 错误；不会额外等待管理请求的 I/O 期限。
async fn serve(
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

#[cfg(all(test, unix))]
#[path = "server/validation_test.rs"]
mod validation;

#[cfg(test)]
#[path = "server/server_test.rs"]
mod server_test;
