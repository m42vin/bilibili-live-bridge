//! 测试同一身份码的多客户端接入。
//!
//! 用同一个身份码同时发起两个接入。两个客户端挂在同一场官方互动上，各自打印直播间事件。
//! 收到推送结束、两边都离开或 Ctrl-C 后关闭这场会话。
//!
//! 需要从环境变量读取 `AUTH_CODE` 作为身份码。

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::session::{Manager, RecvError, Subscription};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bilibili_live_bridge::logging::init()?;
    run().await
}

#[tracing::instrument(name = "multi_client", skip_all, err(Display))]
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_env()?;
    let manager = Manager::from_config(&config)?;
    let auth_code = std::env::var("AUTH_CODE")?;

    let (first, second) = tokio::try_join!(manager.attach(&auth_code), manager.attach(&auth_code))?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            tracing::info!("收到中断，准备关闭场次");
        }
        _ = async {
            tokio::join!(listen("客户端 1", first), listen("客户端 2", second));
        } => {
            tracing::info!("两个客户端都已离开");
        }
    }

    let _ = manager.shutdown().await;
    Ok(())
}

#[tracing::instrument(
    name = "subscription",
    skip_all,
    fields(client = name, room_id = subscription.room_id(), game_id = subscription.game_id())
)]
async fn listen(name: &str, mut subscription: Subscription) {
    loop {
        match subscription.recv().await {
            Ok(event) => {
                let stop = event.ends_push();
                tracing::info!(cmd = event.cmd(), "收到直播间事件");
                tracing::debug!(?event, "直播间事件内容");
                if stop {
                    tracing::info!("平台已停止这场推送");
                    break;
                }
            }
            Err(RecvError::Lagged { skipped }) => {
                tracing::warn!(skipped, "订阅落后，跳过直播间事件");
            }
            Err(RecvError::Closed) => {
                tracing::info!("这场会话的事件通道已关闭");
                break;
            }
        }
    }
}
