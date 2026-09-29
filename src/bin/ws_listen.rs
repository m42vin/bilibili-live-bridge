//! 测试官方 WebSocket 长连接。
//!
//! 用身份码开启一场互动，连接返回的 `wss_link`，并打印推送事件。
//! 长连接自己发送 WebSocket 心跳；本程序按配置间隔调用项目心跳。
//! 收到推送结束、连接关闭或 Ctrl-C 后调用 `/v2/app/end`。
//!
//! 需要从环境变量读取 `AUTH_CODE` 作为身份码。

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::open_live::api::{Client, StartResult};
use bilibili_live_bridge::open_live::ws::Connection;
use tokio::time::{MissedTickBehavior, interval};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bilibili_live_bridge::logging::init()?;
    run().await
}

#[tracing::instrument(name = "ws_listen", skip_all, err(Display))]
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_env()?;
    let client = Client::from_config(&config)?;
    let auth_code = std::env::var("AUTH_CODE")?;

    let started = client.start(&auth_code).await?;
    let session = tokio::select! {
        result = listen(&client, &config, &started) => result,
        result = tokio::signal::ctrl_c() => {
            result?;
            tracing::info!("收到中断，准备关闭场次");
            Ok(())
        }
    };

    let ended = client.end(started.game_id()).await;
    if let Err(error) = &ended {
        tracing::error!(game_id = started.game_id(), %error, "关闭场次失败");
    }
    session?;
    ended?;
    Ok(())
}

#[tracing::instrument(
    name = "session",
    skip_all,
    fields(game_id = started.game_id(), room_id = started.anchor().room_id)
)]
async fn listen(
    client: &Client,
    config: &Config,
    started: &StartResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::from_start(started, config.websocket_heartbeat()).await?;

    let mut ticks = interval(config.app_heartbeat());
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks.tick().await;

    loop {
        tokio::select! {
            incoming = connection.recv() => {
                match incoming? {
                    Some(event) => {
                        let stop = event.ends_push();
                        tracing::info!(cmd = event.cmd(), "收到直播间事件");
                        tracing::debug!(?event, "直播间事件内容");
                        if stop {
                            tracing::info!("平台已停止这场推送");
                            break;
                        }
                    }
                    None => {
                        tracing::info!("长连接已关闭");
                        break;
                    }
                }
            }
            _ = ticks.tick() => {
                client.heartbeat(started.game_id()).await?;
            }
        }
    }

    connection.close().await?;
    Ok(())
}
