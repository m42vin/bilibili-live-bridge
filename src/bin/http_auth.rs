//! 测试 HTTP 鉴权
//! 通过单次的 HTTP 请求，获取游戏 ID，并进行心跳和结束游戏
//! 用于测试 HTTP 鉴权是否正常工作
//!
//! 需要从环境变量读取 `AUTH_CODE` 作为身份码

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::open_live::api::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    bilibili_live_bridge::logging::init()?;
    run().await
}

#[tracing::instrument(name = "http_auth", skip_all, err(Display))]
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::from_env()?;

    let client = Client::from_config(&config)?;

    let auth_code = std::env::var("AUTH_CODE")?;

    let start_result = client.start(&auth_code).await?;

    tracing::info!(
        game_id = start_result.game_id(),
        room_id = start_result.anchor().room_id,
        "HTTP 鉴权成功"
    );

    tokio::time::sleep(config.app_heartbeat()).await;

    client.heartbeat(start_result.game_id()).await?;
    client.end(start_result.game_id()).await?;

    Ok(())
}
