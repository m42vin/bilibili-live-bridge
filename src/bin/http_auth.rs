//! 测试 HTTP 鉴权
//! 通过单次的 HTTP 请求，获取游戏 ID，并进行心跳和结束游戏
//! 用于测试 HTTP 鉴权是否正常工作
//!
//! 需要从环境变量读取 `AUTH_CODE` 作为身份码

use std::future::Future;

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::open_live::api::Client;
use bilibili_live_bridge::open_live::error::ApiError;

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

    heartbeat_and_end(
        client.heartbeat(start_result.game_id()),
        client.end(start_result.game_id()),
    )
    .await?;

    Ok(())
}

/// 心跳失败也必须尝试结束；保留原错误，同时记录清理失败。
async fn heartbeat_and_end(
    heartbeat: impl Future<Output = Result<(), ApiError>>,
    end: impl Future<Output = Result<(), ApiError>>,
) -> Result<(), ApiError> {
    let heartbeat = heartbeat.await;
    let ended = end.await;
    if let Err(error) = &ended {
        tracing::error!(%error, "关闭场次失败");
    }
    heartbeat?;
    ended
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[tokio::test]
    async fn always_ends_after_heartbeat_and_preserves_the_first_error() {
        for heartbeat_fails in [false, true] {
            for end_fails in [false, true] {
                let calls = Mutex::new(Vec::new());
                let result = heartbeat_and_end(
                    async {
                        calls.lock().unwrap().push("heartbeat");
                        if heartbeat_fails {
                            Err(ApiError::Status { status: 503 })
                        } else {
                            Ok(())
                        }
                    },
                    async {
                        calls.lock().unwrap().push("end");
                        if end_fails {
                            Err(ApiError::Status { status: 500 })
                        } else {
                            Ok(())
                        }
                    },
                )
                .await;
                assert_eq!(*calls.lock().unwrap(), ["heartbeat", "end"]);
                match result {
                    Err(ApiError::Status { status }) => {
                        assert_eq!(status, if heartbeat_fails { 503 } else { 500 });
                    }
                    Ok(()) => assert!(!heartbeat_fails && !end_fails),
                    other => panic!("unexpected result: {other:?}"),
                }
            }
        }
    }
}
