//! 测试同一身份码的多客户端接入。
//!
//! 用同一个身份码同时发起两个接入。两个客户端挂在同一场官方互动上，各自打印直播间事件。
//! 收到推送结束、两边都离开或 Ctrl-C 后关闭这场会话。
//!
//! 需要从环境变量读取 `AUTH_CODE` 作为身份码。

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::session::{Manager, RecvError, Subscription};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let manager = Manager::from_config(&config)?;
    let auth_code = std::env::var("AUTH_CODE")?;

    let (first, second) = tokio::try_join!(manager.attach(&auth_code), manager.attach(&auth_code))?;
    println!(
        "客户端 1：场次 {}，房间 {}（{}）",
        first.game_id(),
        first.room_id(),
        first.anchor().uname
    );
    println!(
        "客户端 2：场次 {}，房间 {}（{}）",
        second.game_id(),
        second.room_id(),
        second.anchor().uname
    );

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result?;
            println!("收到中断，准备关闭场次");
        }
        _ = async {
            tokio::join!(listen("客户端 1", first), listen("客户端 2", second));
        } => {
            println!("两个客户端都已离开");
        }
    }

    let _ = manager.shutdown().await;
    println!("已关闭场次");
    Ok(())
}

async fn listen(name: &str, mut subscription: Subscription) {
    loop {
        match subscription.recv().await {
            Ok(event) => {
                let stop = event.ends_push();
                println!("[{name}] {} {event:?}", event.cmd());
                if stop {
                    println!("[{name}] 平台已停止这场推送");
                    break;
                }
            }
            Err(RecvError::Lagged { skipped }) => {
                println!("[{name}] 订阅落后，跳过了 {skipped} 条直播间事件");
            }
            Err(RecvError::Closed) => {
                println!("[{name}] 这场会话的事件通道已关闭");
                break;
            }
        }
    }
}
