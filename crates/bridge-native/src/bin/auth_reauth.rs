//! 验证原场次 `auth_body` 能否在同一个 `game_id` 存续期间重新鉴权。
//!
//! 上游恢复策略依赖一条真实平台假设：释放旧长连接后，用开启时取得的
//! `auth_body` 再次握手仍能通过鉴权。本程序用一次真实场次验证它，不影响
//! 本地单元测试。
//!
//! 依次执行：
//! 1. 用身份码开启一场互动，并按 `wss_link` 顺序建立第一条长连接；
//! 2. 关闭第一条连接，换一个候选地址用原 `auth_body` 重新鉴权；
//! 3. 关闭第二条连接，再对原 `wss_link` 的第一项重新鉴权，检查恢复切换集群是否可行；
//! 4. 在恢复期间和结束后各调用一次项目心跳，检查场次是否仍然存续。
//!
//! 步骤 2 或 3 完成后观察一段 ``--wait-secs``，记录连接被平台静默关闭的时间。
//!
//! 需要 `BILIBILI_ACCESS_KEY_ID`、`BILIBILI_ACCESS_KEY_SECRET`、`BILIBILI_APP_ID`
//! 和 `AUTH_CODE` 环境变量。本程序不会读取 `.env`，请显式导出或用 shell 加载。
//!
//! ```sh
//! set -a && . ./.env && set +a
//! cargo run --locked --bin auth_reauth
//! ```
//!
//! 每次运行都会开启并结束一场真实互动，可能消耗平台额度。

use std::process::ExitCode;
use std::time::Duration;

use bilibili_live_bridge::config::Config;
use bilibili_live_bridge::open_live::api::{Client, StartResult};
use bilibili_live_bridge::open_live::ws::{Connection, HealthPolicy};
use tokio::time::{Instant, timeout};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

#[tracing::instrument(name = "auth_reauth", skip_all, err(Display))]
async fn run() -> Result<(), String> {
    bilibili_live_bridge::logging::init().map_err(|error| error.to_string())?;
    let args = Args::parse()?;
    let config = Config::from_env().map_err(|error| error.to_string())?;
    let client = Client::from_config(&config).map_err(|error| error.to_string())?;
    let auth_code = std::env::var("AUTH_CODE").map_err(|_| "缺少环境变量 AUTH_CODE".to_owned())?;

    tracing::info!(
        wait_secs = args.wait_secs,
        switch_cluster = !args.skip_cluster_switch,
        "开始验证原场次重新鉴权"
    );
    let started = client
        .start(&auth_code)
        .await
        .map_err(|error| format!("开启官方场次失败：{error}"))?;
    tracing::info!(
        game_id = started.game_id(),
        room_id = started.anchor().room_id,
        links = started.wss_link().len(),
        "官方场次已开启"
    );

    let result = probe(&client, &config, &started, &args).await;
    let ended = client.end(started.game_id()).await;
    if let Err(error) = &ended {
        tracing::error!(game_id = started.game_id(), %error, "关闭场次失败");
    }
    let conclusion = result?;
    // 关闭失败不影响已取得的验证结论；它只说明本地清理没有成功。
    ended.map_err(|error| format!("关闭官方场次失败：{error}"))?;
    render(&conclusion);
    Ok(())
}

/// 用一次真实场次验证重新鉴权；返回结论，不负责结束场次。
#[tracing::instrument(name = "probe", skip_all, fields(game_id = started.game_id()))]
async fn probe(
    client: &Client,
    config: &Config,
    started: &StartResult,
    args: &Args,
) -> Result<Conclusion, String> {
    let health = config.upstream_policy().health;
    let heartbeat = config.websocket_heartbeat();
    let mut steps = Vec::new();

    let mut first = connect_one(started, 0, heartbeat, health).await;
    steps.push(step(
        "首次鉴权（wss_link #0）",
        &first,
        "上游恢复的基线；它失败说明本次运行无法得出结论",
    ));
    first
        .as_mut()
        .map_err(|error| format!("首次鉴权未通过，无法验证重新鉴权：{error}"))?
        .close()
        .await
        .map_err(|error| error.to_string())?;

    let mut reused = connect_one(started, 1, heartbeat, health).await;
    steps.push(step(
        "原 auth_body 重新鉴权（wss_link #1，切换集群）",
        &reused,
        "核心假设：会话管理器恢复原场次时走的就是这条路径",
    ));
    if let Ok(connection) = reused.as_mut() {
        observe(connection, args.wait_secs, "重新鉴权的连接").await;
        connection
            .close()
            .await
            .map_err(|error| error.to_string())?;
    }

    let heartbeats = [
        ("释放旧连接后", client.heartbeat(started.game_id()).await),
        ("重新鉴权后", client.heartbeat(started.game_id()).await),
    ];
    for (label, result) in &heartbeats {
        match result {
            Ok(()) => tracing::info!(stage = label, "项目心跳仍然成功，场次存续"),
            Err(error) => tracing::warn!(stage = label, %error, "项目心跳失败"),
        }
    }

    let mut switched = None;
    if !args.skip_cluster_switch && started.wss_link().len() > 1 {
        let mut connection = connect_one(started, 0, heartbeat, health).await;
        steps.push(step(
            "原 auth_body 重新鉴权（回到 wss_link #0）",
            &connection,
            "恢复时优先切换集群，需要确认原鉴权不绑定首次地址",
        ));
        if let Ok(socket) = connection.as_mut() {
            socket.close().await.map_err(|error| error.to_string())?;
        }
        switched = Some(connection.is_ok());
    }

    Ok(Conclusion {
        steps,
        heartbeats: heartbeats.map(|(_, result)| result.is_ok()),
        switched,
    })
}

/// 连接 `wss_link` 的第 `index` 个候选地址；越界时退回第一项。
async fn connect_one(
    started: &StartResult,
    index: usize,
    heartbeat: Duration,
    health: HealthPolicy,
) -> Result<Connection, String> {
    let links = started.wss_link();
    let index = index.min(links.len().saturating_sub(1));
    let link = links[index].clone();
    tracing::info!(endpoint = %link, "尝试鉴权");
    Connection::connect_with_policy([link], started.auth_body(), heartbeat, health)
        .await
        .map_err(|error| format!("{error}"))
}

fn step(label: &str, result: &Result<Connection, String>, why: &str) -> Step {
    Step {
        label: label.to_owned(),
        accepted: result.is_ok(),
        detail: match result {
            Ok(_) => format!("鉴权通过（{why}）"),
            Err(error) => error.clone(),
        },
    }
}

/// 观察一条刚鉴权成功的连接；平台可能接受握手后立即静默关闭。
async fn observe(connection: &mut Connection, wait_secs: u64, label: &str) {
    if wait_secs == 0 {
        return;
    }
    let started_at = Instant::now();
    match timeout(Duration::from_secs(wait_secs), connection.recv()).await {
        Err(_) => tracing::info!(
            seconds = wait_secs,
            "观察期内连接保持打开，未被平台静默关闭"
        ),
        Ok(Ok(Some(event))) => tracing::info!(
            cmd = event.cmd(),
            seconds = started_at.elapsed().as_secs_f64(),
            "观察期内收到事件"
        ),
        Ok(Ok(None)) => tracing::warn!(
            seconds = started_at.elapsed().as_secs_f64(),
            "平台关闭了{label}，恢复假设不成立"
        ),
        Ok(Err(error)) => tracing::warn!(
            seconds = started_at.elapsed().as_secs_f64(),
            %error,
            "平台中断了{label}，恢复假设不成立"
        ),
    }
}

/// 全部步骤结束后打印结论，供人工判断和记录。
fn render(conclusion: &Conclusion) {
    let border = "=".repeat(72);
    eprintln!("\n{border}");
    eprintln!("原场次重新鉴权验证结果");
    eprintln!("{border}");
    for step in &conclusion.steps {
        let mark = if step.accepted { "通过" } else { "失败" };
        eprintln!("[{mark}] {}", step.label);
        eprintln!("       {}", step.detail);
    }
    let [after_release, after_reauth] = conclusion.heartbeats;
    eprintln!(
        "[{}] 项目心跳（释放旧连接后 / 重新鉴权后）：{} / {}",
        if after_release && after_reauth {
            "通过"
        } else {
            "失败"
        },
        yes_no(after_release),
        yes_no(after_reauth)
    );
    if let Some(switched) = conclusion.switched {
        eprintln!(
            "[{}] 切换候选集群后重新鉴权：{}",
            if switched { "通过" } else { "失败" },
            yes_no(switched)
        );
    }
    eprintln!("{border}");
    eprintln!("{}", conclusion.verdict());
    eprintln!("{border}\n");
}

fn yes_no(value: bool) -> &'static str {
    if value { "成功" } else { "失败" }
}

struct Conclusion {
    steps: Vec<Step>,
    /// `[释放旧连接后, 重新鉴权后]` 的项目心跳是否成功。
    heartbeats: [bool; 2],
    switched: Option<bool>,
}

impl Conclusion {
    /// 核心问题是首次重新鉴权（切换集群）是否被接受。
    fn verdict(&self) -> &'static str {
        let Some(second) = self.steps.get(1) else {
            return "结论：步骤不完整，无法判断";
        };
        if second.accepted {
            "结论：原 auth_body 可以被重新鉴权，恢复策略的前提成立；\n\
             仍需确认真实断线后的恢复窗口行为（见 --wait-secs 的观察输出）。"
        } else if self.steps[0].accepted {
            "结论：原 auth_body 无法重新鉴权，恢复策略依赖的前提不成立。\n\
             生产行为会结束订阅而不是重新 start，需要调整恢复设计或降级为人工重连。"
        } else {
            "结论：连首次鉴权都未通过，本次运行无法验证重新鉴权。"
        }
    }
}

struct Step {
    label: String,
    accepted: bool,
    detail: String,
}

/// 命令行参数；验证程序只做一次真实探测，不需要更多配置。
struct Args {
    wait_secs: u64,
    skip_cluster_switch: bool,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut args = Self {
            wait_secs: 0,
            skip_cluster_switch: false,
        };
        let mut rest = std::env::args().skip(1);
        while let Some(argument) = rest.next() {
            match argument.as_str() {
                "--wait-secs" => {
                    let value = rest.next().ok_or("--wait-secs 需要秒数")?;
                    args.wait_secs = value
                        .parse()
                        .map_err(|_| format!("--wait-secs 不是整数：{value}"))?;
                }
                "--skip-cluster-switch" => args.skip_cluster_switch = true,
                "--help" | "-h" => {
                    return Err(
                        "用法：auth_reauth [--wait-secs 秒] [--skip-cluster-switch]".to_owned()
                    );
                }
                other => return Err(format!("无法识别的参数：{other}")),
            }
        }
        Ok(args)
    }
}
