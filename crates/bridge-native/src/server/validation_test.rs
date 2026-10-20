//! 仅显式执行的本地故障及容量验收，子进程复用生产装配，不访问真实平台。

use std::{path::PathBuf, time::Duration};

use futures_util::FutureExt;
use serde_json::{Value, json};
use tokio::time::{Instant, timeout};

use super::*;

#[path = "validation/clients.rs"]
mod clients;
#[path = "validation/platform.rs"]
mod platform;
#[path = "validation/process.rs"]
mod process;
#[path = "validation/scenarios.rs"]
mod scenarios;

use clients::Consumer;
use platform::Platform;
use process::Service;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
struct Settings {
    clients: usize,
    rate: usize,
    seconds: u64,
    cycles: usize,
    payload: usize,
    output: PathBuf,
}

impl Settings {
    fn load() -> Self {
        fn number(key: &str, default: usize, max: usize) -> usize {
            let value =
                std::env::var(key).map_or(default, |v| v.parse().expect("验收参数必须是整数"));
            assert!((1..=max).contains(&value), "{key} 必须在 1..={max} 内");
            value
        }
        Self {
            clients: number("BRIDGE_VALIDATE_CLIENTS", 8, 64),
            rate: number("BRIDGE_VALIDATE_RATE", 2000, 50_000),
            seconds: number("BRIDGE_VALIDATE_SECONDS", 3, 3600) as u64,
            cycles: number("BRIDGE_VALIDATE_CYCLES", 3, 100),
            payload: number("BRIDGE_VALIDATE_PAYLOAD_BYTES", 256, 8192),
            output: std::env::var_os("BRIDGE_VALIDATE_REPORT").map_or_else(
                || PathBuf::from("../../target/validation-report.json"),
                PathBuf::from,
            ),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "只由本地验收父进程启动，不能作为普通平台测试执行"]
async fn child() {
    assert_eq!(std::env::var("BRIDGE_VALIDATION_CHILD").as_deref(), Ok("1"));
    let origin = std::env::var("BRIDGE_VALIDATION_ORIGIN").expect("缺少本地桩地址");
    let url = reqwest::Url::parse(&origin).unwrap();
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("127.0.0.1"), "验收只能连接本地桩");
    crate::logging::init().unwrap();
    let config = Config::from_env().unwrap();
    let api = Client::with_origin(
        config.access_key_id(),
        config.access_key_secret(),
        config.app_id(),
        &origin,
    )
    .unwrap()
    .with_response_limit(config.max_platform_response_bytes())
    .unwrap();
    run_with_client(
        config,
        api,
        ManagerOptions {
            idle_grace: Duration::from_secs(1),
        },
        async {
            let mut signal =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            signal.recv().await;
        },
    )
    .await
    .expect("验证进程服务失败");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "本地进程压测与故障演练，见 docs/validation.md"]
async fn acceptance() {
    let settings = Settings::load();
    assert!(settings.cycles >= 3, "回落验收至少需要三轮非暖机负载");
    assert!(
        settings.rate as u64 * settings.seconds >= 64,
        "每轮至少需要 64 个源事件以压满慢消费者窗口"
    );
    let mut report = json!({
        "schema_version":1,
        "generated_unix_seconds":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        "os":std::env::consts::OS,
        "arch":std::env::consts::ARCH,
        "release":!cfg!(debug_assertions),
        "settings":{"clients":settings.clients,"rate":settings.rate,"seconds":settings.seconds,"cycles":settings.cycles,"payload_bytes":settings.payload},
        "test_policy":{"idle_grace_seconds":1,"event_retention_seconds":2,"event_log_bytes":8388608,"shutdown_seconds":2,"upstream_activity_seconds":3,"mock_remote_expiry_seconds":5},
        "thresholds":{"quota_after_drain":0,"task_slack":2,"fd_slack":4,"rss_after_warmup_slack_kib":65536,"rss_growth_kib":32768,"minimum_rate_ratio":0.8,"p95_upper_ms":1000},
        "cases":[],"load_cycles":[]
    });
    // 每轮除负载外还要实际经过闲置与日志保留期限，并等待资源事实收敛。
    let deadline =
        Duration::from_secs((settings.seconds + 30) * (settings.cycles as u64 + 1) + 180);
    let outcome =
        std::panic::AssertUnwindSafe(timeout(deadline, scenarios::run(&settings, &mut report)))
            .catch_unwind()
            .await;
    let passed = matches!(&outcome, Ok(Ok(Ok(()))));
    report["passed"] = json!(passed);
    if !passed {
        report["error"] = json!(match &outcome {
            Ok(Ok(Err(error))) => error.to_string(),
            Ok(Err(_)) => "整体验收期限耗尽".into(),
            Err(error) => error
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| error.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "验收任务 panic".into()),
            _ => unreachable!(),
        });
    }
    if let Some(parent) = settings.output.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &settings.output,
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!(
        "[validation] report={} passed={passed}",
        settings.output.display()
    );
    match outcome {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => panic!("验收失败：{error}"),
        Ok(Err(_)) => panic!("整体验收期限耗尽"),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

fn check(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

async fn eventually(mut predicate: impl AsyncFnMut() -> Result<bool>, context: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if predicate().await? {
            return Ok(());
        }
        check(Instant::now() < deadline, format!("等待超时：{context}"))?;
        // 轮询 OS/HTTP 事实的采样周期，不用一次固定等待推断收尾完成。
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
