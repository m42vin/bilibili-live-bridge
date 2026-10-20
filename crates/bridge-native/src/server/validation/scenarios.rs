//! 故障矩阵、速率验收和多轮收尾检查；结论只覆盖显式注入的本地模型。

use futures_util::future::try_join_all;
use tokio::{sync::oneshot, task::JoinSet};

use super::clients::Delivery;
use super::process::{self, Metrics};
use super::*;

fn metric(metrics: &Metrics, key: &str) -> f64 {
    process::value(metrics, key)
}
fn operation(metrics: &Metrics, operation: &str, outcome: &str) -> f64 {
    metric(
        metrics,
        &format!(
            "bridge_platform_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}}"
        ),
    )
}
fn zero_resources(metrics: &Metrics) -> bool {
    [
        "connection",
        "handshake",
        "pending_attach",
        "starting_session",
        "session",
    ]
    .iter()
    .all(|kind| {
        metric(
            metrics,
            &format!("bridge_resource_occupied{{resource=\"{kind}\"}}"),
        ) == 0.0
    }) && metric(metrics, "bridge_sending_bytes") == 0.0
        && metric(metrics, "bridge_subscribers") == 0.0
}
fn record(report: &mut Value, name: &str, details: Value) {
    eprintln!("[validation] {name}: {details}");
    report["cases"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":name,"passed":true,"details":details}));
}

#[derive(Clone, Copy)]
struct Sample {
    rss: u64,
    fds: usize,
    tasks: f64,
}
impl Sample {
    async fn read(service: &Service) -> Result<Self> {
        let metrics = service.metrics().await?;
        let (rss, fds) = process::os_resources(service.pid()).await?;
        Ok(Self {
            rss,
            fds,
            tasks: metric(&metrics, "bridge_runtime_alive_tasks"),
        })
    }
    fn json(&self) -> Value {
        json!({"rss_kib":self.rss,"fds":self.fds,"tasks":self.tasks})
    }
    fn merge(&mut self, other: Self) {
        self.rss = self.rss.max(other.rss);
        self.fds = self.fds.max(other.fds);
        self.tasks = self.tasks.max(other.tasks);
    }
}

fn recovered(after: Sample, warm: Sample) -> Result<()> {
    check(after.tasks <= warm.tasks + 2.0, "任务数未回落到暖机基线")?;
    check(after.fds <= warm.fds + 4, "文件描述符未回落到暖机基线")?;
    check(after.rss <= warm.rss + 65536, "暖机后 RSS 增长超过 64 MiB")
}

async fn quiescent(service: &Service, cursor: Value, index: usize) -> Result<Sample> {
    eventually(
        async || Ok(zero_resources(&service.metrics().await?)),
        "连接、会话及发送名额归零",
    )
    .await?;
    // 已结束日志的保留期限是模型的一部分；等到期限后显式访问旧流，驱动原有惰性淘汰。
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let expired = Consumer::connect(&service.url(), Some(cursor), index).await?;
    check(
        expired.ready()["type"] == "resume_unavailable",
        "过期流未明确拒绝",
    )?;
    expired.close().await;
    let mut stable = 0;
    eventually(
        async || {
            let metrics = service.metrics().await?;
            let clean = zero_resources(&metrics)
                && metric(&metrics, "bridge_event_log_bytes") == 0.0
                && metric(&metrics, "bridge_event_log_streams") == 0.0;
            stable = if clean { stable + 1 } else { 0 };
            Ok(stable >= 3)
        },
        "日志回收与资源连续稳定归零",
    )
    .await?;
    Sample::read(service).await
}

async fn consume_group(
    clients: Vec<Consumer>,
    target: usize,
    origin: Instant,
) -> Result<(Vec<Consumer>, Delivery)> {
    let mut jobs = JoinSet::new();
    for mut client in clients {
        jobs.spawn(async move {
            let delivery = client.receive_to(target, origin).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((client, delivery))
        });
    }
    let mut clients = Vec::new();
    let mut deliveries = Delivery::default();
    while let Some(result) = jobs.join_next().await {
        let (client, delivery) = result??;
        clients.push(client);
        deliveries.merge(delivery);
    }
    Ok((clients, deliveries))
}

async fn load_cycle(
    platform: &Platform,
    service: &Service,
    settings: &Settings,
) -> Result<(Value, Sample)> {
    let base = platform.emitted();
    let starts = platform.starts();
    let url = service.url();
    let clients =
        try_join_all((0..settings.clients).map(|_| Consumer::connect(&url, None, base))).await?;
    for client in &clients {
        check(client.ready()["type"] == "ready", "负载客户端未就绪")?;
    }
    let game = clients[0].game().to_owned();
    check(
        clients.iter().all(|client| client.game() == game),
        "同码没有复用场次",
    )?;
    let mut slow = Consumer::connect(&service.url(), None, base).await?;
    let cursor = clients[0].cursor();
    let pid = service.pid();
    let admin = service.admin();
    let (stop, stopped) = oneshot::channel();
    let monitor = tokio::spawn(async move {
        let mut peak = Sample {
            rss: 0,
            fds: 0,
            tasks: 0.0,
        };
        let mut stopped = std::pin::pin!(stopped);
        loop {
            let metrics = process::metrics(admin).await?;
            let (rss, fds) = process::os_resources(pid).await?;
            peak.merge(Sample {
                rss,
                fds,
                tasks: metric(&metrics, "bridge_runtime_alive_tasks"),
            });
            tokio::select! { _ = &mut stopped => return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(peak), _ = tokio::time::sleep(Duration::from_millis(500)) => {} }
        }
    });
    let count = settings.rate * settings.seconds as usize;
    let started = Instant::now();
    let (emission, consumers) = tokio::join!(
        platform.emit(&game, count, settings.rate, settings.payload),
        consume_group(clients, base + count, platform.clock())
    );
    emission?;
    let (clients, deliveries) = consumers?;
    let elapsed = started.elapsed().as_secs_f64();
    check(platform.starts() == starts + 1, "高并发同码接入重复 start")?;
    check(
        (count as f64 / elapsed) >= settings.rate as f64 * 0.8,
        format!("未达到目标速率：{} / {elapsed:.3}s", count),
    )?;
    check(
        deliveries.p95() <= 1000,
        format!("交付延迟上界过高：{}ms", deliveries.p95()),
    )?;
    // 明确跨越保留期限并追加一次，确保慢 ACK 消费者实际进入缺口路径。
    tokio::time::sleep_until(started + Duration::from_millis(2300)).await;
    platform
        .emit(&game, 1, settings.rate, settings.payload)
        .await?;
    let mut slow_events = 0;
    loop {
        let message = slow.next().await?;
        match message["type"].as_str() {
            Some("event") => {
                slow_events += 1;
                check(slow_events <= 64, "未确认事件突破 ACK 窗口")?;
            }
            Some("resume_unavailable") => break,
            Some("status") => {}
            other => return Err(format!("慢消费者未明确报告缺口：{other:?}").into()),
        }
    }
    check(slow_events == 64, "慢消费者未实际压满 ACK 窗口")?;
    slow.close().await;
    for client in clients {
        client.close().await;
    }
    eventually(
        async || Ok(zero_resources(&service.metrics().await?)),
        "负载结束后的名额归零",
    )
    .await?;
    let _ = stop.send(());
    let peak = monitor.await??;
    let after = quiescent(service, cursor, platform.emitted()).await?;
    check(platform.ends(&game) == 1, "负载会话未只清理一次")?;
    check(platform.active() == 0, "负载结束后模拟平台仍有活动场次")?;
    Ok((
        json!({"source_events":count,"effective_source_rate":count as f64/elapsed,"effective_deliveries_per_second":count as f64*settings.clients as f64/elapsed,"elapsed_seconds":elapsed,"delivery":deliveries.report(),"slow_unacked_events":slow_events,"observed_peak":peak.json(),"after_quiescence":after.json()}),
        after,
    ))
}

/// 运行全部本地验收；报告中记录真实观测，任何门槛不满足都会使命令失败。
pub(super) async fn run(settings: &Settings, report: &mut Value) -> Result<()> {
    let mut platform = Platform::start().await;
    let mut service = Service::start(&platform).await?;
    report["cold_baseline"] = Sample::read(&service).await?.json();
    let mut warm = None;
    let mut first = None;
    for cycle in 0..=settings.cycles {
        eprintln!("[validation] load cycle {cycle}/{}", settings.cycles);
        let (mut details, after) = load_cycle(&platform, &service, settings).await?;
        details["cycle"] = json!(cycle);
        details["warmup"] = json!(cycle == 0);
        report["load_cycles"].as_array_mut().unwrap().push(details);
        if let Some(warm) = warm {
            let warm: Sample = warm;
            recovered(after, warm)?;
            if let Some(first) = first {
                check(
                    after.rss <= first + 32768,
                    "多轮收尾 RSS 持续增长超过 32 MiB",
                )?;
            } else {
                first = Some(after.rss);
            }
        } else {
            warm = Some(after);
            report["warm_baseline"] = after.json();
        }
        platform.check_tasks();
    }
    record(
        report,
        "high_rate_slow_consumer_and_reclamation",
        json!({"cycles":settings.cycles,"all_quotas_zero":true,"tasks_and_fds_recovered":true,"rss_plateau_within_threshold":true}),
    );
    reconnect(&platform, &service, settings, report).await?;
    let post_reconnect = Sample::read(&service).await?;
    recovered(post_reconnect, warm.unwrap())?;
    report["post_reconnect"] = post_reconnect.json();
    faults(&platform, &service, report).await?;
    let post_faults = Sample::read(&service).await?;
    recovered(post_faults, warm.unwrap())?;
    report["post_faults"] = post_faults.json();
    lifecycle(&platform, &mut service, report).await?;
    platform.check_tasks();
    Ok(())
}

async fn reconnect(
    platform: &Platform,
    service: &Service,
    settings: &Settings,
    report: &mut Value,
) -> Result<()> {
    let base = platform.emitted();
    let url = service.url();
    let clients =
        try_join_all((0..settings.clients).map(|_| Consumer::connect(&url, None, base))).await?;
    let game = clients[0].game().to_owned();
    let starts = platform.starts();
    let (emitted, consumed) = tokio::join!(
        platform.emit(&game, 100, 2000, 64),
        consume_group(clients, base + 100, platform.clock())
    );
    emitted?;
    let (clients, _) = consumed?;
    let cursor = clients[0].cursor();
    drop(clients);
    platform.emit(&game, 7, 2000, 64).await?;
    let started = Instant::now();
    let resumed = try_join_all(
        (0..settings.clients).map(|_| Consumer::connect(&url, Some(cursor.clone()), base + 100)),
    )
    .await?;
    check(
        resumed
            .iter()
            .all(|client| client.ready()["resumed"] == true && client.game() == game),
        "集中重连未复用原流",
    )?;
    let elapsed = started.elapsed().as_secs_f64();
    let (resumed, deliveries) = consume_group(resumed, base + 107, platform.clock()).await?;
    check(platform.starts() == starts, "集中重连重复 start")?;
    let cursor = resumed[0].cursor();
    for client in resumed {
        client.close().await;
    }
    quiescent(service, cursor, platform.emitted()).await?;
    record(
        report,
        "reconnect_storm",
        json!({"clients":settings.clients,"resume_seconds":elapsed,"offline_replayed_events_per_client":7,"delivery":deliveries.report(),"additional_starts":0}),
    );
    Ok(())
}

async fn faults(platform: &Platform, service: &Service, report: &mut Value) -> Result<()> {
    platform.timeout_start();
    let started = Instant::now();
    let denied = Consumer::connect(&service.url(), None, platform.emitted()).await?;
    check(
        denied.ready()["code"] == "attach_failed",
        "平台超时没有明确失败",
    )?;
    denied.close().await;
    let elapsed = started.elapsed().as_secs_f64();
    check((9.0..14.0).contains(&elapsed), "平台请求未按十秒期限结束")?;
    eventually(
        async || Ok(zero_resources(&service.metrics().await?)),
        "超时接入释放名额",
    )
    .await?;
    check(
        operation(&service.metrics().await?, "start", "timeout") >= 1.0,
        "未记录实际 HTTP 超时",
    )?;
    record(
        report,
        "platform_timeout",
        json!({"elapsed_seconds":elapsed,"resources_released":true}),
    );
    // 冷却的期限是测试配置事实，等待后首次 start 会清除已过期保护。
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut client = Consumer::connect(&service.url(), None, platform.emitted()).await?;
    check(client.ready()["type"] == "ready", "超时后新接入未恢复")?;
    let game = client.game().to_owned();
    let cursor = client.cursor();
    let starts = platform.starts();
    platform.limit_heartbeat(true);
    eventually(
        async || {
            Ok(metric(
                &service.metrics().await?,
                "bridge_heartbeat_recovering_sessions",
            ) == 1.0)
        },
        "HTTP 429 进入心跳恢复",
    )
    .await?;
    check(
        service.get("/readyz").await?.0 == 200,
        "平台暂时限流使整个实例未就绪",
    )?;
    let limited = service.metrics().await?;
    check(
        operation(&limited, "batch_heartbeat", "http") >= 1.0,
        "未记录 429",
    )?;
    platform.limit_heartbeat(false);
    eventually(
        async || {
            Ok(metric(
                &service.metrics().await?,
                "bridge_heartbeat_recovering_sessions",
            ) == 0.0)
        },
        "429 解除后心跳恢复",
    )
    .await?;
    check(platform.ends(&game) == 0, "暂时限流误清理场次")?;
    restored_delivery(platform, &mut client).await?;
    record(
        report,
        "platform_rate_limit",
        json!({"ready_remains_200":true,"same_session_survives":true,"event_delivery_restored":true}),
    );

    let before = operation(&service.metrics().await?, "reconnect", "success");
    platform.auth_gate(false);
    platform.disconnect(&game).await;
    eventually(
        async || {
            Ok(metric(
                &service.metrics().await?,
                "bridge_upstream_sessions{state=\"reconnecting\"}",
            ) == 1.0)
        },
        "上游断线进入恢复",
    )
    .await?;
    check(
        service.get("/readyz").await?.0 == 200,
        "上游断线影响全局 ready",
    )?;
    platform.auth_gate(true);
    eventually(
        async || {
            let metrics = service.metrics().await?;
            Ok(operation(&metrics, "reconnect", "success") > before
                && metric(&metrics, "bridge_upstream_sessions{state=\"connected\"}") == 1.0)
        },
        "原场次重新鉴权",
    )
    .await?;
    check(platform.starts() == starts, "断线恢复重复 start")?;
    restored_delivery(platform, &mut client).await?;
    record(
        report,
        "upstream_disconnect",
        json!({"additional_starts":platform.starts()-starts,"same_game":platform.starts()==starts,"event_delivery_restored":true}),
    );

    platform.auth_gate(false);
    platform.silence(true);
    let ignored = platform.ignored_heartbeats();
    let started = Instant::now();
    eventually(
        async || {
            Ok(metric(
                &service.metrics().await?,
                "bridge_upstream_sessions{state=\"reconnecting\"}",
            ) == 1.0)
        },
        "上游静默达到活动期限",
    )
    .await?;
    check(
        platform.ignored_heartbeats() > ignored,
        "静默桩未实际收到心跳",
    )?;
    check(
        started.elapsed() >= Duration::from_secs(2),
        "静默故障没有经过活动期限",
    )?;
    platform.silence(false);
    platform.auth_gate(true);
    eventually(
        async || {
            Ok(metric(
                &service.metrics().await?,
                "bridge_upstream_sessions{state=\"connected\"}",
            ) == 1.0)
        },
        "静默恢复后的连接",
    )
    .await?;
    check(
        platform.starts() == starts && platform.ends(&game) == 0,
        "恢复过程重新开场或提前清理",
    )?;
    restored_delivery(platform, &mut client).await?;
    record(
        report,
        "upstream_silence",
        json!({"recovery_seconds":started.elapsed().as_secs_f64(),"ignored_heartbeats":platform.ignored_heartbeats()-ignored,"same_session":true,"event_delivery_restored":true}),
    );
    client.close().await;
    quiescent(service, cursor, platform.emitted()).await?;
    Ok(())
}

async fn restored_delivery(platform: &Platform, client: &mut Consumer) -> Result<()> {
    let game = client.game().to_owned();
    let target = platform.emitted() + 1;
    let (sent, received) = tokio::join!(
        platform.emit(&game, 1, 100, 64),
        client.receive_to(target, platform.clock())
    );
    sent?;
    received?;
    Ok(())
}

async fn lifecycle(platform: &Platform, service: &mut Service, report: &mut Value) -> Result<()> {
    let mut client = Consumer::connect(&service.url(), None, platform.emitted()).await?;
    let game = client.game().to_owned();
    let started = Instant::now();
    service.terminate()?;
    check(
        client.next().await?["type"] == "service_restarting",
        "SIGTERM 没有维护通知",
    )?;
    check(
        service.get("/readyz").await?.0 == 503 && service.get("/livez").await?.0 == 200,
        "收尾探针错误",
    )?;
    client.close().await;
    check(
        service.exited().await?.success(),
        format!("正常 SIGTERM 未成功退出：{}", service.log_tail()),
    )?;
    check(platform.ends(&game) == 1, "SIGTERM 没有只 end 一次")?;
    record(
        report,
        "sigterm",
        json!({"elapsed_seconds":started.elapsed().as_secs_f64(),"end_calls":1,"ready_during_drain":503}),
    );

    *service = Service::start(platform).await?;
    let base = platform.emitted();
    let mut client = Consumer::connect(&service.url(), None, base).await?;
    let game = client.game().to_owned();
    let (emitted, received) = tokio::join!(
        platform.emit(&game, 1, 100, 64),
        client.receive_to(base + 1, platform.clock())
    );
    emitted?;
    received?;
    let cursor = client.cursor();
    let old_stream = cursor["stream_id"].clone();
    service.kill()?;
    let status = service.exited().await?;
    use std::os::unix::process::ExitStatusExt;
    check(status.signal() == Some(9), "未观察到真实 SIGKILL 终止")?;
    check(
        platform.ends(&game) == 0 && platform.active() == 1,
        "SIGKILL 被当作优雅清理",
    )?;
    drop(client);
    record(
        report,
        "forced_termination",
        json!({"end_calls":0,"remote_session_remains":true,"process_reaped":true}),
    );

    *service = Service::start(platform).await?;
    let starts = platform.starts();
    let rejected = Consumer::connect(&service.url(), Some(cursor), platform.emitted()).await?;
    check(
        rejected.ready()["type"] == "resume_unavailable",
        "重启后旧流未明确失败",
    )?;
    rejected.close().await;
    check(platform.starts() == starts, "旧游标失败时自动新开场")?;
    eventually(
        async || Ok(platform.active() == 0),
        "模拟平台在失去心跳后自行回收孤儿场次",
    )
    .await?;
    let client = Consumer::connect(&service.url(), None, platform.emitted()).await?;
    check(
        client.ready()["type"] == "ready"
            && client.ready()["game_id"] != game
            && client.ready()["stream_id"] != old_stream,
        "显式首次订阅未建立新流",
    )?;
    record(
        report,
        "restart",
        json!({"old_cursor_rejected":true,"implicit_starts":0,"fresh_subscription_is_explicit":true,"mock_remote_expiry_observed":true}),
    );
    client.close().await;
    eventually(
        async || Ok(zero_resources(&service.metrics().await?)),
        "重启后名额收尾",
    )
    .await?;
    let mut client = Consumer::connect(&service.url(), None, platform.emitted()).await?;
    let blocked_game = client.game().to_owned();
    platform.hold_end(true);
    let started = Instant::now();
    service.terminate()?;
    check(
        client.next().await?["type"] == "service_restarting",
        "阻塞 end 时没有维护通知",
    )?;
    // 不提前结束平台阻塞，验证真实进程按预算返回失败。
    client.close().await;
    check(
        !service.exited().await?.success(),
        "退出预算耗尽后未以失败状态退出",
    )?;
    check(
        started.elapsed() >= Duration::from_secs(2) && started.elapsed() < Duration::from_secs(5),
        "退出预算未生效",
    )?;
    check(
        service.log_tail().contains("维护退出预算耗尽"),
        "缺少退出超时证据",
    )?;
    check(
        platform.ends(&blocked_game) == 1,
        "阻塞 end 注入没有实际生效",
    )?;
    platform.hold_end(false);
    record(
        report,
        "sigterm_blocked_cleanup",
        json!({"elapsed_seconds":started.elapsed().as_secs_f64(),"nonzero_exit":true,"remote_cleanup_not_guaranteed":true}),
    );
    Ok(())
}
