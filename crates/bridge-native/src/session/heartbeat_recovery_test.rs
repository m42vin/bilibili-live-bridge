use super::*;
use crate::session::recovery::NativeMoment;
use crate::session::{HeartbeatPolicy, Subscription};
use bridge_core::session::failure::FailureKind;
use bridge_core::session::recovery::{HeartbeatOutcome, HeartbeatRecovery};
use tokio::time::Instant;

fn platform_error(code: i64) -> ApiError {
    ApiError::Platform {
        code: ErrorCode::from_raw(code),
        message: CODE.to_owned(),
        request_id: None,
    }
}

fn success() -> Result<BatchHeartbeatResult, ApiError> {
    Ok(BatchHeartbeatResult::from_failed(Vec::new()))
}

async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

async fn advance_to(at: Instant) {
    tokio::time::advance(at.saturating_duration_since(Instant::now())).await;
    settle().await;
}

fn retry_at(manager: &Manager, game_id: &str) -> Instant {
    super::super::lock(&manager.state.beats)
        .get(game_id)
        .unwrap()
        .recovery
        .retry_at()
        .map(|at| at.0)
        .unwrap()
}

fn setup(mock: &Arc<Mock>, count: usize) {
    for id in 0..count {
        mock.push_ok(&format!("game-{id:03}"), id as i64 + 1);
        mock.push_connect_ok();
    }
}

async fn attach_rooms(manager: &Manager, count: usize) -> Vec<Subscription> {
    let mut subscriptions = Vec::new();
    for id in 0..count {
        subscriptions.push(manager.attach(&format!("code-{id}")).await.unwrap());
    }
    settle().await;
    subscriptions
}

fn policy_manager(mock: &Arc<Mock>, every: Duration, policy: HeartbeatPolicy) -> Manager {
    Manager::from_parts(
        mock.clone(),
        every,
        16,
        ManagerOptions::default(),
        policy,
        super::super::UpstreamPolicy {
            reconnect: None,
            ..super::super::UpstreamPolicy::default()
        },
    )
}

#[tokio::test(start_paused = true)]
async fn transient_error_recovers_and_returns_to_shared_schedule() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 1);
    mock.push_heartbeat(Err(platform_error(5001)));
    mock.push_heartbeat(success());
    mock.push_heartbeat(Err(platform_error(5002)));
    let manager = manager(&mock, Duration::from_secs(20));
    let mut subscriptions = attach_rooms(&manager, 1).await;
    advance_to(origin + Duration::from_secs(20)).await;
    assert_eq!(mock.batches().len(), 1);
    let first_retry = retry_at(&manager, "game-000");
    assert!(first_retry >= origin + Duration::from_millis(21600));
    assert!(first_retry <= origin + Duration::from_millis(22400));
    mock.connection(0).emit(danmaku("kept alive"));
    assert_eq!(text(&subscriptions[0].recv().await.unwrap()), "kept alive");
    advance_to(first_retry).await;
    {
        let beats = super::super::lock(&manager.state.beats);
        let slot = beats.get("game-000").unwrap();
        assert_eq!(slot.recovery.last_confirmed().0, first_retry);
        assert_eq!(slot.recovery.failures(), 0);
        assert!(slot.recovery.retry_at().is_none());
    }
    advance_to(origin + Duration::from_secs(40)).await;
    assert_eq!(
        mock.batches().len(),
        3,
        "recovery must not reset the shared interval"
    );
    {
        let beats = super::super::lock(&manager.state.beats);
        let slot = beats.get("game-000").unwrap();
        assert_eq!(slot.recovery.failures(), 1);
        assert_eq!(slot.recovery.last_confirmed().0, first_retry);
    }
    assert!(mock.ends().is_empty());
    assert_eq!(mock.heartbeat_peak.load(Ordering::SeqCst), 1);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn deadline_cancels_hanging_retry_and_cleans_once() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 1);
    mock.push_heartbeat(Err(platform_error(5001)));
    let (_gate, mut entered) = mock.hold_heartbeat(success());
    let manager = policy_manager(
        &mock,
        Duration::from_secs(20),
        HeartbeatPolicy {
            failure_timeout: Duration::from_secs(25),
            ..HeartbeatPolicy::default()
        },
    );
    let mut subscriptions = attach_rooms(&manager, 1).await;
    advance_to(origin + Duration::from_secs(20)).await;
    advance_to(retry_at(&manager, "game-000")).await;
    entered.wait_for(|entered| *entered).await.unwrap();
    assert_eq!(mock.heartbeat_active.load(Ordering::SeqCst), 1);
    advance_to(origin + Duration::from_secs(25)).await;
    assert_eq!(
        subscriptions[0].recv().await.unwrap_err(),
        RecvError::Closed
    );
    assert_eq!(mock.heartbeat_active.load(Ordering::SeqCst), 0);
    assert_eq!(mock.ends(), ["game-000"]);
    assert!(super::super::lock(&manager.state.beats).is_empty());
    let _ = manager.shutdown().await;
    assert_eq!(mock.end_attempts(), 1);
}

#[tokio::test(start_paused = true)]
async fn request_timeout_is_retryable_and_a_later_request_recovers() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 1);
    let (_gate, mut entered) = mock.hold_heartbeat(success());
    let manager = manager(&mock, Duration::from_secs(20));
    let _subscriptions = attach_rooms(&manager, 1).await;
    advance_to(origin + Duration::from_secs(20)).await;
    entered.wait_for(|entered| *entered).await.unwrap();
    advance_to(origin + Duration::from_secs(30)).await;
    assert_eq!(mock.heartbeat_active.load(Ordering::SeqCst), 0);
    assert_eq!(mock.batches().len(), 1);
    advance_to(retry_at(&manager, "game-000")).await;
    assert_eq!(mock.batches().len(), 2);
    {
        let beats = super::super::lock(&manager.state.beats);
        assert!(beats.get("game-000").unwrap().recovery.retry_at().is_none());
    }
    assert!(mock.ends().is_empty());
    assert_eq!(mock.heartbeat_peak.load(Ordering::SeqCst), 1);
    let _ = manager.shutdown().await;
}

#[tokio::test]
async fn permanent_error_closes_only_its_batch_immediately() {
    let mock = Mock::new();
    setup(&mock, 2);
    mock.push_heartbeat(Err(platform_error(4002)));
    let manager = manager(&mock, QUIET);
    let mut subscriptions = attach_rooms(&manager, 2).await;
    manager.beat_once().await;
    for subscription in &mut subscriptions {
        assert_eq!(subscription.recv().await.unwrap_err(), RecvError::Closed);
    }
    assert_eq!(mock.batches().len(), 1);
    assert_eq!(mock.end_attempts(), 2);
    let _ = manager.shutdown().await;
}

#[tokio::test]
async fn unknown_and_ambiguous_expiration_errors_preserve_multi_id_sessions() {
    for code in [912345, 7000, 7003] {
        let mock = Mock::new();
        setup(&mock, 2);
        mock.push_heartbeat(Err(platform_error(code)));
        let manager = manager(&mock, QUIET);
        let mut subscriptions = attach_rooms(&manager, 2).await;
        manager.beat_once().await;
        for (id, subscription) in subscriptions.iter_mut().enumerate() {
            mock.connection(id).emit(danmaku("still live"));
            assert_eq!(text(&subscription.recv().await.unwrap()), "still live");
        }
        assert!(mock.ends().is_empty());
        assert!(
            super::super::lock(&manager.state.beats)
                .values()
                .all(|slot| slot.recovery.retry_at().is_some())
        );
        let _ = manager.shutdown().await;
    }
}

#[tokio::test]
async fn single_id_expiration_is_terminal() {
    let mock = Mock::new();
    setup(&mock, 1);
    mock.push_heartbeat(Err(platform_error(7003)));
    let manager = manager(&mock, QUIET);
    let mut subscriptions = attach_rooms(&manager, 1).await;
    manager.beat_once().await;
    assert_eq!(
        subscriptions[0].recv().await.unwrap_err(),
        RecvError::Closed
    );
    assert_eq!(mock.ends(), ["game-000"]);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn access_limits_do_not_increase_request_frequency() {
    for error in [platform_error(4009), ApiError::Status { status: 429 }] {
        let origin = Instant::now();
        let mock = Mock::new();
        setup(&mock, 1);
        mock.push_heartbeat(Err(error));
        let manager = manager(&mock, Duration::from_secs(20));
        let _subscriptions = attach_rooms(&manager, 1).await;
        advance_to(origin + Duration::from_secs(20)).await;
        let retry = retry_at(&manager, "game-000");
        assert_eq!(retry, origin + Duration::from_secs(40));
        advance_to(retry - Duration::from_millis(1)).await;
        assert_eq!(mock.batches().len(), 1);
        advance_to(retry).await;
        assert_eq!(mock.batches().len(), 2);
        assert!(
            super::super::lock(&manager.state.beats)
                .get("game-000")
                .unwrap()
                .recovery
                .retry_at()
                .is_none()
        );
        let _ = manager.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn one_batch_backoff_does_not_block_other_normal_batches() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 201);
    mock.push_heartbeat(Err(platform_error(5001)));
    let mut manager = manager(&mock, Duration::from_secs(20));
    // 批量心跳跨 200 场测试显式扩大日志流数量；生产默认限制为 128。
    Arc::get_mut(&mut manager.state).unwrap().log =
        crate::session::delivery::EventLog::new(crate::session::DeliveryPolicy {
            max_streams: 256,
            ..crate::session::DeliveryPolicy::default()
        });
    let _subscriptions = attach_rooms(&manager, 201).await;
    advance_to(origin + Duration::from_secs(20)).await;
    mock.wait_batches(2).await;
    settle().await;
    let batches = mock.batches();
    assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [200, 1]);
    assert_eq!(batches[1], ["game-200"]);
    advance_to(retry_at(&manager, "game-000")).await;
    assert_eq!(mock.batches().len(), 3);
    assert_eq!(mock.batches()[2].len(), 200);
    assert_eq!(mock.heartbeat_peak.load(Ordering::SeqCst), 1);
    assert!(mock.ends().is_empty());
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn normal_tick_has_priority_when_retry_is_due_at_the_same_instant() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 2);
    let manager = manager(&mock, Duration::from_secs(20));
    let _subscriptions = attach_rooms(&manager, 2).await;
    {
        let mut beats = super::super::lock(&manager.state.beats);
        let retry = beats.get_mut("game-000").unwrap();
        retry.recovery.complete(
            NativeMoment(origin + Duration::from_secs(18)),
            HeartbeatOutcome::Failure(FailureKind::Retryable),
            100,
        );
    }
    manager.state.heartbeat_wake.notify_one();
    advance_to(origin + Duration::from_secs(20)).await;
    assert_eq!(
        mock.batches(),
        [vec!["game-001".to_owned()], vec!["game-000".to_owned()]],
        "request times: {:?}, now: {:?}",
        super::super::lock(&mock.inner)
            .batch_times
            .iter()
            .map(|at| at.duration_since(origin))
            .collect::<Vec<_>>(),
        Instant::now().duration_since(origin)
    );
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_slow_request_does_not_spawn_duplicate_heartbeats() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 1);
    let (gate, mut entered) = mock.hold_heartbeat(success());
    let manager = manager(&mock, Duration::from_secs(2));
    let _subscriptions = attach_rooms(&manager, 1).await;
    advance_to(origin + Duration::from_secs(2)).await;
    entered.wait_for(|entered| *entered).await.unwrap();
    advance_to(origin + Duration::from_secs(8)).await;
    assert_eq!(mock.batches().len(), 1);
    gate.send_replace(true);
    settle().await;
    assert_eq!(mock.batches().len(), 1);
    advance_to(origin + Duration::from_secs(10)).await;
    assert_eq!(mock.batches().len(), 2);
    assert_eq!(mock.heartbeat_peak.load(Ordering::SeqCst), 1);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn late_success_does_not_revive_an_expired_member_of_an_inflight_batch() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 2);
    mock.push_heartbeat(Err(platform_error(5001)));
    let (gate, mut entered) = mock.hold_heartbeat(success());
    let manager = manager(&mock, Duration::from_secs(20));
    let mut subscriptions = attach_rooms(&manager, 2).await;
    super::super::lock(&manager.state.beats)
        .get_mut("game-000")
        .unwrap()
        .recovery = HeartbeatRecovery::new(
        manager.state.heartbeat_policy,
        manager.state.app_heartbeat,
        NativeMoment(origin - Duration::from_secs(20)),
    );
    advance_to(origin + Duration::from_secs(20)).await;
    advance_to(retry_at(&manager, "game-000")).await;
    entered.wait_for(|entered| *entered).await.unwrap();
    advance_to(origin + Duration::from_secs(25)).await;
    assert_eq!(
        subscriptions[0].recv().await.unwrap_err(),
        RecvError::Closed
    );
    assert_eq!(mock.heartbeat_active.load(Ordering::SeqCst), 1);
    gate.send_replace(true);
    settle().await;
    assert!(!super::super::lock(&manager.state.beats).contains_key("game-000"));
    {
        let beats = super::super::lock(&manager.state.beats);
        assert!(beats.get("game-001").unwrap().recovery.retry_at().is_none());
    }
    mock.connection(1).emit(danmaku("survives"));
    assert_eq!(text(&subscriptions[1].recv().await.unwrap()), "survives");
    let _ = manager.shutdown().await;
    assert_eq!(mock.end_attempts(), 2);
}

#[tokio::test(start_paused = true)]
async fn late_failure_does_not_close_a_replacement_registration() {
    let origin = Instant::now();
    let mock = Mock::new();
    setup(&mock, 2);
    mock.push_heartbeat(Err(platform_error(5001)));
    let (gate, mut entered) = mock.hold_heartbeat(Ok(BatchHeartbeatResult::from_failed(vec![
        "game-000".to_owned(),
    ])));
    let manager = manager(&mock, Duration::from_secs(20));
    let mut subscriptions = attach_rooms(&manager, 2).await;
    advance_to(origin + Duration::from_secs(20)).await;
    advance_to(retry_at(&manager, "game-000")).await;
    entered.wait_for(|entered| *entered).await.unwrap();
    drop(subscriptions.remove(0));
    mock.wait_ends(1).await;
    mock.push_ok("game-000", 3);
    mock.push_connect_ok();
    let mut replacement = manager.attach("replacement").await.unwrap();
    gate.send_replace(true);
    settle().await;
    {
        let beats = super::super::lock(&manager.state.beats);
        assert!(beats.get("game-000").unwrap().recovery.retry_at().is_none());
        assert!(!beats.get("game-000").unwrap().in_flight);
    }
    mock.connection(2).emit(danmaku("new registration"));
    assert_eq!(text(&replacement.recv().await.unwrap()), "new registration");
    let _ = manager.shutdown().await;
    assert_eq!(mock.end_attempts(), 3);
}

#[tokio::test(start_paused = true)]
async fn shutdown_does_not_wait_for_backoff_or_a_hanging_request() {
    for held in [false, true] {
        let origin = Instant::now();
        let mock = Mock::new();
        setup(&mock, 1);
        let _gate = if held {
            Some(mock.hold_heartbeat(success()).0)
        } else {
            mock.push_heartbeat(Err(platform_error(5001)));
            None
        };
        let manager = manager(&mock, Duration::from_secs(20));
        let _subscriptions = attach_rooms(&manager, 1).await;
        advance_to(origin + Duration::from_secs(20)).await;
        let before = Instant::now();
        timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .unwrap();
        assert_eq!(Instant::now(), before);
        assert_eq!(mock.end_attempts(), 1);
        assert_eq!(mock.heartbeat_active.load(Ordering::SeqCst), 0);
        assert!(super::super::lock(&manager.state.beats).is_empty());
    }
}

#[test]
fn unsafe_policy_warns_without_changing_values_or_blocking_construction() {
    let (dispatch, logs) = crate::logging::logging_test::capture("warn");
    let policy = HeartbeatPolicy {
        failure_timeout: Duration::from_secs(120),
        retry_initial_delay: Duration::from_secs(20),
        retry_max_delay: Duration::from_secs(10),
    };
    tracing::dispatcher::with_default(&dispatch, || {
        let api = crate::open_live::api::Client::new("key", "secret", 1).unwrap();
        let manager = Manager::with_heartbeat_policy(
            api,
            Duration::from_secs(20),
            Duration::from_secs(120),
            ManagerOptions::default(),
            policy,
        )
        .unwrap();
        assert_eq!(manager.state.app_heartbeat, Duration::from_secs(120));
        assert_eq!(manager.state.heartbeat_policy, policy);
    });
    let output = logs.output();
    assert!(output.contains("超过建议的 45 秒"));
    assert!(output.contains("未给恢复期限留出余量"));
    assert!(output.contains("初始退避超过上限"));
    assert!(!output.contains("secret"));
}

#[tokio::test(start_paused = true)]
async fn recovery_logs_include_budget_and_codes_without_platform_message() {
    let (dispatch, logs) = crate::logging::logging_test::capture("warn");
    async {
        let mock = Mock::new();
        setup(&mock, 1);
        mock.push_heartbeat(Err(platform_error(912345)));
        let manager = manager(&mock, QUIET);
        let _subscriptions = attach_rooms(&manager, 1).await;
        manager.beat_once().await;
        let _ = manager.shutdown().await;
    }
    .with_subscriber(dispatch)
    .await;
    let output = logs.output();
    for field in [
        "error_category",
        "platform_code=Some(912345)",
        "failures=1",
        "retry_delay_secs",
        "remaining_budget_secs",
    ] {
        assert!(output.contains(field), "{field}: {output}");
    }
    assert!(output.contains("未知平台心跳错误码"));
    assert!(!output.contains(CODE));
    assert!(!output.contains("auth-body"));
}
