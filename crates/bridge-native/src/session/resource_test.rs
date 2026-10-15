//! 小预算与受控平台桩验证 Manager 的所有入口；不依赖外部网络。
use super::*;
use crate::resource::{RatePolicy, Rejection, Resource, ResourceBudget, ResourcePolicy};
use crate::session::{DeliveryError, EventCursor};

fn limits() -> ResourcePolicy {
    ResourcePolicy {
        max_connections: 16,
        max_handshakes: 4,
        max_pending_attaches: 4,
        max_starting_sessions: 2,
        max_sessions: 2,
        max_subscribers_per_room: 2,
        start: RatePolicy {
            per_sec: 1,
            burst: 8,
        },
        ..Default::default()
    }
}
fn limited(mock: &Arc<Mock>, policy: ResourcePolicy) -> Manager {
    grace_manager(mock)
        .with_resource_budget(ResourceBudget::new(policy).unwrap())
        .unwrap()
}
fn refused(error: SessionError, expected: Resource) {
    assert!(
        matches!(error,SessionError::Resource(Rejection {resource,..}) if resource==expected),
        "{error:?}"
    );
}
async fn occupied(manager: &Manager, index: usize, n: usize) {
    timeout(Duration::from_secs(2), async {
        while manager.resource_budget().snapshot().occupied[index] != n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("资源占用未达到预期");
}

#[tokio::test]
async fn admission_rejects_before_start_when_session_or_starting_slots_are_full() {
    for (policy, expected) in [
        (
            ResourcePolicy {
                max_sessions: 1,
                max_starting_sessions: 1,
                ..limits()
            },
            Resource::Session,
        ),
        (
            ResourcePolicy {
                max_starting_sessions: 1,
                ..limits()
            },
            Resource::StartingSession,
        ),
    ] {
        let mock = Mock::new();
        mock.hold_starts();
        mock.push_ok("one", 7);
        mock.push_connect_ok();
        let manager = limited(&mock, policy);
        let task = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.attach("first").await })
        };
        mock.wait_entered(1).await;
        refused(
            manager.attach("unresolved-other-code").await.unwrap_err(),
            expected,
        );
        assert_eq!(mock.start_codes(), ["first"]);
        assert_eq!(manager.resource_budget().snapshot().start_calls, 1);
        mock.release_starts();
        let sub = task.await.unwrap().unwrap();
        let _ = manager.shutdown().await;
        drop(sub);
        assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
    }
}

#[tokio::test]
async fn shared_start_waiters_are_bounded_and_cancel_does_not_abort_the_owner() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("one", 7);
    mock.push_connect_ok();
    let manager = limited(
        &mock,
        ResourcePolicy {
            max_pending_attaches: 2,
            max_starting_sessions: 1,
            ..limits()
        },
    );
    let first = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_entered(1).await;
    let second = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    occupied(&manager, 2, 2).await;
    refused(
        manager.attach(CODE).await.unwrap_err(),
        Resource::PendingAttach,
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    occupied(&manager, 2, 1).await;
    assert_eq!(manager.resource_budget().snapshot().occupied[3..], [1, 1]);
    mock.release_starts();
    let sub = second.await.unwrap().unwrap();
    assert_eq!(mock.start_codes(), [CODE]);
    assert_eq!(manager.resource_budget().snapshot().occupied[2..4], [0, 0]);
    let _ = manager.shutdown().await;
    drop(sub);
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
}

#[tokio::test]
async fn known_session_reuse_ignores_start_capacity_and_rate_but_resume_obeys_room_limit() {
    let mock = Mock::new();
    mock.push_ok("one", 7);
    mock.push_connect_ok();
    mock.push_ok("two", 8);
    mock.push_connect_ok();
    let manager = limited(
        &mock,
        ResourcePolicy {
            max_starting_sessions: 1,
            start: RatePolicy {
                per_sec: 1,
                burst: 2,
            },
            ..limits()
        },
    );
    let first = manager.attach_delivery(CODE, None).await.unwrap();
    // 新身份码占满开场并发，同时耗尽两个 start 令牌。
    mock.start_gate.send_replace(false);
    mock.hold_starts();
    let starting = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach("other").await })
    };
    mock.wait_entered(2).await;
    let second = manager.clone().attach(CODE).await.unwrap();
    assert_eq!(manager.resource_budget().snapshot().start_calls, 2);
    let cursor = EventCursor {
        stream_id: first.stream_id.clone(),
        seq: first.base_seq,
    };
    let error = manager
        .attach_delivery(CODE, Some(&cursor))
        .await
        .err()
        .unwrap();
    let DeliveryError::Attach(error) = error else {
        panic!("活动续传不能退化为历史回放");
    };
    refused(error, Resource::Subscriber);
    drop(second);
    let resumed = manager.attach_delivery(CODE, Some(&cursor)).await.unwrap();
    assert_eq!(manager.resource_budget().snapshot().start_calls, 2);
    mock.release_starts();
    let other = starting.await.unwrap().unwrap();
    let _ = manager.shutdown().await;
    drop((first, resumed, other));
    assert_eq!(manager.resource_budget().snapshot().subscribers, 0);
}

#[tokio::test(start_paused = true)]
async fn failed_start_consumes_rate_and_refills_by_explicit_elapsed_time() {
    let mock = Mock::new();
    mock.push_start(Err(ApiError::Invalid {
        message: "denied".into(),
    }));
    mock.push_ok("two", 7);
    mock.push_connect_ok();
    let manager = limited(
        &mock,
        ResourcePolicy {
            start: RatePolicy {
                per_sec: 1,
                burst: 1,
            },
            ..limits()
        },
    );
    assert!(matches!(
        manager.attach("bad").await,
        Err(SessionError::Start { .. })
    ));
    refused(
        manager.attach("next").await.unwrap_err(),
        Resource::StartRate,
    );
    assert_eq!(manager.resource_budget().snapshot().start_calls, 1);
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
    tokio::time::advance(Duration::from_secs(1)).await;
    let sub = manager.attach("next").await.unwrap();
    assert_eq!(mock.start_codes(), ["bad", "next"]);
    let _ = manager.shutdown().await;
    drop(sub);
}

#[tokio::test]
async fn cleaning_session_keeps_its_slot_and_ended_history_does_not() {
    let mock = Mock::new();
    mock.push_ok("one", 7);
    mock.push_connect_ok();
    let manager = limited(
        &mock,
        ResourcePolicy {
            max_sessions: 1,
            max_starting_sessions: 1,
            ..limits()
        },
    );
    let first = manager.attach_delivery(CODE, None).await.unwrap();
    let cursor = EventCursor {
        stream_id: first.stream_id.clone(),
        seq: first.base_seq,
    };
    let session = super::super::lock(&manager.state.tables).by_code[CODE].clone();
    mock.hold_end();
    mock.connection(0).hangup();
    mock.wait_end_entered(1).await;
    refused(
        manager.attach("new-room").await.unwrap_err(),
        Resource::Session,
    );
    assert_eq!(manager.resource_budget().snapshot().occupied[4], 1);
    let error = manager
        .attach_delivery(CODE, Some(&cursor))
        .await
        .err()
        .unwrap();
    let DeliveryError::Attach(error) = error else {
        panic!("清理中的流应暂时拒绝，而不是无守卫接入");
    };
    refused(error, Resource::Session);
    mock.release_end();
    session.until_finished().await;
    assert_eq!(manager.resource_budget().snapshot().occupied[4], 0);
    let mut replay = manager.attach_delivery(CODE, Some(&cursor)).await.unwrap();
    assert!(replay.recv().await.unwrap().is_none());
    assert_eq!(manager.resource_budget().snapshot().start_calls, 1);
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
    let _ = manager.shutdown().await;
    drop((first, replay));
}

#[tokio::test]
async fn duplicate_room_cleanup_holds_starting_and_session_slots_until_end_returns() {
    let mock = Mock::new();
    mock.push_ok("one", 7);
    mock.push_ok("two", 7);
    mock.push_connect_ok();
    let manager = limited(&mock, limits());
    let first = manager.attach("a").await.unwrap();
    mock.hold_end();
    let task = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach("b").await })
    };
    mock.wait_end_entered(1).await;
    assert_eq!(manager.resource_budget().snapshot().occupied[3..], [1, 2]);
    mock.release_end();
    let second = task.await.unwrap().unwrap();
    assert_eq!(first.game_id(), second.game_id());
    assert_eq!(manager.resource_budget().snapshot().occupied[3..], [0, 1]);
    let _ = manager.shutdown().await;
    drop((first, second));
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
}

#[tokio::test]
async fn failed_connect_cleanup_and_cancelled_waiter_retain_admission_permits() {
    let mock = Mock::new();
    mock.push_ok("one", 7);
    mock.push_connect_err("failed");
    mock.hold_end();
    let manager = limited(
        &mock,
        ResourcePolicy {
            max_starting_sessions: 1,
            ..limits()
        },
    );
    let task = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_end_entered(1).await;
    task.abort();
    let _ = task.await;
    occupied(&manager, 2, 0).await;
    assert_eq!(manager.resource_budget().snapshot().occupied[3..], [1, 1]);
    refused(
        manager.attach("new").await.unwrap_err(),
        Resource::StartingSession,
    );
    let session = super::super::lock(&manager.state.tables).by_code[CODE].clone();
    mock.release_end();
    session.until_finished().await;
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
    let _ = manager.shutdown().await;
}

#[tokio::test]
async fn last_waiter_cancel_during_start_keeps_owner_until_shutdown_cleanup() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("one", 7);
    let manager = limited(&mock, limits());
    let task = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_entered(1).await;
    task.abort();
    let _ = task.await;
    occupied(&manager, 2, 0).await;
    assert_eq!(manager.resource_budget().snapshot().occupied[3..], [1, 1]);
    let shutdown = {
        let manager = manager.clone();
        tokio::spawn(async move {
            let _ = manager.shutdown().await;
        })
    };
    timeout(Duration::from_secs(2), async {
        while !manager.is_shutting_down() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!shutdown.is_finished());
    assert!(mock.ends().is_empty());
    mock.release_starts();
    shutdown.await.unwrap();
    assert_eq!(mock.ends(), ["one"]);
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
}

#[tokio::test]
async fn shared_budget_limits_subscriptions_across_distinct_managers() {
    let a = Mock::new();
    a.push_ok("a", 7);
    a.push_connect_ok();
    let b = Mock::new();
    b.push_ok("b", 7);
    b.push_connect_ok();
    let budget = ResourceBudget::new(ResourcePolicy {
        max_subscribers_per_room: 1,
        ..limits()
    })
    .unwrap();
    let first_manager = grace_manager(&a)
        .with_resource_budget(budget.clone())
        .unwrap();
    let second_manager = grace_manager(&b)
        .with_resource_budget(budget.clone())
        .unwrap();
    let first = first_manager.attach("a").await.unwrap();
    refused(
        second_manager.attach("b").await.unwrap_err(),
        Resource::Subscriber,
    );
    drop(first);
    let second = second_manager.attach("b").await.unwrap();
    assert_eq!(budget.snapshot().start_calls, 2);
    assert_eq!(budget.snapshot().subscribers, 1);
    let _ = first_manager.shutdown().await;
    let _ = second_manager.shutdown().await;
    drop(second);
    assert_eq!(budget.snapshot().occupied, [0; 5]);
    assert_eq!(budget.snapshot().subscribers, 0);
}

#[tokio::test]
async fn full_budget_does_not_block_project_heartbeat_upstream_recovery_or_end() {
    let mock = Mock::new();
    mock.push_ok("one", 7);
    mock.push_connect_ok();
    let budget = ResourceBudget::new(ResourcePolicy {
        max_sessions: 1,
        max_starting_sessions: 1,
        start: RatePolicy {
            per_sec: 1,
            burst: 1,
        },
        ..limits()
    })
    .unwrap();
    let manager = Manager::from_parts(
        mock.clone(),
        QUIET,
        16,
        ManagerOptions::default(),
        Default::default(),
        super::super::UpstreamPolicy::default(),
    )
    .with_resource_budget(budget.clone())
    .unwrap();
    let mut sub = manager.attach(CODE).await.unwrap();
    let mut status = sub.watch_status();
    refused(
        manager.attach("other").await.unwrap_err(),
        Resource::Session,
    );
    let (gate, mut entered) = mock.push_connect_wait();
    mock.connection(0).hangup();
    entered.wait_for(|entered| *entered).await.unwrap();
    assert_eq!(
        *status.borrow(),
        crate::session::UpstreamStatus::Reconnecting
    );
    manager.beat_once().await;
    assert_eq!(mock.batches(), vec![vec!["one".to_owned()]]);
    assert_eq!(budget.snapshot().occupied[3..], [0, 1]);
    gate.send_replace(true);
    status
        .wait_for(|s| *s == crate::session::UpstreamStatus::Connected)
        .await
        .unwrap();
    mock.connection(1).emit(danmaku("recovered"));
    assert_eq!(text(&sub.recv().await.unwrap()), "recovered");
    let _ = manager.shutdown().await;
    drop(sub);
    assert_eq!(budget.snapshot().start_calls, 1);
    assert_eq!(budget.snapshot().occupied, [0; 5]);
}

#[tokio::test(start_paused = true)]
async fn cancelled_only_waiter_releases_the_unsubscribed_session_after_idle_grace() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("one", 7);
    mock.push_connect_ok();
    let manager = limited(&mock, limits());
    let task = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_entered(1).await;
    let session = super::super::lock(&manager.state.tables).by_code[CODE].clone();
    task.abort();
    let _ = task.await;
    occupied(&manager, 2, 0).await;
    mock.release_starts();
    occupied(&manager, 3, 0).await;
    assert_eq!(manager.resource_budget().snapshot().occupied[4], 1);
    assert_eq!(manager.resource_budget().snapshot().subscribers, 0);
    tokio::time::advance(Duration::from_secs(30)).await;
    timeout(Duration::from_secs(1), session.until_finished())
        .await
        .unwrap();
    assert_eq!(mock.ends(), ["one"]);
    assert_eq!(manager.resource_budget().snapshot().occupied, [0; 5]);
    let _ = manager.shutdown().await;
}
