use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tracing::instrument::WithSubscriber;

use super::super::platform::{BoxFuture, LiveSocket, Platform};
use super::{Manager, ManagerOptions, RecvError};
use crate::open_live::api::{Anchor, BatchHeartbeatResult, StartResult};
use crate::open_live::error::{ApiError, ErrorCode};
use crate::open_live::ws::{Danmaku, InteractionEnd, LiveEvent, WsError};
use crate::session::SessionError;

const QUIET: Duration = Duration::from_secs(3600);
const CODE: &str = "super-secret-code";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_session_logs_keep_context_and_hide_credentials() {
    let (dispatch, logs) = crate::logging::logging_test::capture("trace");
    async {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        let manager = manager(&mock, QUIET);
        let mut subscription = manager.attach(CODE).await.unwrap();
        mock.connection(0).emit(danmaku("private-danmaku-content"));
        subscription.recv().await.unwrap();
        let _ = manager.shutdown().await;
    }
    .with_subscriber(dispatch)
    .await;

    let output = logs.output();
    for message in ["直播间会话已建立", "转发直播间事件", "直播间会话已结束"]
    {
        let line = output
            .lines()
            .find(|line| line.contains(message))
            .unwrap_or_else(|| panic!("missing {message}: {output}"));
        assert!(line.contains("room_id=7"), "{line}");
        assert!(line.contains("game_id=game-1"), "{line}");
    }
    assert!(!output.contains(CODE));
    assert!(!output.contains("auth-body"));
    assert!(!output.contains("private-danmaku-content"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_and_background_cleanup_failures_are_logged() {
    let (dispatch, logs) = crate::logging::logging_test::capture("warn");
    async {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        mock.fail_heartbeats_with_transport();
        mock.fail_end();
        let manager = manager(&mock, QUIET);
        let _subscription = manager.attach(CODE).await.unwrap();
        for _ in 0..super::HEARTBEAT_TRANSPORT_LIMIT {
            manager.beat_once().await;
        }
        let _ = manager.shutdown().await;
    }
    .with_subscriber(dispatch)
    .await;

    let output = logs.output();
    assert!(output.contains("会话批量心跳失败"));
    assert!(output.contains("HTTP 503"));
    assert!(output.contains("transport_failure_limit"), "{output}");
    let cleanup = output
        .lines()
        .find(|line| line.contains("关闭官方场次失败"))
        .unwrap();
    assert!(cleanup.contains("WARN"), "{cleanup}");
    assert!(cleanup.contains("HTTP 500"), "{cleanup}");
    assert!(cleanup.contains("room_id=7"), "{cleanup}");
    assert!(cleanup.contains("game_id=game-1"), "{cleanup}");
    assert!(!output.contains(CODE));
}

#[test]
fn rejects_zero_heartbeat_and_debug_hides_the_access_key() {
    let api =
        crate::open_live::api::Client::new("access-key-id-value", "access-key-secret-value", 7)
            .unwrap();
    let error = Manager::new(api, Duration::ZERO, Duration::from_secs(20)).unwrap_err();
    assert_eq!(error.to_string(), "心跳间隔必须大于 0");

    let api =
        crate::open_live::api::Client::new("access-key-id-value", "access-key-secret-value", 7)
            .unwrap();
    let manager = Manager::new(api, Duration::from_secs(20), Duration::from_secs(20)).unwrap();
    let rendered = format!("{manager:?}");
    assert!(!rendered.contains("access-key-secret-value"));
    assert!(!rendered.contains("access-key-id-value"));
    assert!(!manager.is_shutting_down());
}

#[tokio::test]
async fn rejects_blank_identity_code_without_calling_start() {
    let mock = Mock::new();
    let manager = manager(&mock, QUIET);
    let error = manager.attach("  ").await.unwrap_err();
    assert_eq!(
        error,
        SessionError::Invalid {
            message: "身份码不能为空".to_owned(),
        }
    );
    assert!(mock.start_codes().is_empty());
}

#[tokio::test]
async fn same_code_shares_one_upstream_and_fans_events_out() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);

    let mut first = manager.attach("  super-secret-code  ").await.unwrap();
    let mut second = manager.attach(CODE).await.unwrap();

    assert_eq!(mock.start_codes(), [CODE.to_owned()]);
    assert_eq!(first.room_id(), 7);
    assert_eq!(second.game_id(), "game-1");
    assert_eq!(first.anchor().room_id, 7);
    assert_eq!(mock.connections().len(), 1);
    let rendered = format!("{manager:?} {first:?}");
    assert!(!rendered.contains(CODE));
    assert!(!rendered.contains("auth-body"));

    let socket = mock.connection(0);
    socket.emit(danmaku("hello"));
    assert_eq!(text(&first.recv().await.unwrap()), "hello");
    assert_eq!(text(&second.recv().await.unwrap()), "hello");
    assert!(mock.ends().is_empty());
    assert!(!socket.is_closed());

    drop(second);
    socket.emit(danmaku("still-open"));
    assert_eq!(text(&first.recv().await.unwrap()), "still-open");
    assert!(mock.ends().is_empty());

    drop(first);
    mock.wait_ends(1).await;
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
    assert!(socket.is_closed());
}

#[tokio::test(start_paused = true)]
async fn pending_socket_close_does_not_block_shutdown_or_local_cleanup() {
    // 即使 end 也失败，关闭超时后仍须释放索引、广播和完成通知。
    for fail_end in [false, true] {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        if fail_end {
            mock.fail_end();
        }
        let manager = manager(&mock, QUIET);
        let mut sub = manager.attach(CODE).await.unwrap();
        let session = Arc::clone(&sub.session);
        let socket = mock.connection(0);
        socket.hold_close();
        let started = tokio::time::Instant::now();

        timeout(
            super::SOCKET_CLOSE_TIMEOUT + Duration::from_secs(1),
            async {
                tokio::join!(manager.shutdown(), session.until_finished());
            },
        )
        .await
        .expect("pending close blocked shutdown or its completion notification");

        assert_eq!(started.elapsed(), super::SOCKET_CLOSE_TIMEOUT);
        assert!(
            socket.is_closed(),
            "close must be attempted before timing out"
        );
        assert!(
            socket.tx.is_closed(),
            "socket must be dropped after timeout"
        );
        assert_eq!(mock.end_attempts(), 1);
        assert!(session.is_finished());
        assert!(super::lock(&manager.state.tables).by_code.is_empty());
        assert!(super::lock(&manager.state.tables).by_room.is_empty());
        assert!(super::lock(&manager.state.beats).is_empty());
        assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
        let _ = manager.shutdown().await;
        drop(sub);
        assert_eq!(mock.end_attempts(), 1, "cleanup must not run twice");
    }
}

#[tokio::test(start_paused = true)]
async fn pending_socket_close_allows_same_code_to_start_after_cleanup() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.push_ok("game-2", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let first = manager.attach(CODE).await.unwrap();
    let old_session = Arc::clone(&first.session);
    let old_socket = mock.connection(0);
    old_socket.hold_close();
    drop(first);

    let second = timeout(
        super::SOCKET_CLOSE_TIMEOUT + Duration::from_secs(1),
        manager.attach(CODE),
    )
    .await
    .expect("same-code attach waited forever for socket close")
    .unwrap();

    assert!(old_session.is_finished());
    assert!(old_socket.tx.is_closed());
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
    assert_eq!(mock.start_codes(), [CODE.to_owned(), CODE.to_owned()]);
    assert_eq!(second.game_id(), "game-2");
    assert_eq!(super::lock(&manager.state.tables).by_code.len(), 1);
    assert_eq!(super::lock(&manager.state.tables).by_room.len(), 1);
    let _ = manager.shutdown().await;
    assert_eq!(mock.ends(), ["game-1".to_owned(), "game-2".to_owned()]);
}

#[tokio::test]
async fn concurrent_attaches_with_the_same_code_start_once() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);

    let first = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_entered(1).await;
    let second = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::task::yield_now().await;
            if mock.start_codes().len() > 1 {
                panic!("同一个身份码发起了两次 start");
            }
            if second.is_finished() {
                panic!("第二路接入在 start 返回前就结束了");
            }
            // 给第二路几次调度机会去撞上正在进行的 start。
            if mock.start_codes().len() == 1 {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
                break;
            }
        }
    })
    .await
    .expect("second attach did not park");
    assert!(!second.is_finished());

    mock.release_starts();
    let (first, second) = timeout(Duration::from_secs(2), async {
        tokio::join!(first, second)
    })
    .await
    .expect("attach hung");
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();
    assert_eq!(first.game_id(), "game-1");
    assert_eq!(second.game_id(), "game-1");
    assert_eq!(mock.start_codes(), [CODE.to_owned()]);
    assert_eq!(mock.connections().len(), 1);
}

#[tokio::test]
async fn different_codes_for_one_room_share_the_first_session() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("game-a", 42);
    mock.push_ok("game-b", 42);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);

    let first = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach("code-a").await })
    };
    let second = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach("code-b").await })
    };
    mock.wait_entered(2).await;
    mock.release_starts();

    let (first, second) = timeout(Duration::from_secs(2), async {
        tokio::join!(first, second)
    })
    .await
    .expect("attach hung");
    let mut first = first.unwrap().unwrap();
    let mut second = second.unwrap().unwrap();
    assert_eq!(first.game_id(), second.game_id());
    assert_eq!(first.room_id(), 42);
    assert_eq!(mock.connections().len(), 1);

    let winner = first.game_id().to_owned();
    let loser = if winner == "game-a" {
        "game-b"
    } else {
        "game-a"
    };
    assert_eq!(mock.ends(), [loser.to_owned()]);

    mock.connection(0).emit(danmaku("shared"));
    assert_eq!(text(&first.recv().await.unwrap()), "shared");
    assert_eq!(text(&second.recv().await.unwrap()), "shared");

    drop(first);
    drop(second);
    mock.wait_ends(2).await;
    assert!(mock.ends().contains(&"game-a".to_owned()));
    assert!(mock.ends().contains(&"game-b".to_owned()));
}

#[tokio::test]
async fn interaction_end_is_delivered_and_closes_the_session() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let mut sub = manager.attach(CODE).await.unwrap();

    mock.connection(0)
        .emit(LiveEvent::InteractionEnd(InteractionEnd {
            game_id: "game-1".to_owned(),
            timestamp: 10,
        }));

    match &*sub.recv().await.unwrap() {
        LiveEvent::InteractionEnd(end) => assert_eq!(end.game_id, "game-1"),
        other => panic!("unexpected event {other:?}"),
    }
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
    assert!(mock.connection(0).is_closed());
}

#[tokio::test]
async fn upstream_close_ends_the_session() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let mut sub = manager.attach(CODE).await.unwrap();

    mock.connection(0).hangup();
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
    assert!(mock.connection(0).is_closed());
}

#[tokio::test]
async fn platform_heartbeat_error_ends_the_session() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.fail_heartbeats_with_platform();
    let manager = manager(&mock, Duration::from_millis(20));
    let mut sub = manager.attach(CODE).await.unwrap();

    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(mock.heartbeats(), ["game-1".to_owned()]);
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
}

#[tokio::test]
async fn repeated_transport_heartbeat_errors_end_the_session() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.fail_heartbeats_with_transport();
    let manager = manager(&mock, Duration::from_millis(20));
    let mut sub = manager.attach(CODE).await.unwrap();

    assert_eq!(
        timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("heartbeat did not close the session")
            .unwrap_err(),
        RecvError::Closed
    );
    assert_eq!(mock.heartbeats().len(), 3);
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
}

#[tokio::test]
async fn two_rooms_share_one_batch_heartbeat() {
    let mock = Mock::new();
    mock.push_ok("game-a", 1);
    mock.push_ok("game-b", 2);
    mock.push_connect_ok();
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let _first = manager.attach("code-a").await.unwrap();
    let _second = manager.attach("code-b").await.unwrap();

    manager.beat_once().await;

    let batches = mock.batches();
    assert_eq!(batches.len(), 1, "{batches:?}");
    let mut ids = batches[0].clone();
    ids.sort();
    assert_eq!(ids, ["game-a".to_owned(), "game-b".to_owned()]);
    assert!(mock.ends().is_empty());
}

#[tokio::test]
async fn failed_game_id_closes_only_that_session() {
    let mock = Mock::new();
    mock.push_ok("game-a", 1);
    mock.push_ok("game-b", 2);
    mock.push_connect_ok();
    mock.push_connect_ok();
    mock.fail_game_ids(&["game-a"]);
    let manager = manager(&mock, QUIET);
    let mut closed = manager.attach("code-a").await.unwrap();
    let mut live = manager.attach("code-b").await.unwrap();

    manager.beat_once().await;

    assert_eq!(closed.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(mock.ends(), ["game-a".to_owned()]);
    mock.connection(1).emit(danmaku("still-live"));
    assert_eq!(text(&live.recv().await.unwrap()), "still-live");
    assert!(mock.ends().contains(&"game-a".to_owned()));
    assert!(!mock.ends().contains(&"game-b".to_owned()));
}

#[tokio::test]
async fn start_error_does_not_end_and_can_be_retried() {
    let mock = Mock::new();
    mock.push_start(Err(ApiError::Platform {
        code: ErrorCode::IdentityCode,
        message: "身份码错误".to_owned(),
        request_id: Some("req-1".to_owned()),
    }));
    let manager = manager(&mock, QUIET);
    let error = manager.attach(CODE).await.unwrap_err();
    assert_eq!(error.platform_code(), Some(ErrorCode::IdentityCode));
    assert!(error.to_string().contains("7007"));
    assert!(mock.ends().is_empty());
    assert!(mock.connections().is_empty());

    mock.push_ok("game-2", 9);
    mock.push_connect_ok();
    let sub = manager.attach(CODE).await.unwrap();
    assert_eq!(sub.game_id(), "game-2");
    assert_eq!(sub.room_id(), 9);
}

#[tokio::test]
async fn connect_error_ends_the_game_and_reports_a_failed_end() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_err("官方长连接全部失败");
    mock.fail_end();
    let manager = manager(&mock, QUIET);
    let error = manager.attach(CODE).await.unwrap_err();
    let rendered = error.to_string();
    assert!(rendered.contains("官方长连接全部失败"), "{rendered}");
    assert!(rendered.contains("关闭场次失败"), "{rendered}");
    assert!(mock.ends().is_empty());
    assert_eq!(mock.end_attempts(), 1);
}

#[tokio::test]
async fn shutdown_during_start_ends_the_game_and_rejects_new_attaches() {
    let mock = Mock::new();
    mock.hold_starts();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let attach = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_entered(1).await;

    let shutdown = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.shutdown().await })
    };
    timeout(Duration::from_secs(2), async {
        while !manager.is_shutting_down() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown did not start");

    mock.release_starts();
    let error = timeout(Duration::from_secs(2), attach)
        .await
        .expect("attach hung")
        .unwrap()
        .unwrap_err();
    assert_eq!(error, SessionError::ShuttingDown);
    timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("shutdown hung")
        .unwrap();
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
    assert!(mock.connections().is_empty());

    let error = manager.attach("other-code").await.unwrap_err();
    assert_eq!(error, SessionError::ShuttingDown);
}

#[tokio::test]
async fn next_attach_waits_until_the_previous_game_ends() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_ok("game-2", 7);
    mock.push_connect_ok();
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let sub = manager.attach(CODE).await.unwrap();
    mock.hold_end();
    drop(sub);

    let next = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_end_entered(1).await;
    timeout(Duration::from_millis(200), async {
        while mock.start_codes().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect_err("second start ran before the first game ended");
    assert_eq!(mock.start_codes(), [CODE.to_owned()]);

    mock.release_end();
    let sub = timeout(Duration::from_secs(2), next)
        .await
        .expect("second attach hung")
        .unwrap()
        .unwrap();
    assert_eq!(sub.game_id(), "game-2");
    assert_eq!(mock.start_codes(), [CODE.to_owned(), CODE.to_owned()]);
    assert_eq!(mock.ends(), ["game-1".to_owned()]);
}

#[tokio::test(start_paused = true)]
async fn idle_grace_keeps_heartbeats_and_reconnect_cancels_the_old_deadline() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = grace_manager(&mock);
    let first = manager.attach(CODE).await.unwrap();
    let socket = mock.connection(0);
    drop(first);

    socket.emit(danmaku("during-idle"));
    socket.wait_received(1).await;
    manager.beat_once().await;
    assert_eq!(mock.heartbeats(), ["game-1"]);
    assert!(!socket.is_closed());
    tokio::time::advance(Duration::from_secs(29)).await;
    let mut second = manager.attach(CODE).await.unwrap();
    assert_eq!(second.game_id(), "game-1");
    assert_eq!(mock.start_codes(), [CODE]);

    tokio::time::advance(Duration::from_secs(31)).await;
    socket.emit(danmaku("after-reconnect"));
    assert_eq!(text(&second.recv().await.unwrap()), "after-reconnect");
    assert!(mock.ends().is_empty());

    drop(second);
    tokio::time::advance(Duration::from_secs(29)).await;
    assert!(mock.ends().is_empty());
    tokio::time::advance(Duration::from_secs(1)).await;
    mock.wait_ends(1).await;
    assert_eq!(mock.ends(), ["game-1"]);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn idle_expiration_closes_once_and_next_attach_starts_a_new_game() {
    let mock = Mock::new();
    for game in ["game-1", "game-2"] {
        mock.push_ok(game, 7);
        mock.push_connect_ok();
    }
    let manager = grace_manager(&mock);
    drop(manager.attach(CODE).await.unwrap());
    tokio::time::advance(Duration::from_secs(30)).await;
    let second = manager.attach(CODE).await.unwrap();
    assert_eq!(second.game_id(), "game-2");
    assert_eq!(mock.ends(), ["game-1"]);
    assert_eq!(mock.connections().len(), 2);
    let _ = manager.shutdown().await;
    assert_eq!(mock.ends(), ["game-1", "game-2"]);
}

#[tokio::test(start_paused = true)]
async fn terminal_events_and_upstream_close_bypass_idle_grace() {
    for interaction_end in [true, false] {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        let manager = grace_manager(&mock);
        drop(manager.attach(CODE).await.unwrap());
        if interaction_end {
            mock.connection(0)
                .emit(LiveEvent::InteractionEnd(InteractionEnd {
                    game_id: "game-1".to_owned(),
                    ..InteractionEnd::default()
                }));
        } else {
            mock.connection(0).hangup();
        }
        mock.wait_ends(1).await;
        assert!(mock.connection(0).is_closed());
        let _ = manager.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_and_heartbeat_failure_bypass_idle_grace() {
    for heartbeat_failure in [true, false] {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        let manager = grace_manager(&mock);
        drop(manager.attach(CODE).await.unwrap());
        if heartbeat_failure {
            mock.fail_heartbeats_with_platform();
            manager.beat_once().await;
            mock.wait_ends(1).await;
        }
        let _ = manager.shutdown().await;
        assert_eq!(mock.ends(), ["game-1"]);
        assert!(mock.connection(0).is_closed());
    }
}

#[tokio::test]
async fn lagged_subscription_continues_from_retained_events() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let mut subscription = manager.attach(CODE).await.unwrap();
    let socket = mock.connection(0);
    for index in 0..20 {
        socket.emit(danmaku(&index.to_string()));
    }
    socket.wait_received(20).await;
    assert_eq!(
        subscription.recv().await.unwrap_err(),
        RecvError::Lagged { skipped: 4 }
    );
    assert_eq!(text(&subscription.recv().await.unwrap()), "4");
    let _ = manager.shutdown().await;
}

fn grace_manager(mock: &Arc<Mock>) -> Manager {
    let platform: Arc<dyn Platform> = mock.clone();
    Manager::from_parts(
        platform,
        QUIET,
        16,
        ManagerOptions {
            idle_grace: Duration::from_secs(30),
        },
    )
}

fn manager(mock: &Arc<Mock>, app_heartbeat: Duration) -> Manager {
    let mock = Arc::clone(mock);
    let platform: Arc<dyn Platform> = mock;
    Manager::from_platform(platform, app_heartbeat, 16)
}

fn danmaku(msg: &str) -> LiveEvent {
    LiveEvent::Danmaku(Danmaku {
        msg: msg.to_owned(),
        room_id: 7,
        ..Danmaku::default()
    })
}

fn text(event: &LiveEvent) -> &str {
    match event {
        LiveEvent::Danmaku(danmaku) => &danmaku.msg,
        other => panic!("unexpected event {other:?}"),
    }
}

fn room(game_id: &str, room_id: i64) -> StartResult {
    StartResult::from_parts(
        game_id,
        "auth-body",
        vec!["wss://example.invalid/live".to_owned()],
        Anchor {
            room_id,
            uname: "anchor".to_owned(),
            uface: String::new(),
            uid: 1,
            open_id: "anchor-open".to_owned(),
            union_id: String::new(),
        },
    )
}

struct Mock {
    inner: Mutex<MockInner>,
    entered: watch::Sender<usize>,
    start_gate: watch::Sender<bool>,
    hold_starts: AtomicBool,
    end_gate: watch::Sender<bool>,
    hold_end: AtomicBool,
    end_entered: AtomicUsize,
    fail_end: AtomicBool,
}

struct MockInner {
    starts: VecDeque<Result<StartResult, ApiError>>,
    connects: VecDeque<ConnectPlan>,
    connections: Vec<TestConn>,
    start_codes: Vec<String>,
    batches: Vec<Vec<String>>,
    failed_ids: Vec<String>,
    heartbeat_mode: HeartbeatMode,
    ends: Vec<String>,
}

enum ConnectPlan {
    Ok,
    Err(String),
    Wait(watch::Receiver<bool>, watch::Sender<bool>),
}

#[derive(Clone, Copy)]
enum HeartbeatMode {
    Ok,
    Platform,
    Unavailable,
}

#[derive(Clone)]
struct TestConn {
    tx: mpsc::UnboundedSender<Option<LiveEvent>>,
    closed: Arc<AtomicBool>,
    hold_close: Arc<AtomicBool>,
    received: watch::Receiver<usize>,
}

impl TestConn {
    fn emit(&self, event: LiveEvent) {
        self.tx
            .send(Some(event))
            .expect("session dropped the socket");
    }

    fn hangup(&self) {
        self.tx.send(None).expect("session dropped the socket");
    }

    fn hold_close(&self) {
        self.hold_close.store(true, Ordering::SeqCst);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn wait_received(&self, count: usize) {
        let mut received = self.received.clone();
        timeout(
            Duration::from_secs(2),
            received.wait_for(|value| *value >= count),
        )
        .await
        .expect("upstream did not receive events")
        .unwrap();
    }
}

struct MockSocket {
    incoming: mpsc::UnboundedReceiver<Option<LiveEvent>>,
    closed: Arc<AtomicBool>,
    hold_close: Arc<AtomicBool>,
    received: watch::Sender<usize>,
}

impl Mock {
    fn new() -> Arc<Self> {
        let (entered, _) = watch::channel(0);
        let (start_gate, _) = watch::channel(false);
        let (end_gate, _) = watch::channel(false);
        Arc::new(Self {
            inner: Mutex::new(MockInner {
                starts: VecDeque::new(),
                connects: VecDeque::new(),
                connections: Vec::new(),
                start_codes: Vec::new(),
                batches: Vec::new(),
                failed_ids: Vec::new(),
                heartbeat_mode: HeartbeatMode::Ok,
                ends: Vec::new(),
            }),
            entered,
            start_gate,
            hold_starts: AtomicBool::new(false),
            end_gate,
            hold_end: AtomicBool::new(false),
            end_entered: AtomicUsize::new(0),
            fail_end: AtomicBool::new(false),
        })
    }

    fn hold_starts(&self) {
        self.hold_starts.store(true, Ordering::SeqCst);
    }

    fn release_starts(&self) {
        self.start_gate.send(true).expect("start gate closed");
    }

    fn hold_end(&self) {
        self.hold_end.store(true, Ordering::SeqCst);
    }

    fn release_end(&self) {
        self.end_gate.send(true).expect("end gate closed");
    }

    fn fail_end(&self) {
        self.fail_end.store(true, Ordering::SeqCst);
    }

    fn fail_heartbeats_with_platform(&self) {
        let mut inner = super::lock(&self.inner);
        inner.heartbeat_mode = HeartbeatMode::Platform;
        inner.failed_ids.clear();
    }

    fn fail_game_ids(&self, game_ids: &[&str]) {
        let mut inner = super::lock(&self.inner);
        inner.heartbeat_mode = HeartbeatMode::Platform;
        inner.failed_ids = game_ids
            .iter()
            .map(|game_id| (*game_id).to_owned())
            .collect();
    }

    fn fail_heartbeats_with_transport(&self) {
        super::lock(&self.inner).heartbeat_mode = HeartbeatMode::Unavailable;
    }

    fn push_ok(&self, game_id: &str, room_id: i64) {
        self.push_start(Ok(room(game_id, room_id)));
    }

    fn push_start(&self, result: Result<StartResult, ApiError>) {
        super::lock(&self.inner).starts.push_back(result);
    }

    fn push_connect_ok(&self) {
        super::lock(&self.inner).connects.push_back(ConnectPlan::Ok);
    }

    fn push_connect_wait(&self) -> (watch::Sender<bool>, watch::Receiver<bool>) {
        let (gate, waiting) = watch::channel(false);
        let (entered, entered_rx) = watch::channel(false);
        super::lock(&self.inner)
            .connects
            .push_back(ConnectPlan::Wait(waiting, entered));
        (gate, entered_rx)
    }

    fn push_connect_err(&self, message: impl Into<String>) {
        super::lock(&self.inner)
            .connects
            .push_back(ConnectPlan::Err(message.into()));
    }

    fn start_codes(&self) -> Vec<String> {
        super::lock(&self.inner).start_codes.clone()
    }

    fn ends(&self) -> Vec<String> {
        super::lock(&self.inner).ends.clone()
    }

    fn heartbeats(&self) -> Vec<String> {
        super::lock(&self.inner)
            .batches
            .iter()
            .flatten()
            .cloned()
            .collect()
    }

    fn batches(&self) -> Vec<Vec<String>> {
        super::lock(&self.inner).batches.clone()
    }

    fn connections(&self) -> Vec<TestConn> {
        super::lock(&self.inner).connections.clone()
    }

    fn connection(&self, index: usize) -> TestConn {
        self.connections()
            .into_iter()
            .nth(index)
            .expect("missing connection")
    }

    fn end_attempts(&self) -> usize {
        self.end_entered.load(Ordering::SeqCst)
    }

    async fn wait_entered(&self, count: usize) {
        let mut entered = self.entered.subscribe();
        timeout(Duration::from_secs(2), async {
            loop {
                if *entered.borrow() >= count {
                    return;
                }
                entered.changed().await.expect("entered closed");
            }
        })
        .await
        .expect("start did not begin");
    }

    async fn wait_end_entered(&self, count: usize) {
        timeout(Duration::from_secs(2), async {
            while self.end_entered.load(Ordering::SeqCst) < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("end did not begin");
    }

    async fn wait_ends(&self, count: usize) {
        timeout(Duration::from_secs(2), async {
            while self.ends().len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("end was not called");
    }
}

impl Platform for Mock {
    fn start<'a>(&'a self, code: &'a str) -> BoxFuture<'a, Result<StartResult, ApiError>> {
        Box::pin(async move {
            let result = {
                let mut inner = super::lock(&self.inner);
                inner.start_codes.push(code.to_owned());
                inner.starts.pop_front().expect("unexpected start")
            };
            self.entered.send_modify(|count| *count += 1);
            if self.hold_starts.load(Ordering::SeqCst) {
                let mut gate = self.start_gate.subscribe();
                while !*gate.borrow() {
                    gate.changed().await.expect("start gate closed");
                }
            }
            result
        })
    }

    fn batch_heartbeat<'a>(
        &'a self,
        game_ids: &'a [String],
    ) -> BoxFuture<'a, Result<BatchHeartbeatResult, ApiError>> {
        Box::pin(async move {
            let (mode, failed_ids) = {
                let mut inner = super::lock(&self.inner);
                inner.batches.push(game_ids.to_vec());
                (inner.heartbeat_mode, inner.failed_ids.clone())
            };
            match mode {
                HeartbeatMode::Ok => Ok(BatchHeartbeatResult::from_failed(Vec::new())),
                HeartbeatMode::Platform => {
                    let failed = if failed_ids.is_empty() {
                        game_ids.to_vec()
                    } else {
                        game_ids
                            .iter()
                            .filter(|game_id| failed_ids.contains(game_id))
                            .cloned()
                            .collect()
                    };
                    Ok(BatchHeartbeatResult::from_failed(failed))
                }
                HeartbeatMode::Unavailable => Err(ApiError::Status { status: 503 }),
            }
        })
    }

    fn end<'a>(&'a self, game_id: &'a str) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(async move {
            self.end_entered.fetch_add(1, Ordering::SeqCst);
            if self.hold_end.load(Ordering::SeqCst) {
                let mut gate = self.end_gate.subscribe();
                while !*gate.borrow() {
                    gate.changed().await.expect("end gate closed");
                }
            }
            if self.fail_end.load(Ordering::SeqCst) {
                return Err(ApiError::Status { status: 500 });
            }
            super::lock(&self.inner).ends.push(game_id.to_owned());
            Ok(())
        })
    }

    fn connect<'a>(
        &'a self,
        _started: &'a StartResult,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        Box::pin(async move {
            let plan = super::lock(&self.inner)
                .connects
                .pop_front()
                .expect("unexpected connect");
            match plan {
                ConnectPlan::Err(message) => return Err(WsError::Connect { message }),
                ConnectPlan::Ok => {}
                ConnectPlan::Wait(mut gate, entered) => {
                    entered.send_replace(true);
                    gate.wait_for(|open| *open)
                        .await
                        .expect("connect gate closed");
                }
            }
            let (tx, incoming) = mpsc::unbounded_channel();
            let closed = Arc::new(AtomicBool::new(false));
            let hold_close = Arc::new(AtomicBool::new(false));
            let (received, received_rx) = watch::channel(0);
            super::lock(&self.inner).connections.push(TestConn {
                tx,
                closed: Arc::clone(&closed),
                hold_close: Arc::clone(&hold_close),
                received: received_rx,
            });
            let socket: Box<dyn LiveSocket> = Box::new(MockSocket {
                incoming,
                closed,
                hold_close,
                received,
            });
            Ok(socket)
        })
    }
}

impl LiveSocket for MockSocket {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>> {
        Box::pin(async move {
            let event = self.incoming.recv().await.unwrap_or_default();
            self.received.send_modify(|count| *count += 1);
            Ok(event)
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>> {
        Box::pin(async move {
            self.closed.store(true, Ordering::SeqCst);
            if self.hold_close.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

#[tokio::test(start_paused = true)]
async fn heartbeats_run_during_connect_and_keep_their_schedule_after_ready() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    let (gate, mut entered) = mock.push_connect_wait();
    let manager = manager(&mock, Duration::from_secs(20));
    let attach = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    entered.wait_for(|entered| *entered).await.unwrap();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    for expected_batches in 1..=2 {
        tokio::time::advance(Duration::from_secs(20)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(mock.batches().len(), expected_batches);
        assert_eq!(mock.batches().last().unwrap(), &["game-1".to_owned()]);
        assert!(!attach.is_finished());
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    gate.send_replace(true);
    let sub = attach.await.unwrap().unwrap();
    tokio::time::advance(Duration::from_secs(15)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        mock.batches().len(),
        3,
        "ready must not restart the heartbeat interval"
    );
    let _ = manager.shutdown().await;
    drop(sub);
    assert!(super::lock(&manager.state.beats).is_empty());
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
    assert_eq!(mock.ends(), ["game-1"]);
}

#[tokio::test]
async fn heartbeat_failure_cancels_pending_connect_and_cleans_the_game() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = manager(&mock, QUIET);
    let attach = {
        let state = Arc::clone(&manager.state);
        tokio::spawn(async move { state.attach_once(CODE).await })
    };
    entered.wait_for(|entered| *entered).await.unwrap();
    mock.fail_game_ids(&["game-1"]);
    manager.beat_once().await;
    let error = timeout(Duration::from_secs(1), attach)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error, SessionError::Closed);
    assert_eq!(mock.ends(), ["game-1"]);
    assert!(mock.connections().is_empty());
    assert!(super::lock(&manager.state.beats).is_empty());
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
    let _ = manager.shutdown().await;
    assert_eq!(mock.end_attempts(), 1);
}

#[tokio::test]
async fn shutdown_cancels_pending_connect_and_waits_for_end() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = manager(&mock, QUIET);
    let attach = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    entered.wait_for(|entered| *entered).await.unwrap();
    timeout(Duration::from_secs(1), manager.shutdown())
        .await
        .unwrap();
    assert_eq!(
        attach.await.unwrap().unwrap_err(),
        SessionError::ShuttingDown
    );
    assert_eq!(mock.ends(), ["game-1"]);
    assert!(mock.connections().is_empty());
    assert!(super::lock(&manager.state.beats).is_empty());
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
}

#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_duplicate_game_cleanup_after_code_redirect() {
    let mock = Mock::new();
    mock.push_ok("game-a", 42);
    mock.push_ok("game-b", 42);
    mock.push_connect_ok();
    let manager = manager(&mock, QUIET);
    let first = manager.attach("code-a").await.unwrap();
    mock.hold_end();
    let other = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach("code-b").await })
    };
    mock.wait_end_entered(1).await;
    // 重复场次已经进入等待；让活动场次的 end 独立完成。
    mock.hold_end.store(false, Ordering::SeqCst);
    assert!(
        timeout(Duration::from_millis(50), manager.shutdown())
            .await
            .is_err()
    );
    assert_eq!(mock.ends(), ["game-a"]);
    assert_eq!(super::lock(&manager.state.tables).sessions.len(), 1);
    mock.release_end();
    assert_eq!(
        other.await.unwrap().unwrap_err(),
        SessionError::ShuttingDown
    );
    let _ = manager.shutdown().await;
    drop(first);
    assert_eq!(mock.ends(), ["game-a", "game-b"]);
    assert_eq!(mock.end_attempts(), 2);
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
}

#[tokio::test]
async fn failed_connect_removes_heartbeats_before_waiting_for_end() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_err("connect failed");
    mock.hold_end();
    let manager = manager(&mock, QUIET);
    let attach = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    mock.wait_end_entered(1).await;
    assert!(super::lock(&manager.state.beats).is_empty());
    assert_eq!(super::lock(&manager.state.tables).sessions.len(), 1);
    manager.beat_once().await;
    assert!(mock.batches().is_empty());
    mock.release_end();
    assert!(matches!(
        attach.await.unwrap(),
        Err(SessionError::Connect { .. })
    ));
    let _ = manager.shutdown().await;
    assert_eq!(mock.ends(), ["game-1"]);
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
}

#[tokio::test]
async fn cancelled_connect_removes_heartbeats_and_cleans_the_known_game() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = manager(&mock, QUIET);
    let attach = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.attach(CODE).await })
    };
    entered.wait_for(|entered| *entered).await.unwrap();
    assert_eq!(super::lock(&manager.state.beats).len(), 1);
    attach.abort();
    assert!(attach.await.unwrap_err().is_cancelled());
    assert!(super::lock(&manager.state.beats).is_empty());
    assert!(super::lock(&manager.state.tables).sessions.is_empty());
    // 被取消的接入派生尽力清理，按契约先等待它结束再关闭 runtime。
    mock.wait_ends(1).await;
    let _ = manager.shutdown().await;
    assert_eq!(mock.ends(), ["game-1"]);
    assert_eq!(mock.end_attempts(), 1);
}
