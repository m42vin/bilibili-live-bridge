use tokio::time::{Instant, advance};

use super::super::{ReconnectPolicy, UpstreamPolicy, UpstreamStatus};
use super::*;

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

fn recovering(mock: &Arc<Mock>, grace: Duration) -> Manager {
    Manager::from_parts(
        mock.clone(),
        Duration::from_secs(20),
        16,
        ManagerOptions { idle_grace: grace },
        super::super::HeartbeatPolicy::default(),
        UpstreamPolicy::default(),
    )
    .with_resource_budget(crate::resource::ResourceBudget::for_tests())
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn restores_same_game_with_new_subscribers_and_continuing_heartbeats() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let (gate, mut entered) = mock.push_connect_wait();
    let manager = recovering(&mock, Duration::ZERO);
    let mut first = manager.attach(CODE).await.unwrap();
    let mut status = first.watch_status();
    let old = mock.connection(0);
    old.hangup();
    entered.wait_for(|entered| *entered).await.unwrap();
    assert_eq!(*status.borrow_and_update(), UpstreamStatus::Reconnecting);
    assert!(old.is_closed());
    let mut second = manager.attach(CODE).await.unwrap();
    assert_eq!(first.game_id(), second.game_id());
    assert_eq!(
        *second.watch_status().borrow(),
        UpstreamStatus::Reconnecting
    );
    advance(Duration::from_secs(20)).await;
    settle().await;
    assert_eq!(mock.heartbeats(), vec!["game-1"]);
    gate.send_replace(true);
    status
        .wait_for(|state| *state == UpstreamStatus::Connected)
        .await
        .unwrap();
    mock.connection(1).emit(danmaku("restored"));
    assert_eq!(text(&first.recv().await.unwrap()), "restored");
    assert_eq!(text(&second.recv().await.unwrap()), "restored");
    assert_eq!(mock.start_codes(), vec![CODE]);
    assert!(mock.ends().is_empty());
    let _ = manager.shutdown().await;
    assert_eq!(mock.ends(), vec!["game-1"]);
    assert_eq!(*status.borrow(), UpstreamStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn absolute_budget_cancels_pending_connect_even_after_short_recovery() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.push_connect_ok();
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = recovering(&mock, Duration::ZERO);
    let mut sub = manager.attach(CODE).await.unwrap();
    let origin = Instant::now();
    mock.connection(0).hangup();
    settle().await;
    assert_eq!(super::super::lock(&mock.inner).connections.len(), 2);
    advance(Duration::from_secs(40)).await;
    mock.connection(1).hangup();
    entered.wait_for(|value| *value).await.unwrap();
    advance(Duration::from_secs(79)).await;
    settle().await;
    assert!(mock.ends().is_empty());
    advance(Duration::from_secs(1)).await;
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(origin.elapsed(), Duration::from_secs(120));
    assert_eq!(mock.ends(), vec!["game-1"]);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_brief_recoveries_cannot_extend_the_original_window() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    for _ in 0..13 {
        mock.push_connect_ok();
    }
    let manager = recovering(&mock, Duration::ZERO);
    let mut sub = manager.attach(CODE).await.unwrap();
    mock.connection(0).hangup();
    settle().await;
    for index in 1..=12 {
        advance(Duration::from_secs(10)).await;
        mock.connection(index).hangup();
        settle().await;
    }
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(mock.ends(), vec!["game-1"]);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_stable_connection_starts_a_fresh_recovery_budget() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.push_connect_ok();
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = recovering(&mock, Duration::ZERO);
    let mut sub = manager.attach(CODE).await.unwrap();
    mock.connection(0).hangup();
    settle().await;
    advance(Duration::from_secs(60)).await;
    settle().await;
    mock.connection(1).hangup();
    entered.wait_for(|value| *value).await.unwrap();
    advance(Duration::from_secs(119)).await;
    settle().await;
    assert!(mock.ends().is_empty());
    advance(Duration::from_secs(1)).await;
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn stop_idle_or_project_failure_interrupts_recovery_and_ends_once() {
    for reason in ["shutdown", "idle", "heartbeat"] {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        let (_gate, mut entered) = mock.push_connect_wait();
        let manager = recovering(&mock, Duration::from_secs(30));
        let sub = manager.attach(CODE).await.unwrap();
        mock.connection(0).hangup();
        entered.wait_for(|value| *value).await.unwrap();
        match reason {
            "shutdown" => {
                let _ = manager.shutdown().await;
            }
            "idle" => {
                drop(sub);
                advance(Duration::from_secs(30)).await;
                settle().await;
                let _ = manager.shutdown().await;
            }
            "heartbeat" => {
                mock.fail_all_game_ids();
                manager.beat_once().await;
                let _ = manager.shutdown().await;
            }
            _ => unreachable!(),
        }
        assert_eq!(mock.ends(), vec!["game-1"]);
        assert_eq!(mock.start_codes(), vec![CODE]);
    }
}

#[tokio::test(start_paused = true)]
async fn auth_rejection_decode_protocol_and_interaction_end_are_terminal() {
    for reason in ["auth", "decode", "protocol", "interaction"] {
        let mock = Mock::new();
        mock.push_ok("game-1", 7);
        mock.push_connect_ok();
        let manager = recovering(&mock, Duration::ZERO);
        let mut sub = manager.attach(CODE).await.unwrap();
        let socket = mock.connection(0);
        match reason {
            "auth" => {
                super::super::lock(&mock.inner)
                    .connects
                    .push_back(ConnectPlan::Reject);
                socket.hangup();
            }
            "decode" => socket
                .tx
                .send(Err(WsError::Decode {
                    message: "bad data".to_owned(),
                }))
                .unwrap(),
            "protocol" => socket
                .tx
                .send(Err(WsError::Protocol {
                    message: "bad packet".to_owned(),
                }))
                .unwrap(),
            "interaction" => {
                socket.emit(LiveEvent::InteractionEnd(InteractionEnd::default()));
                assert!(sub.recv().await.unwrap().ends_push());
            }
            _ => unreachable!(),
        }
        assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
        assert_eq!(mock.ends(), vec!["game-1"]);
        let _ = manager.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn budget_also_covers_old_socket_release() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    let manager = Manager::from_parts(
        mock.clone(),
        Duration::from_secs(20),
        16,
        ManagerOptions::default(),
        super::super::HeartbeatPolicy::default(),
        UpstreamPolicy {
            reconnect: Some(ReconnectPolicy {
                failure_timeout: Duration::from_secs(2),
                ..ReconnectPolicy::default()
            }),
            ..UpstreamPolicy::default()
        },
    )
    .with_resource_budget(crate::resource::ResourceBudget::for_tests())
    .unwrap();
    let mut sub = manager.attach(CODE).await.unwrap();
    let socket = mock.connection(0);
    socket.hold_close();
    let origin = Instant::now();
    socket.hangup();
    assert_eq!(sub.recv().await.unwrap_err(), RecvError::Closed);
    assert_eq!(origin.elapsed(), Duration::from_secs(2));
    assert_eq!(mock.ends(), vec!["game-1"]);
    let _ = manager.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn failed_attempts_back_off_without_starting_or_ending_games() {
    let mock = Mock::new();
    mock.push_ok("game-1", 7);
    mock.push_connect_ok();
    mock.push_connect_err("network");
    let (_gate, mut entered) = mock.push_connect_wait();
    let manager = recovering(&mock, Duration::ZERO);
    let _sub = manager.attach(CODE).await.unwrap();
    let origin = Instant::now();
    mock.connection(0).hangup();
    entered.wait_for(|value| *value).await.unwrap();
    assert!(
        (Duration::from_millis(1600)..=Duration::from_millis(2400)).contains(&origin.elapsed())
    );
    assert_eq!(mock.start_codes(), vec![CODE]);
    assert!(mock.ends().is_empty());
    let _ = manager.shutdown().await;
}
