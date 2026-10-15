use super::*;
use crate::open_live::ws::LiveEvent;
use serde_json::json;

fn event(value: serde_json::Value) -> LiveEvent {
    LiveEvent::Unknown {
        cmd: "TEST".into(),
        data: value,
    }
}
fn register(log: &Arc<EventLog>, id: &str) {
    log.register(
        id,
        7,
        "game-1",
        vec!["code".into()],
        watch::channel(UpstreamStatus::Connected).1,
    )
    .unwrap();
}

#[tokio::test]
async fn retains_without_receivers_and_reads_history_then_live_and_terminal() {
    let log = EventLog::new(DeliveryPolicy::default());
    register(&log, "one");
    log.append("one", &event(json!(1))).unwrap();
    log.append("one", &event(json!(2))).unwrap();
    let mut reader = log.test_reader("one", "code", Some(0), None).unwrap();
    assert_eq!(reader.recv().await.unwrap().unwrap().seq, 1);
    log.append("one", &event(json!(3))).unwrap();
    assert_eq!(reader.recv().await.unwrap().unwrap().seq, 2);
    assert_eq!(reader.recv().await.unwrap().unwrap().seq, 3);
    log.finish("one");
    assert!(reader.recv().await.unwrap().is_none());
    let mut replay = log.test_reader("one", "code", Some(2), None).unwrap();
    assert_eq!(replay.recv().await.unwrap().unwrap().seq, 3);
    assert!(replay.recv().await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn time_eviction_and_finished_stream_expiry_are_explicit() {
    let log = EventLog::new(DeliveryPolicy {
        retention: Duration::from_secs(5),
        ..DeliveryPolicy::default()
    });
    register(&log, "one");
    log.append("one", &event(json!(1))).unwrap();
    let reader = log.test_reader("one", "code", Some(0), None).unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    let gap = reader.validate_after(0).unwrap_err();
    assert_eq!(gap.reason, "cursor_expired");
    assert_eq!(gap.earliest_after, Some(1));
    log.finish("one");
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        log.test_reader("one", "code", Some(1), None)
            .err()
            .unwrap()
            .reason,
        "stream_unavailable"
    );
}

#[tokio::test]
async fn global_bytes_and_oversized_event_keep_a_contiguous_suffix() {
    let log = EventLog::new(DeliveryPolicy {
        max_bytes: 400,
        ..DeliveryPolicy::default()
    });
    register(&log, "one");
    register(&log, "two");
    log.append("one", &event(json!("a".repeat(100)))).unwrap();
    log.append("two", &event(json!("b".repeat(100)))).unwrap();
    assert!(super::super::manager::lock(&log.state).bytes <= 400);
    assert_eq!(
        log.test_reader("one", "code", Some(0), None)
            .err()
            .unwrap()
            .reason,
        "cursor_expired"
    );
    log.append("two", &event(json!("x".repeat(1000)))).unwrap();
    assert_eq!(
        log.test_reader("two", "code", Some(1), None)
            .err()
            .unwrap()
            .earliest_after,
        Some(2)
    );
    log.append("two", &event(json!(3))).unwrap();
    let mut reader = log.test_reader("two", "code", Some(2), None).unwrap();
    assert_eq!(reader.recv().await.unwrap().unwrap().seq, 3);
}

#[test]
fn unknown_stream_wrong_code_and_future_cursor_do_not_expose_history() {
    let log = EventLog::new(DeliveryPolicy::default());
    register(&log, "one");
    for (id, code) in [("missing", "code"), ("one", "other")] {
        let gap = log.test_reader(id, code, Some(0), None).err().unwrap();
        assert_eq!(gap, unavailable());
    }
    assert_eq!(
        log.test_reader("one", "code", Some(1), None)
            .err()
            .unwrap()
            .reason,
        "cursor_ahead"
    );
}

#[test]
fn stream_limit_rejects_active_overflow_and_evicts_finished_streams() {
    let log = EventLog::new(DeliveryPolicy {
        max_streams: 1,
        ..DeliveryPolicy::default()
    });
    register(&log, "one");
    assert!(
        log.register(
            "two",
            7,
            "g",
            vec!["code".into()],
            watch::channel(UpstreamStatus::Connected).1
        )
        .is_err()
    );
    log.finish("one");
    register(&log, "two");
    assert_eq!(
        log.test_reader("one", "code", Some(0), None)
            .err()
            .unwrap()
            .reason,
        "stream_unavailable"
    );
}

#[test]
fn identity_fingerprints_are_bounded_and_recently_used_aliases_are_retained() {
    let log = EventLog::new(DeliveryPolicy::default());
    register(&log, "one");
    for index in 0..200 {
        log.add_code("one", &format!("code-{index}"));
    }
    assert_eq!(
        super::super::manager::lock(&log.state).streams["one"]
            .codes
            .len(),
        128
    );
    assert!(log.test_reader("one", "code-0", Some(0), None).is_err());
    assert!(log.test_reader("one", "code-199", Some(0), None).is_ok());
    log.add_code("one", "code-0");
    assert!(log.test_reader("one", "code-0", Some(0), None).is_ok());
}
