//! 房间日志保留边界与累计 ACK 窗口的行为测试。

use super::{Log, Window};

#[test]
fn retention_reports_gaps_and_ahead() {
    let mut log = Log::new();
    for _ in 0..257 {
        log.push(0, "event".into());
    }
    assert_eq!(log.check(0), Err("cursor_expired"));
    assert_eq!(log.check(258), Err("cursor_ahead"));
    assert_eq!(log.next(1), Some("event"));

    log.trim(300_000);
    assert_eq!(log.check(256), Err("cursor_expired"));
    assert_eq!(log.check(257), Ok(()));
}

#[test]
fn oversized_record_creates_explicit_gap() {
    let mut log = Log::new();
    log.push(0, "x".repeat(1_048_577));
    assert_eq!(log.check(0), Err("cursor_expired"));
}

#[test]
fn ack_window_rejects_future_and_tolerates_duplicates() {
    let mut w = Window {
        acked: 0,
        sent: 2,
        limit: 2,
    };
    assert!(!w.available());
    assert!(w.acknowledge(3).is_err());
    w.acknowledge(1).unwrap();
    w.acknowledge(0).unwrap();
    assert!(w.available());
    assert_eq!(w.acked, 1);
}
