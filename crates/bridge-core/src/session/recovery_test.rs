use super::*;

fn t(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn heartbeat() -> HeartbeatRecovery {
    HeartbeatRecovery::new(HeartbeatPolicy::default(), t(20), t(0))
}

fn retryable() -> HeartbeatOutcome {
    HeartbeatOutcome::Failure(FailureKind::Retryable)
}

#[test]
fn heartbeat_budget_starts_at_last_confirmation_but_activates_only_on_failure() {
    let mut state = heartbeat();
    assert!(!state.expired(t(100)));
    assert_eq!(state.next_deadline(false), None);
    assert_eq!(
        state.complete(t(100), HeartbeatOutcome::Confirmed, 100),
        HeartbeatDecision::Confirmed {
            previous_failures: 0
        }
    );
    assert_eq!(
        state.complete(t(120), retryable(), 100),
        HeartbeatDecision::Retry {
            delay: t(2),
            deadline: t(145)
        }
    );
    assert_eq!(state.last_confirmed(), t(100));
    assert_eq!(state.retry_at(), Some(t(122)));
    assert_eq!(state.next_deadline(false), Some(t(122)));
    assert_eq!(state.next_deadline(true), Some(t(145)));
}

#[test]
fn first_failure_after_budget_closes_without_creating_a_new_window() {
    let mut state = heartbeat();
    assert_eq!(
        state.complete(t(45), retryable(), 100),
        HeartbeatDecision::Close(HeartbeatClose::Exhausted)
    );
    assert_eq!(state.failures(), 1);
    assert_eq!(
        state.complete(t(46), HeartbeatOutcome::Confirmed, 100),
        HeartbeatDecision::Close(HeartbeatClose::Exhausted)
    );
}

#[test]
fn heartbeat_success_resets_retries_and_the_next_failure_gets_a_new_budget() {
    let mut state = heartbeat();
    state.complete(t(20), retryable(), 100);
    state.complete(t(22), retryable(), 100);
    assert_eq!(state.failures(), 2);
    assert_eq!(state.retry_at(), Some(t(26)));
    assert_eq!(
        state.complete(t(26), HeartbeatOutcome::Confirmed, 100),
        HeartbeatDecision::Confirmed {
            previous_failures: 2
        }
    );
    assert_eq!(state.failures(), 0);
    assert_eq!(state.retry_at(), None);
    assert_eq!(
        state.complete(t(40), retryable(), 100),
        HeartbeatDecision::Retry {
            delay: t(2),
            deadline: t(71)
        }
    );
}

#[test]
fn heartbeat_late_success_cannot_revive_an_inflight_recovery() {
    for success_at in [45, 46] {
        let mut state = heartbeat();
        state.complete(t(20), retryable(), 100);
        assert!(!state.expired(t(44)));
        assert_eq!(
            state.complete(t(success_at), HeartbeatOutcome::Confirmed, 100),
            HeartbeatDecision::Close(HeartbeatClose::Exhausted)
        );
        assert_eq!(state.last_confirmed(), t(0));
    }
}

#[test]
fn access_limited_retry_may_be_after_budget_and_deadline_still_wins() {
    let mut state = heartbeat();
    assert_eq!(
        state.complete(
            t(40),
            HeartbeatOutcome::Failure(FailureKind::AccessLimited),
            100
        ),
        HeartbeatDecision::Retry {
            delay: t(20),
            deadline: t(45)
        }
    );
    assert_eq!(state.retry_at(), Some(t(60)));
    assert_eq!(state.next_deadline(false), Some(t(45)));
    assert_eq!(state.expire(t(45)), Some(HeartbeatClose::Exhausted));
}

#[test]
fn permanent_and_explicit_game_failures_remain_terminal() {
    for (outcome, reason, failures) in [
        (HeartbeatOutcome::FailedGame, HeartbeatClose::FailedGame, 0),
        (
            HeartbeatOutcome::Failure(FailureKind::Permanent),
            HeartbeatClose::Permanent,
            1,
        ),
    ] {
        let mut state = heartbeat();
        assert_eq!(
            state.complete(t(20), outcome, 100),
            HeartbeatDecision::Close(reason)
        );
        assert_eq!(state.failures(), failures);
        assert_eq!(
            state.complete(t(21), HeartbeatOutcome::Confirmed, 100),
            HeartbeatDecision::Close(reason)
        );
    }
}

#[test]
fn upstream_first_attempt_is_immediate_and_failures_back_off_within_original_window() {
    let mut state = UpstreamRecovery::new(UpstreamPolicy::default(), t(0));
    assert_eq!(state.stable_at(), None);
    assert!(state.disconnected(t(10)));
    assert_eq!(state.deadline(), Some(t(130)));
    assert_eq!(state.begin_attempt(t(10)), Some(1));
    assert_eq!(state.attempt_failed(t(11), 100), Some(t(2)));
    assert_eq!(state.retry_at(), Some(t(13)));
    assert_eq!(state.begin_attempt(t(12)), None);
    assert_eq!(state.begin_attempt(t(13)), Some(2));
    assert_eq!(state.attempt_failed(t(14), 100), Some(t(4)));
    assert_eq!(state.deadline(), Some(t(130)));
    assert_eq!(state.begin_attempt(t(130)), None);
}

#[test]
fn brief_upstream_recoveries_preserve_budget_and_counters() {
    let mut state = UpstreamRecovery::new(UpstreamPolicy::default(), t(0));
    assert!(state.disconnected(t(0)));
    assert_eq!(state.begin_attempt(t(0)), Some(1));
    state.attempt_failed(t(1), 100);
    assert_eq!(state.begin_attempt(t(3)), Some(2));
    assert!(state.restored(t(4)));
    assert_eq!(state.stable_at(), Some(t(64)));
    assert!(!state.observe_healthy(t(40)));
    assert!(state.disconnected(t(40)));
    assert_eq!(state.deadline(), Some(t(120)));
    assert_eq!(state.begin_attempt(t(40)), Some(3));
    assert_eq!(state.attempt_failed(t(41), 100), Some(t(4)));
    assert_eq!(state.failures(), 2);
}

#[test]
fn stable_upstream_resets_only_when_health_event_is_processed() {
    let mut state = UpstreamRecovery::new(UpstreamPolicy::default(), t(0));
    state.disconnected(t(0));
    state.begin_attempt(t(0));
    state.restored(t(1));
    assert!(!state.observe_healthy(t(60)));
    // An incoming disconnect wins over a simultaneously ready health timer in native.
    let mut disconnect_first = state.clone();
    disconnect_first.disconnected(t(61));
    assert_eq!(disconnect_first.deadline(), Some(t(120)));
    assert!(!disconnect_first.observe_healthy(t(61)));
    assert!(state.observe_healthy(t(61)));
    assert_eq!(state.deadline(), None);
    assert_eq!(state.attempts(), 0);
    assert!(state.disconnected(t(61)));
    assert_eq!(state.deadline(), Some(t(181)));
}

#[test]
fn expired_window_rejects_late_socket_and_includes_old_socket_release() {
    let mut state = UpstreamRecovery::new(UpstreamPolicy::default(), t(0));
    state.disconnected(t(10));
    let mut closing = state.clone();
    assert_eq!(closing.begin_attempt(t(130)), None);
    assert_eq!(state.begin_attempt(t(129)), Some(1));
    assert!(!state.restored(t(130)));
    assert!(!state.observe_healthy(t(200)));
}

#[test]
fn healthy_connection_is_not_closed_merely_because_old_budget_elapsed() {
    let mut state = UpstreamRecovery::new(UpstreamPolicy::default(), t(0));
    state.disconnected(t(0));
    state.begin_attempt(t(119));
    assert!(state.restored(t(119)));
    assert_eq!(state.stable_at(), Some(t(179)));
    assert!(!state.observe_healthy(t(120)));
    assert!(state.observe_healthy(t(179)));
    assert!(state.disconnected(t(180)));
    assert_eq!(state.deadline(), Some(t(300)));
}

#[test]
fn disabled_reconnect_never_starts_a_window() {
    let policy = UpstreamPolicy {
        reconnect: None,
        ..Default::default()
    };
    let mut state = UpstreamRecovery::new(policy, t(0));
    assert!(!state.disconnected(t(1)));
    assert_eq!(state.deadline(), None);
    assert_eq!(state.begin_attempt(t(1)), None);
}

#[test]
fn overflow_closes_early_instead_of_wrapping_the_deadline() {
    let mut heartbeat = HeartbeatRecovery::new(HeartbeatPolicy::default(), t(20), Duration::MAX);
    assert_eq!(
        heartbeat.complete(Duration::MAX, retryable(), 100),
        HeartbeatDecision::Close(HeartbeatClose::Exhausted)
    );
    let mut upstream = UpstreamRecovery::new(UpstreamPolicy::default(), Duration::MAX);
    assert!(!upstream.disconnected(Duration::MAX));
}
