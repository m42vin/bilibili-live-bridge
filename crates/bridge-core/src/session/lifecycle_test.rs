use super::*;

fn t(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn starting(grace: u64) -> SessionLifecycle {
    SessionLifecycle::starting(ManagerOptions {
        idle_grace: t(grace),
    })
}

fn subscribed(grace: u64) -> SessionLifecycle {
    let mut state = starting(grace);
    assert!(state.ready());
    assert!(state.subscribe(t(0)));
    state
}

#[test]
fn concurrent_admissions_reuse_starting_but_cannot_subscribe_until_ready() {
    let mut state = starting(30);
    assert_eq!(state.phase(), AdmissionPhase::Starting);
    assert_eq!(state.claim(), ClaimDecision::Reuse);
    assert!(!state.subscribe(t(0)));
    assert_eq!(state.idle_deadline(), None);
    assert!(!state.expire_idle(t(100)));
    assert!(state.ready());
    assert!(!state.ready());
    assert!(state.subscribe(t(100)));
    assert!(state.subscribe(t(101)));
    assert_eq!(state.subscriber_count(), 2);
}

#[test]
fn only_last_subscriber_starts_idle_grace() {
    let mut state = subscribed(30);
    state.subscribe(t(0));
    assert_eq!(state.release(t(1)), IdleAction::Unchanged);
    assert_eq!(state.idle_deadline(), None);
    assert_eq!(state.release(t(2)), IdleAction::Schedule(t(32)));
    assert_eq!(state.claim(), ClaimDecision::Reuse);
    assert_eq!(state.release(t(3)), IdleAction::Unchanged);
    assert_eq!(state.idle_deadline(), Some(t(32)));
}

#[test]
fn rejoin_cancels_old_idle_deadline_and_a_new_leave_gets_a_new_deadline() {
    let mut state = subscribed(30);
    assert_eq!(state.release(t(0)), IdleAction::Schedule(t(30)));
    assert!(state.subscribe(t(20)));
    assert_eq!(state.idle_deadline(), None);
    assert!(!state.expire_idle(t(30)));
    assert_eq!(state.release(t(31)), IdleAction::Schedule(t(61)));
    assert!(!state.expire_idle(t(32)));
    assert!(state.expire_idle(t(61)));
    assert!(!state.expire_idle(t(62)));
    assert_eq!(state.idle_deadline(), None);
}

#[test]
fn exact_idle_deadline_rejects_rejoin_even_before_timer_delivery() {
    let mut state = subscribed(30);
    state.release(t(0));
    assert!(!state.subscribe(t(30)));
    assert!(state.is_closing());
    assert_eq!(state.subscriber_count(), 0);
    assert_eq!(state.claim(), ClaimDecision::WaitForCleanup);
    assert!(!state.is_finished());
}

#[test]
fn zero_grace_stops_immediately_but_cleanup_still_must_finish() {
    let mut state = subscribed(0);
    assert_eq!(state.release(t(1)), IdleAction::Close);
    assert_eq!(state.phase(), AdmissionPhase::Ready);
    assert_eq!(state.claim(), ClaimDecision::WaitForCleanup);
    assert!(!state.subscribe(t(1)));
    assert!(!state.complete_cleanup());
    assert!(state.close());
    assert!(!state.is_finished());
    assert!(state.complete_cleanup());
    assert!(!state.complete_cleanup());
    assert_eq!(state.claim(), ClaimDecision::Replace);
}

#[test]
fn close_during_start_can_publish_ready_but_cannot_admit_subscribers() {
    let mut state = starting(30);
    assert!(state.begin_close());
    assert!(!state.begin_close());
    assert!(state.ready());
    assert!(!state.subscribe(t(1)));
    assert_eq!(state.subscriber_count(), 0);
    assert!(state.fail());
    assert!(state.complete_cleanup());
    assert_eq!(state.phase(), AdmissionPhase::Failed);
}

#[test]
fn shutdown_bypasses_grace_and_does_not_wait_for_subscription_drop() {
    let mut state = subscribed(30);
    assert!(state.begin_close());
    assert_eq!(state.claim(), ClaimDecision::WaitForCleanup);
    assert!(state.close());
    assert!(state.complete_cleanup());
    assert_eq!(state.subscriber_count(), 1);
    assert_eq!(state.release(t(2)), IdleAction::Unchanged);
    assert_eq!(state.subscriber_count(), 0);
    assert_eq!(state.idle_deadline(), None);
}

#[test]
fn each_terminal_phase_is_sticky_and_requires_explicit_completion() {
    for phase in [
        AdmissionPhase::Failed,
        AdmissionPhase::Redirected,
        AdmissionPhase::Closed,
    ] {
        let mut state = starting(0);
        let changed = match phase {
            AdmissionPhase::Failed => state.fail(),
            AdmissionPhase::Redirected => state.redirect(),
            AdmissionPhase::Closed => state.close(),
            _ => unreachable!(),
        };
        assert!(changed);
        assert_eq!(state.phase(), phase);
        assert_eq!(state.claim(), ClaimDecision::WaitForCleanup);
        assert!(!state.ready());
        assert!(!state.subscribe(t(0)));
        assert!(!state.fail());
        assert!(!state.redirect());
        assert!(!state.close());
        assert!(state.complete_cleanup());
        assert_eq!(state.phase(), phase);
        assert_eq!(state.claim(), ClaimDecision::Replace);
    }
}

#[test]
fn incomplete_admission_cannot_be_marked_finished() {
    let mut state = starting(0);
    assert!(!state.complete_cleanup());
    state.ready();
    assert!(!state.complete_cleanup());
    state.begin_close();
    assert!(!state.complete_cleanup());
}

#[test]
fn unrepresentable_idle_deadline_closes_without_panicking() {
    let mut state = subscribed(30);
    assert_eq!(state.release(Duration::MAX), IdleAction::Close);
    assert!(state.is_closing());
}

#[test]
fn cancelled_start_without_id_only_requires_local_failure() {
    let mut owner = AdmissionOwner::default();
    assert_eq!(owner.cancel(), Some(CancelledAdmission { game_id: None }));
    assert_eq!(owner.cancel(), None);
}

#[test]
fn cancelled_connect_returns_the_known_game_once() {
    let mut owner = AdmissionOwner::default();
    owner.acquired("game-1".into());
    assert_eq!(owner.game_id(), Some("game-1"));
    assert_eq!(
        owner.cancel(),
        Some(CancelledAdmission {
            game_id: Some("game-1".into())
        })
    );
    assert_eq!(owner.game_id(), None);
    assert_eq!(owner.cancel(), None);
}

#[test]
fn committed_upstream_owns_cleanup_and_cancelled_leader_must_not_end_it() {
    let mut owner = AdmissionOwner::default();
    owner.acquired("game-1".into());
    owner.commit();
    assert_eq!(owner.game_id(), None);
    assert_eq!(owner.cancel(), None);
    owner.acquired("late-game".into());
    assert_eq!(owner.game_id(), None);
}

#[test]
fn completed_duplicate_cleanup_releases_game_before_redirect_or_failure() {
    let mut owner = AdmissionOwner::default();
    owner.acquired("duplicate".into());
    owner.release_game();
    assert_eq!(owner.cancel(), Some(CancelledAdmission { game_id: None }));
    let mut owner = AdmissionOwner::default();
    owner.acquired("duplicate".into());
    owner.release_game();
    owner.commit();
    assert_eq!(owner.cancel(), None);
}

#[test]
fn pending_admissions_keep_ready_session_alive_until_the_last_waiter_leaves() {
    let mut life = SessionLifecycle::<Duration>::starting(ManagerOptions::default());
    assert!(life.begin_attach(Duration::ZERO));
    life.ready();
    assert_eq!(life.idle_if_unused(Duration::ZERO), IdleAction::Unchanged);
    assert!(life.subscribe(Duration::ZERO));
    assert_eq!(life.end_attach(Duration::ZERO), IdleAction::Unchanged);
    assert_eq!(life.release(Duration::ZERO), IdleAction::Close);
}
