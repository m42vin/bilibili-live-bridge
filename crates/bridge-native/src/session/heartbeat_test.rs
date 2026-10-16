use super::super::Manager;
use super::*;
use crate::open_live::error::ErrorCode;

impl Manager {
    /// 立即采样心跳，复用生产请求与反馈处理路径。
    pub(in crate::session) async fn beat_once(&self) {
        pump(&self.state).await;
    }
}

/// 测试用立即采样；生产请求只能由 run_heartbeats 的单请求调度器启动。
async fn pump(state: &State) {
    let pending: Vec<PendingBeat> = {
        let mut beats = lock(&state.beats);
        beats
            .iter_mut()
            .filter(|(_, slot)| !slot.in_flight)
            .map(|(game_id, slot)| {
                slot.in_flight = true;
                slot.normal_due = None;
                PendingBeat {
                    game_id: game_id.clone(),
                    session: slot.session.clone(),
                }
            })
            .collect()
    };
    for chunk in pending.chunks(MAX_BATCH_HEARTBEAT) {
        let pending: Vec<_> = {
            let beats = lock(&state.beats);
            chunk
                .iter()
                .filter(|beat| {
                    beats
                        .get(&beat.game_id)
                        .is_some_and(|slot| Weak::ptr_eq(&slot.session, &beat.session))
                })
                .cloned()
                .collect()
        };
        if pending.is_empty() {
            continue;
        }
        let mut request = request(state, pending);
        let outcome = request.future.as_mut().await;
        expire_recoveries(state);
        finish_request(state, &request.pending, outcome);
    }
}

fn platform(code: i64) -> ApiError {
    ApiError::Platform {
        code: ErrorCode::from_raw(code),
        message: "platform message".to_owned(),
        request_id: None,
    }
}

#[test]
fn classifies_platform_errors_and_ambiguous_batch_expiration() {
    for code in [4003, 4004, 5000, 5001, 5002] {
        assert_eq!(
            classify(&platform(code), 2),
            FailureKind::Retryable,
            "{code}"
        );
    }
    for code in [
        4000, 4001, 4002, 4005, 4006, 4007, 4008, 4010, 4011, 4012, 4013, 5003, 5004, 5005, 5011,
        7004, 7005, 8002,
    ] {
        assert_eq!(
            classify(&platform(code), 2),
            FailureKind::Permanent,
            "{code}"
        );
    }
    assert_eq!(classify(&platform(4009), 2), FailureKind::AccessLimited);
    assert_eq!(classify(&platform(912345), 2), FailureKind::Unknown);
    for code in [7000, 7003] {
        assert_eq!(classify(&platform(code), 1), FailureKind::Permanent);
        assert_eq!(classify(&platform(code), 2), FailureKind::AmbiguousExpired);
    }
}

#[test]
fn classifies_http_transport_decode_and_local_invalid_errors() {
    for status in [408, 425, 500, 503, 504, 302] {
        assert_eq!(
            classify(&ApiError::Status { status }, 2),
            FailureKind::Retryable
        );
    }
    for status in [400, 401, 403, 404, 422, 499] {
        assert_eq!(
            classify(&ApiError::Status { status }, 2),
            FailureKind::Permanent
        );
    }
    assert_eq!(
        classify(&ApiError::Status { status: 429 }, 2),
        FailureKind::AccessLimited
    );
    assert_eq!(
        classify(
            &ApiError::Decode {
                message: "decode".into()
            },
            2
        ),
        FailureKind::Retryable
    );
    assert_eq!(
        classify(
            &ApiError::Invalid {
                message: "invalid".into()
            },
            2
        ),
        FailureKind::Permanent
    );
    let source = reqwest::Client::new()
        .get("invalid url")
        .build()
        .unwrap_err();
    assert_eq!(
        classify(
            &ApiError::Transport {
                message: "transport",
                source: Box::new(source),
            },
            2
        ),
        FailureKind::Retryable
    );
}

#[test]
fn zero_and_unrepresentable_policy_durations_are_invalid() {
    let policy = HeartbeatPolicy::default();
    for duration in [Duration::ZERO, Duration::MAX] {
        for candidate in [
            HeartbeatPolicy {
                failure_timeout: duration,
                ..policy
            },
            HeartbeatPolicy {
                retry_initial_delay: duration,
                ..policy
            },
            HeartbeatPolicy {
                retry_max_delay: duration,
                ..policy
            },
        ] {
            assert!(crate::policy::validate_heartbeat(candidate).is_err());
        }
    }
    assert!(crate::policy::validate_heartbeat(policy).is_ok());
}
