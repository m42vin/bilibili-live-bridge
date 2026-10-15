use super::*;
use crate::open_live::error::ErrorCode;

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
fn backoff_is_exponential_jittered_capped_and_saturating() {
    let policy = HeartbeatPolicy::default();
    for (failures, base) in [(1, 2), (2, 4), (3, 8), (4, 10), (u32::MAX, 10)] {
        for jitter in 80..=120 {
            let expected = (Duration::from_secs(base).mul_f64(f64::from(jitter) / 100.0))
                .min(policy.retry_max_delay);
            assert_eq!(retry_delay(policy, failures, jitter), expected);
        }
    }
    let tiny = HeartbeatPolicy {
        retry_initial_delay: Duration::from_nanos(1),
        retry_max_delay: Duration::from_secs(10),
        ..policy
    };
    assert_eq!(retry_delay(tiny, 1, 80), Duration::from_nanos(1));
    assert_eq!(
        retry_delay(tiny, 34, 100),
        Duration::from_nanos(1_u64 << 33)
    );
    assert_eq!(retry_delay(tiny, u32::MAX, 100), Duration::from_secs(10));
    let oversized_initial = HeartbeatPolicy {
        retry_initial_delay: Duration::from_secs(20),
        ..policy
    };
    assert_eq!(
        retry_delay(oversized_initial, 1, 120),
        Duration::from_secs(10)
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
            assert!(candidate.validate().is_err());
        }
    }
    assert!(policy.validate().is_ok());
}
