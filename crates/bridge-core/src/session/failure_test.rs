use super::*;
fn platform(code: i64) -> ApiFailure {
    ApiFailure::Platform(ErrorCode::from_raw(code))
}
#[test]
fn classifies_platform_errors_and_ambiguous_batch_expiration() {
    for code in [4003, 4004, 5000, 5001, 5002] {
        assert_eq!(
            classify(platform(code), 2),
            FailureKind::Retryable,
            "{code}"
        );
    }
    for code in [
        4000, 4001, 4002, 4005, 4006, 4007, 4008, 4010, 4011, 4012, 4013, 5003, 5004, 5005, 5011,
        7004, 7005, 8002,
    ] {
        assert_eq!(
            classify(platform(code), 2),
            FailureKind::Permanent,
            "{code}"
        );
    }
    assert_eq!(classify(platform(4009), 2), FailureKind::AccessLimited);
    assert_eq!(classify(platform(912345), 2), FailureKind::Unknown);
    for code in [7000, 7003] {
        assert_eq!(classify(platform(code), 1), FailureKind::Permanent);
        assert_eq!(classify(platform(code), 2), FailureKind::AmbiguousExpired);
    }
}

#[test]
fn classifies_runtime_summaries() {
    for status in [408, 425, 500, 503, 504, 302] {
        assert_eq!(
            classify(ApiFailure::HttpStatus(status), 2),
            FailureKind::Retryable
        );
    }
    for status in [400, 401, 403, 404, 422, 499] {
        assert_eq!(
            classify(ApiFailure::HttpStatus(status), 2),
            FailureKind::Permanent
        );
    }
    assert_eq!(
        classify(ApiFailure::HttpStatus(429), 2),
        FailureKind::AccessLimited
    );
    assert_eq!(classify(ApiFailure::Invalid, 2), FailureKind::Permanent);
    assert_eq!(classify(ApiFailure::Transport, 2), FailureKind::Retryable);
    assert_eq!(classify(ApiFailure::Decode, 2), FailureKind::Retryable);
    for failure in [UpstreamFailure::Transport, UpstreamFailure::Connect] {
        assert!(failure.retryable());
    }
    for failure in [
        UpstreamFailure::Invalid,
        UpstreamFailure::Protocol,
        UpstreamFailure::Decode,
    ] {
        assert!(!failure.retryable());
    }
}
