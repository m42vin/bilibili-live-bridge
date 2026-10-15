use super::*;
use crate::session::failure::FailureKind;
fn retry_delay(policy: HeartbeatPolicy, failures: u32, jitter: u32) -> Duration {
    policy.retry_delay(failures, jitter)
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
fn validates_relations_without_a_runtime_clock() {
    let policy = UpstreamPolicy::default();
    assert_eq!(
        policy.reconnect.unwrap().failure_timeout,
        Duration::from_secs(120)
    );
    assert!(policy.validate(Duration::from_secs(20)).is_ok());
    assert!(policy.validate(Duration::from_secs(55)).is_err());
    assert!(policy.validate(Duration::ZERO).is_err());
    let invalid = UpstreamPolicy {
        reconnect: Some(ReconnectPolicy {
            retry_initial_delay: Duration::from_secs(11),
            ..ReconnectPolicy::default()
        }),
        ..policy
    };
    assert!(invalid.validate(Duration::from_secs(20)).is_err());
    for field in 0..3 {
        let mut p = HeartbeatPolicy::default();
        match field {
            0 => p.failure_timeout = Duration::ZERO,
            1 => p.retry_initial_delay = Duration::ZERO,
            _ => p.retry_max_delay = Duration::ZERO,
        }
        assert!(p.validate().is_err());
        let mut p = ReconnectPolicy::default();
        match field {
            0 => p.failure_timeout = Duration::ZERO,
            1 => p.retry_initial_delay = Duration::ZERO,
            _ => p.retry_max_delay = Duration::ZERO,
        }
        assert!(
            UpstreamPolicy {
                reconnect: Some(p),
                ..policy
            }
            .validate(Duration::from_secs(20))
            .is_err()
        );
    }
    assert!(
        HealthPolicy {
            write_timeout: Duration::MAX,
            activity_timeout: Duration::MAX
        }
        .validate(Duration::from_nanos(1))
        .is_err()
    );
    // Core does not impose a native Instant's representable range.
    assert!(
        HeartbeatPolicy {
            failure_timeout: Duration::MAX,
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
    assert!(
        UpstreamPolicy {
            reconnect: None,
            ..policy
        }
        .validate(Duration::from_secs(20))
        .is_ok()
    );
}
#[test]
fn diagnostics_are_advisory_and_report_all_risks() {
    use HeartbeatDiagnostic::*;
    let normal = HeartbeatPolicy::default();
    assert!(
        normal
            .diagnostics(Duration::from_secs(20), Duration::from_secs(10))
            .is_empty()
    );
    let policy = HeartbeatPolicy {
        failure_timeout: Duration::from_secs(46),
        retry_initial_delay: Duration::from_secs(11),
        ..normal
    };
    assert!(policy.validate().is_ok());
    assert_eq!(
        policy.diagnostics(Duration::from_secs(36), Duration::from_secs(10)),
        vec![
            FailureTimeoutExceedsRecommended,
            InsufficientRecoveryMargin,
            InitialDelayExceedsMaximum
        ]
    );
    assert_eq!(policy.retry_initial_delay, Duration::from_secs(11));
    assert_eq!(
        normal.diagnostics(Duration::MAX, Duration::from_secs(1)),
        vec![InsufficientRecoveryMargin]
    );
}
#[test]
fn access_limits_and_reconnect_keep_distinct_delay_rules() {
    let hb = HeartbeatPolicy::default();
    let reconnect = ReconnectPolicy::default();
    for failures in [0, 1, 2, 3, 4, u32::MAX] {
        for jitter in [80, 100, 120] {
            let delay = hb.retry_delay(failures, jitter);
            assert_eq!(reconnect.retry_delay(failures, jitter), delay);
            assert_eq!(
                hb.retry_delay_for(
                    FailureKind::Retryable,
                    Duration::from_secs(20),
                    failures,
                    jitter
                ),
                delay
            );
            assert_eq!(
                hb.retry_delay_for(
                    FailureKind::AccessLimited,
                    Duration::from_secs(20),
                    failures,
                    jitter
                ),
                Duration::from_secs(20)
            );
        }
    }
    let huge = HeartbeatPolicy {
        retry_initial_delay: Duration::MAX,
        retry_max_delay: Duration::MAX,
        ..hb
    };
    assert_eq!(huge.retry_delay(u32::MAX, 120), Duration::MAX);
}
