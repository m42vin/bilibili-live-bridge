use std::env::VarError;
use std::time::Duration;

use super::{DEFAULT_HEARTBEAT, DEFAULT_LISTEN, *};

fn load(vars: &[(&str, &str)]) -> Result<Config, ConfigError> {
    Config::from_reader(|key| {
        vars.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
            .ok_or(VarError::NotPresent)
    })
}

fn required_vars() -> Vec<(&'static str, &'static str)> {
    vec![
        (ENV_ACCESS_KEY_ID, "key-id"),
        (ENV_ACCESS_KEY_SECRET, "super-secret"),
        (ENV_APP_ID, "165032067539475"),
    ]
}

#[test]
fn loads_required_values_and_defaults() {
    let config = load(&required_vars()).unwrap();

    assert_eq!(config.access_key_id(), "key-id");
    assert_eq!(config.access_key_secret(), "super-secret");
    assert_eq!(config.app_id(), 165_032_067_539_475);
    assert_eq!(config.listen(), DEFAULT_LISTEN);
    assert_eq!(config.websocket_heartbeat(), DEFAULT_HEARTBEAT);
    assert_eq!(config.app_heartbeat(), DEFAULT_HEARTBEAT);
    assert_eq!(config.heartbeat_policy(), HeartbeatPolicy::default());
    assert_eq!(config.upstream_policy(), UpstreamPolicy::default());
}

#[test]
fn loads_upstream_policy_and_can_disable_reconnect() {
    let mut vars = required_vars();
    vars.extend([
        (ENV_WEBSOCKET_WRITE_TIMEOUT_SECS, "3"),
        (ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS, "50"),
        (ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS, "90"),
        (ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS, "500"),
        (ENV_UPSTREAM_RECONNECT_RETRY_MAX_MS, "3000"),
    ]);
    let policy = load(&vars).unwrap().upstream_policy();
    assert_eq!(policy.health.write_timeout, Duration::from_secs(3));
    assert_eq!(policy.health.activity_timeout, Duration::from_secs(50));
    let reconnect = policy.reconnect.unwrap();
    assert_eq!(reconnect.failure_timeout, Duration::from_secs(90));
    assert_eq!(reconnect.retry_initial_delay, Duration::from_millis(500));
    assert_eq!(reconnect.retry_max_delay, Duration::from_secs(3));
    let mut vars = required_vars();
    vars.push((ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS, "0"));
    assert!(load(&vars).unwrap().upstream_policy().reconnect.is_none());
}

#[test]
fn rejects_invalid_upstream_durations_and_combinations() {
    for (key, value) in [
        (ENV_WEBSOCKET_WRITE_TIMEOUT_SECS, "0"),
        (ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS, "0"),
        (ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS, "25"),
        (ENV_UPSTREAM_RECONNECT_TIMEOUT_SECS, "-1"),
        (ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS, "0"),
        (ENV_UPSTREAM_RECONNECT_RETRY_INITIAL_MS, "10001"),
        (ENV_UPSTREAM_RECONNECT_RETRY_MAX_MS, "0"),
        (ENV_WEBSOCKET_ACTIVITY_TIMEOUT_SECS, "18446744073709551615"),
    ] {
        let mut vars = required_vars();
        vars.push((key, value));
        assert!(
            matches!(load(&vars), Err(ConfigError::Invalid { .. })),
            "{key}={value}"
        );
    }
}

#[test]
fn loads_heartbeat_policy_and_preserves_unsafe_values() {
    let mut vars = required_vars();
    vars.extend([
        (ENV_APP_HEARTBEAT_SECS, "59"),
        (ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS, " 120 "),
        (ENV_APP_HEARTBEAT_RETRY_INITIAL_MS, "20000"),
        (ENV_APP_HEARTBEAT_RETRY_MAX_MS, "10000"),
    ]);
    let config = load(&vars).unwrap();
    assert_eq!(config.app_heartbeat(), Duration::from_secs(59));
    assert_eq!(
        config.heartbeat_policy(),
        HeartbeatPolicy {
            failure_timeout: Duration::from_secs(120),
            retry_initial_delay: Duration::from_secs(20),
            retry_max_delay: Duration::from_secs(10),
        }
    );
}

#[test]
fn blank_heartbeat_policy_values_use_defaults() {
    let mut vars = required_vars();
    vars.extend([
        (ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS, " "),
        (ENV_APP_HEARTBEAT_RETRY_INITIAL_MS, ""),
        (ENV_APP_HEARTBEAT_RETRY_MAX_MS, "\n"),
    ]);
    assert_eq!(
        load(&vars).unwrap().heartbeat_policy(),
        HeartbeatPolicy::default()
    );
}

#[test]
fn rejects_invalid_heartbeat_policy_values() {
    for key in [
        ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS,
        ENV_APP_HEARTBEAT_RETRY_INITIAL_MS,
        ENV_APP_HEARTBEAT_RETRY_MAX_MS,
    ] {
        for value in ["0", "-1", "1.5", "nope", "18446744073709551616"] {
            let mut vars = required_vars();
            vars.push((key, value));
            assert!(matches!(load(&vars), Err(ConfigError::Invalid { var, .. }) if var == key));
        }
    }
    let mut vars = required_vars();
    vars.push((
        ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS,
        "18446744073709551615",
    ));
    assert!(matches!(
        load(&vars),
        Err(ConfigError::Invalid {
            var: ENV_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS,
            ..
        })
    ));
}

#[test]
fn trims_values_and_applies_optional_settings() {
    let mut vars = required_vars();
    vars[0].1 = "  key-id  ";
    vars[2].1 = " 42 ";
    vars.extend([
        (ENV_LISTEN, " 0.0.0.0:9800 "),
        (ENV_WEBSOCKET_HEARTBEAT_SECS, "15"),
        (ENV_APP_HEARTBEAT_SECS, "59"),
    ]);
    let config = load(&vars).unwrap();

    assert_eq!(config.access_key_id(), "key-id");
    assert_eq!(config.app_id(), 42);
    assert_eq!(config.listen(), "0.0.0.0:9800".parse().unwrap());
    assert_eq!(config.websocket_heartbeat(), Duration::from_secs(15));
    assert_eq!(config.app_heartbeat(), Duration::from_secs(59));
}

#[test]
fn treats_blank_optional_values_as_unset() {
    let mut vars = required_vars();
    vars.extend([
        (ENV_LISTEN, " "),
        (ENV_WEBSOCKET_HEARTBEAT_SECS, ""),
        (ENV_APP_HEARTBEAT_SECS, "\n"),
    ]);
    let config = load(&vars).unwrap();

    assert_eq!(config.listen(), DEFAULT_LISTEN);
    assert_eq!(config.websocket_heartbeat(), DEFAULT_HEARTBEAT);
    assert_eq!(config.app_heartbeat(), DEFAULT_HEARTBEAT);
}

#[test]
fn reports_the_first_missing_required_variable() {
    let error = load(&[(ENV_ACCESS_KEY_SECRET, "secret"), (ENV_APP_ID, "1")]).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::Missing {
            var: ENV_ACCESS_KEY_ID
        }
    ));
    assert_eq!(error.to_string(), "缺少必填环境变量 BILIBILI_ACCESS_KEY_ID");
}

#[test]
fn rejects_blank_secret() {
    let error = load(&[
        (ENV_ACCESS_KEY_ID, "key-id"),
        (ENV_ACCESS_KEY_SECRET, "  "),
        (ENV_APP_ID, "1"),
    ])
    .unwrap_err();

    assert!(matches!(
        error,
        ConfigError::Missing {
            var: ENV_ACCESS_KEY_SECRET
        }
    ));
}

#[test]
fn rejects_app_id_that_is_not_a_positive_i64() {
    for value in ["0", "-1", "nope", "9223372036854775808"] {
        let mut vars = required_vars();
        vars[2].1 = value;
        let error = load(&vars).unwrap_err();
        assert!(
            matches!(
                error,
                ConfigError::Invalid {
                    var: ENV_APP_ID,
                    ..
                }
            ),
            "{value}"
        );
    }
}

#[test]
fn rejects_intervals_outside_the_platform_windows() {
    let cases = [
        (ENV_WEBSOCKET_HEARTBEAT_SECS, "0"),
        (ENV_WEBSOCKET_HEARTBEAT_SECS, "30"),
        (ENV_APP_HEARTBEAT_SECS, "60"),
        (ENV_APP_HEARTBEAT_SECS, "1.5"),
    ];
    for (var, value) in cases {
        let mut vars = required_vars();
        vars.push((var, value));
        let error = load(&vars).unwrap_err();
        assert!(
            matches!(error, ConfigError::Invalid { var: found, .. } if found == var),
            "{var}={value}"
        );
    }
}

#[test]
fn rejects_unparseable_listen_address() {
    let mut vars = required_vars();
    vars.push((ENV_LISTEN, "not-an-address"));
    let error = load(&vars).unwrap_err();

    assert!(matches!(
        error,
        ConfigError::Invalid {
            var: ENV_LISTEN,
            ..
        }
    ));
}

#[test]
fn debug_output_redacts_the_access_key_secret() {
    let config = load(&required_vars()).unwrap();
    let rendered = format!("{config:?}");

    assert!(!rendered.contains("super-secret"));
    assert!(rendered.contains("<redacted>"));
    assert!(rendered.contains("key-id"));
    assert!(rendered.contains("165032067539475"));
}

#[cfg(unix)]
#[test]
fn rejects_non_unicode_environment_values() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let error = Config::from_reader(|key| {
        if key == ENV_ACCESS_KEY_ID {
            Err(VarError::NotUnicode(OsString::from_vec(vec![0xff])))
        } else {
            Err(VarError::NotPresent)
        }
    })
    .unwrap_err();

    assert!(matches!(
        error,
        ConfigError::Invalid {
            var: ENV_ACCESS_KEY_ID,
            ..
        }
    ));
    assert!(!error.to_string().contains('\u{ff}'));
}

#[test]
fn loads_delivery_policy_and_rejects_invalid_limits() {
    let mut vars = required_vars();
    vars.extend([
        (ENV_EVENT_RETENTION_SECS, "60"),
        (ENV_EVENT_LOG_MAX_BYTES, "4096"),
        (ENV_EVENT_LOG_MAX_STREAMS, "4"),
        (ENV_ACK_WINDOW, "8"),
    ]);
    let policy = load(&vars).unwrap().delivery_policy();
    assert_eq!(policy.retention, Duration::from_secs(60));
    assert_eq!(policy.max_bytes, 4096);
    assert_eq!(policy.max_streams, 4);
    assert_eq!(policy.ack_window, 8);
    for key in [
        ENV_EVENT_RETENTION_SECS,
        ENV_EVENT_LOG_MAX_BYTES,
        ENV_EVENT_LOG_MAX_STREAMS,
        ENV_ACK_WINDOW,
    ] {
        for value in ["0", "-1", "bad"] {
            let mut vars = required_vars();
            vars.push((key, value));
            assert!(matches!(load(&vars), Err(ConfigError::Invalid { .. })));
        }
    }
}
