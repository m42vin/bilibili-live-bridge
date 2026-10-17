use std::time::Duration;

use super::*;

fn snapshot() -> Snapshot {
    Snapshot {
        ready: true,
        resources: ResourceSnapshot::default(),
        limits: ResourcePolicy::default(),
        upstream: [0; 2],
        heartbeat_recovering: 0,
        log: [0; 4],
        delivery: DeliveryPolicy::default(),
    }
}

#[tokio::test(start_paused = true)]
async fn completed_and_cancelled_operations_are_counted_once_with_seconds() {
    let counters = Counters::default();
    let timer = counters.operation(Operation::Start);
    tokio::time::advance(Duration::from_secs(2)).await;
    timer.finish(Outcome::Success);
    let timer = counters.operation(Operation::Start);
    tokio::time::advance(Duration::from_secs(3)).await;
    drop(timer);
    counters.fault(Fault::ShutdownTimeout);
    let text = counters.render(snapshot());
    assert!(
        text.contains(
            "bridge_platform_operations_total{operation=\"start\",outcome=\"success\"} 1\n"
        )
    );
    assert!(text.contains(
        "bridge_platform_operations_total{operation=\"start\",outcome=\"cancelled\"} 1\n"
    ));
    assert!(
        text.contains("bridge_platform_operation_duration_seconds_count{operation=\"start\"} 2\n")
    );
    assert!(
        text.contains("bridge_platform_operation_duration_seconds_sum{operation=\"start\"} 5\n")
    );
    assert!(text.contains("bridge_failures_total{kind=\"shutdown_timeout\"} 1\n"));
    assert!(
        Counters::default()
            .render(snapshot())
            .contains("bridge_failures_total{kind=\"shutdown_timeout\"} 0\n")
    );
    assert!(text.ends_with('\n'));
    assert!(text.len() < 32 * 1024);
    let samples: Vec<_> = text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| line.split_once(' ').unwrap().0)
        .collect();
    let unique: std::collections::HashSet<_> = samples.iter().collect();
    assert_eq!(
        samples.len(),
        unique.len(),
        "每个名称与标签组合只能出现一次"
    );
}
