//! 每个管理器独享的有界计数器与固定标签编码，不保存凭据、事件正文或动态错误文本。

use std::sync::Mutex;

use tokio::time::Instant;

use crate::open_live::{error::ApiError, ws::WsError};
use crate::resource::{ResourcePolicy, ResourceSnapshot};
use crate::session::DeliveryPolicy;

const OPERATIONS: [&str; 6] = [
    "start",
    "batch_heartbeat",
    "end",
    "connect",
    "reconnect",
    "socket_close",
];
const OUTCOMES: [&str; 9] = [
    "success",
    "invalid",
    "transport",
    "timeout",
    "decode",
    "http",
    "platform",
    "protocol",
    "cancelled",
];
const FAULTS: [&str; 16] = [
    "upstream_eof",
    "upstream_invalid",
    "upstream_transport",
    "upstream_decode",
    "upstream_protocol",
    "socket_close_timeout",
    "heartbeat_timeout",
    "heartbeat_failed_game",
    "downstream_io",
    "downstream_invalid",
    "downstream_handshake",
    "resume_unavailable",
    "shutdown_timeout",
    "heartbeat_recovery_exhausted",
    "upstream_recovery_exhausted",
    "downstream_oversized",
];
const RESOURCES: [&str; 10] = [
    "connection",
    "handshake",
    "pending_attach",
    "starting_session",
    "session",
    "start_rate",
    "subscriber",
    "client_message",
    "sending_bytes",
    "failed_code",
];

/// 被观测的平台操作，枚举顺序对应固定指标标签。
#[derive(Clone, Copy)]
pub(crate) enum Operation {
    /// 开启场次。
    Start,
    /// 批量项目心跳。
    BatchHeartbeat,
    /// 关闭场次。
    End,
    /// 首次上游连接及鉴权。
    Connect,
    /// 原场次上游恢复。
    Reconnect,
    /// 上游连接关闭。
    SocketClose,
}

/// 有限操作结果类别，不保存原始错误或平台业务码。
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    /// 操作成功。
    Success,
    /// 输入或模型不满足约束。
    Invalid,
    /// 网络传输失败。
    Transport,
    /// HTTP 客户端超时。
    Timeout,
    /// 解码失败。
    Decode,
    /// 非成功 HTTP 状态。
    Http,
    /// 平台业务拒绝。
    Platform,
    /// 长连接协议错误。
    Protocol,
    /// 等待被运行时取消，包括外层截止时间。
    Cancelled,
}

impl Outcome {
    /// 从 API 错误类型提取固定结果类别。
    pub(crate) fn api(error: &ApiError) -> Self {
        match error {
            ApiError::Invalid { .. } => Self::Invalid,
            ApiError::Transport { source, .. } if source.is_timeout() => Self::Timeout,
            ApiError::Transport { .. } => Self::Transport,
            ApiError::Decode { .. } | ApiError::ResponseTooLarge { .. } => Self::Decode,
            ApiError::Status { .. } => Self::Http,
            ApiError::Platform { .. } => Self::Platform,
        }
    }

    /// 从长连接错误类型提取固定结果类别。
    pub(crate) fn ws(error: &WsError) -> Self {
        match error {
            WsError::Invalid { .. } => Self::Invalid,
            WsError::Connect { .. } | WsError::Transport { .. } => Self::Transport,
            WsError::Decode { .. } => Self::Decode,
            WsError::Protocol { .. } => Self::Protocol,
        }
    }
}

/// 对服务有意义的故障事件；各计数器表示不同边界，不能直接相加作为总失败数。
#[derive(Clone, Copy)]
pub(crate) enum Fault {
    /// 上游读取遇到 EOF。
    UpstreamEof,
    /// 上游输入或状态无效。
    UpstreamInvalid,
    /// 上游读取传输失败。
    UpstreamTransport,
    /// 上游业务解码失败。
    UpstreamDecode,
    /// 上游包或协议无效。
    UpstreamProtocol,
    /// 上游关闭超时。
    SocketCloseTimeout,
    /// 调度器的项目心跳请求超时。
    HeartbeatTimeout,
    /// 批量成功响应中拒绝的有效场次数。
    HeartbeatFailedGame,
    /// 下游发送、心跳或编码失败，不包含维护取消。
    DownstreamIo,
    /// 下游业务消息或 ACK 无效。
    DownstreamInvalid,
    /// 下游握手失败或超时。
    DownstreamHandshake,
    /// 下游因不能完整续传而结束。
    ResumeUnavailable,
    /// 服务退出总预算耗尽。
    ShutdownTimeout,
    /// 项目心跳恢复预算耗尽，关闭有效场次。
    HeartbeatRecoveryExhausted,
    /// 原场次上游恢复预算耗尽。
    UpstreamRecoveryExhausted,
    /// 下游 JSON 超出单连接编码上限，未跳过对应事件。
    DownstreamOversized,
}

impl Fault {
    /// 将上游读取错误归入有限类别。
    pub(crate) fn upstream(error: &WsError) -> Self {
        match error {
            WsError::Invalid { .. } => Self::UpstreamInvalid,
            WsError::Connect { .. } | WsError::Transport { .. } => Self::UpstreamTransport,
            WsError::Decode { .. } => Self::UpstreamDecode,
            WsError::Protocol { .. } => Self::UpstreamProtocol,
        }
    }
}

#[derive(Default)]
struct OperationStats {
    outcomes: [u64; 9],
    count: u64,
    seconds: f64,
}

#[derive(Default)]
struct Stats {
    operations: [OperationStats; 6],
    faults: [u64; 16],
}

/// 固定大小的观测状态，与 Manager 克隆共享，独立管理器互不影响。
#[derive(Default)]
pub(crate) struct Counters(Mutex<Stats>);

/// 独立组件的只读采样，保留计费口径，不宣称是整个服务的原子快照。
pub(crate) struct Snapshot {
    /// 服务是否尚未进入关闭。
    pub ready: bool,
    /// 当前资源占用和拒绝累计数。
    pub resources: ResourceSnapshot,
    /// 配置的资源上限。
    pub limits: ResourcePolicy,
    /// 已鉴权的上游与恢复中的上游数量。
    pub upstream: [usize; 2],
    /// 项目心跳当前失败次数大于零的登记数。
    pub heartbeat_recovering: usize,
    /// 日志实际计费字节、流数、事件数和累计移除或未保留事件数。
    pub log: [u64; 4],
    /// 日志配置上限。
    pub delivery: DeliveryPolicy,
}

impl Counters {
    /// 开始观测一个操作，future 被取消也会记录 cancelled 和已等待耗时。
    pub(crate) fn operation(&self, operation: Operation) -> Timer<'_> {
        Timer {
            counters: self,
            operation,
            outcome: Outcome::Cancelled,
            started: Instant::now(),
        }
    }

    /// 增加一种固定故障事件的累计数。
    pub(crate) fn fault(&self, fault: Fault) {
        let mut stats = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let value = &mut stats.faults[fault as usize];
        *value = value.saturating_add(1);
    }

    /// 编码只含固定名称和标签的 Prometheus 0.0.4 文本。
    pub(crate) fn render(&self, snapshot: Snapshot) -> String {
        let mut text = String::new();
        scalar(
            &mut text,
            "bridge_ready",
            "gauge",
            u64::from(snapshot.ready),
        );
        family(&mut text, "bridge_resource_occupied", "gauge");
        for (name, value) in RESOURCES.iter().zip(snapshot.resources.occupied) {
            sample(
                &mut text,
                "bridge_resource_occupied",
                "resource",
                name,
                value,
            );
        }
        family(&mut text, "bridge_resource_limit", "gauge");
        let limits = snapshot.limits;
        for (name, kind, value) in [
            (
                "bridge_sending_bytes",
                "gauge",
                snapshot.resources.sending_bytes as u64,
            ),
            (
                "bridge_send_limit_bytes",
                "gauge",
                limits.max_send_bytes as u64,
            ),
            (
                "bridge_connection_send_limit_bytes",
                "gauge",
                limits.max_connection_send_bytes as u64,
            ),
            (
                "bridge_replay_bytes_per_second",
                "gauge",
                limits.replay.per_sec,
            ),
            ("bridge_replay_burst_bytes", "gauge", limits.replay.burst),
            (
                "bridge_failed_code_entries",
                "gauge",
                snapshot.resources.failed_code_entries as u64,
            ),
            (
                "bridge_failed_code_limit",
                "gauge",
                limits.max_failed_codes as u64,
            ),
            (
                "bridge_failed_code_cooldown_seconds",
                "gauge",
                limits.failed_code_cooldown.as_secs(),
            ),
        ] {
            scalar(&mut text, name, kind, value);
        }
        for (name, value) in RESOURCES.iter().zip([
            limits.max_connections,
            limits.max_handshakes,
            limits.max_pending_attaches,
            limits.max_starting_sessions,
            limits.max_sessions,
        ]) {
            sample(&mut text, "bridge_resource_limit", "resource", name, value);
        }
        scalar(
            &mut text,
            "bridge_subscribers",
            "gauge",
            snapshot.resources.subscribers as u64,
        );
        scalar(
            &mut text,
            "bridge_start_requests_total",
            "counter",
            snapshot.resources.start_calls,
        );
        family(&mut text, "bridge_resource_rejections_total", "counter");
        for (name, value) in RESOURCES.iter().zip(snapshot.resources.rejected) {
            sample(
                &mut text,
                "bridge_resource_rejections_total",
                "resource",
                name,
                value,
            );
        }
        family(&mut text, "bridge_upstream_sessions", "gauge");
        for (name, value) in ["connected", "reconnecting"].iter().zip(snapshot.upstream) {
            sample(&mut text, "bridge_upstream_sessions", "state", name, value);
        }
        scalar(
            &mut text,
            "bridge_heartbeat_recovering_sessions",
            "gauge",
            snapshot.heartbeat_recovering as u64,
        );
        for (name, kind, value) in [
            ("bridge_event_log_bytes", "gauge", snapshot.log[0]),
            ("bridge_event_log_streams", "gauge", snapshot.log[1]),
            ("bridge_event_log_events", "gauge", snapshot.log[2]),
            (
                "bridge_event_log_dropped_events_total",
                "counter",
                snapshot.log[3],
            ),
            (
                "bridge_event_log_limit_bytes",
                "gauge",
                snapshot.delivery.max_bytes as u64,
            ),
            (
                "bridge_event_log_limit_streams",
                "gauge",
                snapshot.delivery.max_streams as u64,
            ),
        ] {
            scalar(&mut text, name, kind, value);
        }
        // 计数与耗时在同一锁内取样，保持每个操作的 count 与结果分类总和一致。
        let stats = self.0.lock().unwrap_or_else(|error| error.into_inner());
        family(&mut text, "bridge_platform_operations_total", "counter");
        for (operation, stats) in OPERATIONS.iter().zip(&stats.operations) {
            for (outcome, count) in OUTCOMES.iter().zip(stats.outcomes) {
                text.push_str(&format!("bridge_platform_operations_total{{operation=\"{operation}\",outcome=\"{outcome}\"}} {count}\n"));
            }
        }
        family(
            &mut text,
            "bridge_platform_operation_duration_seconds",
            "summary",
        );
        for (operation, stats) in OPERATIONS.iter().zip(&stats.operations) {
            text.push_str(&format!("bridge_platform_operation_duration_seconds_count{{operation=\"{operation}\"}} {}\nbridge_platform_operation_duration_seconds_sum{{operation=\"{operation}\"}} {}\n", stats.count, stats.seconds));
        }
        family(&mut text, "bridge_failures_total", "counter");
        for (kind, value) in FAULTS.iter().zip(stats.faults) {
            sample(&mut text, "bridge_failures_total", "kind", kind, value);
        }
        text
    }
}

/// 一个操作的结果与耗时守卫，析构时仅更新固定计数器，不执行网络或清理。
pub(crate) struct Timer<'a> {
    counters: &'a Counters,
    operation: Operation,
    outcome: Outcome,
    started: Instant,
}

impl Timer<'_> {
    /// 登记完成结果，消费守卫以确保只记录一次。
    pub(crate) fn finish(mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let mut stats = self
            .counters
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let stats = &mut stats.operations[self.operation as usize];
        let count = &mut stats.outcomes[self.outcome as usize];
        *count = count.saturating_add(1);
        stats.count = stats.count.saturating_add(1);
        stats.seconds += self.started.elapsed().as_secs_f64();
    }
}

fn family(text: &mut String, name: &str, kind: &str) {
    text.push_str(&format!("# TYPE {name} {kind}\n"));
}

fn scalar(text: &mut String, name: &str, kind: &str, value: u64) {
    family(text, name, kind);
    text.push_str(&format!("{name} {value}\n"));
}

fn sample(text: &mut String, name: &str, key: &str, label: &str, value: impl std::fmt::Display) {
    text.push_str(&format!("{name}{{{key}=\"{label}\"}} {value}\n"));
}

#[cfg(test)]
#[path = "metrics_test.rs"]
mod metrics_test;
