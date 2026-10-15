use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};

static NOOP_DISPATCH: OnceLock<tracing::Dispatch> = OnceLock::new();

#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub(crate) fn output(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn capture(directives: &str) -> (tracing::Dispatch, CapturedLogs) {
    // tracing-core 在只有一个 dispatcher 时按当前线程缓存 callsite 的 interest。
    // 并行测试可能先在未安装 subscriber 的线程触发同一位置；保留第二个 dispatcher，
    // 让缓存合并全部 subscriber 的 interest，同时保持各测试的日志互相隔离。
    NOOP_DISPATCH
        .get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(super::filter(directives).unwrap())
        .with_ansi(false)
        .without_time()
        .with_writer(move || writer.clone())
        .finish();
    (tracing::Dispatch::new(subscriber), logs)
}

#[test]
fn defaults_to_info_without_directives() {
    let (dispatch, logs) = capture("");
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!("hidden_debug_event");
        tracing::info!("visible_info_event");
        tracing::warn!("visible_warning_event");
    });
    let output = logs.output();
    assert!(!output.contains("hidden_debug_event"));
    assert!(output.contains("visible_info_event"));
    assert!(output.contains("visible_warning_event"));
    assert!(!output.contains('\u{1b}'));
}

#[test]
fn module_directives_enable_debug_without_dependency_noise() {
    let (dispatch, logs) = capture("warn,bilibili_live_bridge=debug");
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!("bridge_debug_event");
        tracing::info!(target: "third_party", "hidden_dependency_event");
        tracing::warn!(target: "third_party", "dependency_warning_event");
    });
    let output = logs.output();
    assert!(output.contains("bridge_debug_event"));
    assert!(!output.contains("hidden_dependency_event"));
    assert!(output.contains("dependency_warning_event"));
}

#[test]
fn rejects_invalid_log_directives() {
    assert!(super::filter("bilibili_live_bridge=verbose").is_err());
}
