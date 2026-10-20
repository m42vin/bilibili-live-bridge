//! 验证可执行入口的运行时收尾不会在服务退出预算之外无限等待。

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use super::*;

#[test]
fn runtime_exit_preserves_failure_without_waiting_for_blocking_helpers() {
    let (release, blocked) = mpsc::channel();
    let (started, running) = mpsc::channel();
    let (stopped, helper_done) = mpsc::channel();
    let (finished, result) = mpsc::channel();
    let server = thread::spawn(move || {
        let outcome = run_runtime(async {
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
                stopped.send(()).unwrap();
            });
            // 明确确认辅助任务已经在阻塞，而非尚未开始就被运行时取消。
            tokio::task::spawn_blocking(move || running.recv().unwrap())
                .await
                .unwrap();
            Err::<(), _>(io::Error::new(
                io::ErrorKind::TimedOut,
                "test shutdown budget",
            ))
        });
        finished.send(outcome).unwrap();
    });

    // 先观察入口返回，再解除辅助任务；失败路径也先清理线程，避免测试泄漏。
    let outcome = result.recv_timeout(Duration::from_secs(3));
    release.send(()).unwrap();
    server.join().unwrap();
    helper_done.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        outcome.unwrap().unwrap().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}
