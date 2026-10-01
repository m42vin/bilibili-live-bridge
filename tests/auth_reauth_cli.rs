//! `auth_reauth` 探测程序的离线契约测试。
//!
//! 真实鉴权需要平台凭据和一次真实场次，不适合放进测试套件。这里只固定
//! 不依赖网络的部分：缺少凭据和参数错误时必须快速失败并给出可读原因。
//! 需要验证重新鉴权时手动运行 `cargo run --locked --bin auth_reauth`。

use std::process::Command;

/// 用空环境启动，避免本地 `.env` 或已导出的变量把测试变成联网测试。
fn probe(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_auth_reauth"))
        .args(args)
        .env_clear()
        .output()
        .expect("auth_reauth 应该可以启动")
}

#[test]
fn missing_credentials_fail_before_any_network_call() {
    let output = probe(&[]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("BILIBILI_ACCESS_KEY_ID"),
        "应指出缺少哪个变量，实际为：{stderr}"
    );
}

#[test]
fn invalid_arguments_are_rejected_with_usage() {
    let output = probe(&["--wait-secs", "abc"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--wait-secs"),
        "应指出出错的参数，实际为：{stderr}"
    );

    let output = probe(&["--unknown"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--unknown"),
        "应回显无法识别的参数，实际为：{stderr}"
    );
}
