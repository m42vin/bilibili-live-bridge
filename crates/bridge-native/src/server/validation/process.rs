//! 测试子进程、HTTP 采样与 Linux/macOS 进程资源读取；不修改父进程环境。

use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, BufReader},
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, mpsc},
    thread,
};

use super::*;

/// 一个只持有假凭据的独立验证服务进程，析构时回收残留进程。
pub(super) struct Service {
    child: Child,
    logs: Arc<Mutex<VecDeque<String>>>,
    reader: Option<thread::JoinHandle<()>>,
    business: SocketAddr,
    admin: SocketAddr,
}

impl Service {
    /// 运行当前测试可执行文件的唯一 child 用例，所有平台地址和凭据均显式隔离。
    pub(super) async fn start(platform: &Platform) -> Result<Self> {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "server::validation::child",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_clear()
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        for (key, value) in [
            ("BRIDGE_VALIDATION_CHILD", "1"),
            ("BRIDGE_VALIDATION_ORIGIN", platform.origin()),
            ("BILIBILI_ACCESS_KEY_ID", "validation-key"),
            ("BILIBILI_ACCESS_KEY_SECRET", "validation-secret"),
            ("BILIBILI_APP_ID", "1"),
            ("BRIDGE_LISTEN", "127.0.0.1:0"),
            ("BRIDGE_ADMIN_LISTEN", "127.0.0.1:0"),
            ("RUST_LOG", "info"),
            ("BRIDGE_SHUTDOWN_TIMEOUT_SECS", "2"),
            ("BRIDGE_WEBSOCKET_HEARTBEAT_SECS", "1"),
            ("BRIDGE_WEBSOCKET_WRITE_TIMEOUT_SECS", "1"),
            ("BRIDGE_WEBSOCKET_ACTIVITY_TIMEOUT_SECS", "3"),
            ("BRIDGE_UPSTREAM_RECONNECT_TIMEOUT_SECS", "5"),
            ("BRIDGE_UPSTREAM_RECONNECT_RETRY_INITIAL_MS", "100"),
            ("BRIDGE_UPSTREAM_RECONNECT_RETRY_MAX_MS", "200"),
            ("BRIDGE_APP_HEARTBEAT_SECS", "1"),
            ("BRIDGE_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS", "20"),
            ("BRIDGE_APP_HEARTBEAT_RETRY_INITIAL_MS", "100"),
            ("BRIDGE_APP_HEARTBEAT_RETRY_MAX_MS", "200"),
            ("BRIDGE_EVENT_RETENTION_SECS", "2"),
            ("BRIDGE_EVENT_LOG_MAX_BYTES", "8388608"),
            ("BRIDGE_MAX_CONNECTIONS", "256"),
            ("BRIDGE_MAX_HANDSHAKES", "64"),
            ("BRIDGE_MAX_PENDING_ATTACHES", "128"),
            ("BRIDGE_MAX_STARTING_SESSIONS", "16"),
            ("BRIDGE_MAX_SESSIONS", "16"),
            ("BRIDGE_MAX_SUBSCRIBERS_PER_ROOM", "128"),
            ("BRIDGE_CLIENT_MESSAGE_RATE_PER_SEC", "100000"),
            ("BRIDGE_CLIENT_MESSAGE_BURST", "100000"),
            ("BRIDGE_START_RATE_PER_SEC", "100"),
            ("BRIDGE_START_BURST", "100"),
            ("BRIDGE_FAILED_CODE_COOLDOWN_SECS", "1"),
            ("BRIDGE_REPLAY_BYTES_PER_SEC", "16777216"),
        ] {
            command.env(key, value);
        }
        let mut child = command.spawn()?;
        let stderr = child.stderr.take().unwrap();
        let logs = Arc::new(Mutex::new(VecDeque::new()));
        let ring = logs.clone();
        let (ready, addresses) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stderr)
                .lines()
                .map_while(std::result::Result::ok)
            {
                if line.contains("已开始监听")
                    && let Some(address) = line.split_whitespace().find_map(|part| {
                        part.strip_prefix("listen=")
                            .and_then(|part| part.parse::<SocketAddr>().ok())
                    })
                {
                    let _ = ready.send((line.contains("管理入口"), address));
                }
                let mut ring = ring.lock().unwrap();
                if ring.len() == 128 {
                    ring.pop_front();
                }
                ring.push_back(line);
            }
        });
        let mut service = Self {
            child,
            logs,
            reader: Some(reader),
            business: "127.0.0.1:0".parse()?,
            admin: "127.0.0.1:0".parse()?,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while service.business.port() == 0 || service.admin.port() == 0 {
            while let Ok((admin, address)) = addresses.try_recv() {
                if admin {
                    service.admin = address;
                } else {
                    service.business = address;
                }
            }
            check(
                service.child.try_wait()?.is_none(),
                format!("验证服务启动失败：{}", service.log_tail()),
            )?;
            check(
                Instant::now() < deadline,
                format!("验证服务未发布监听地址：{}", service.log_tail()),
            )?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        check(service.get("/readyz").await?.0 == 200, "启动后未就绪")?;
        Ok(service)
    }

    /// 业务 WebSocket 地址。
    pub(super) fn url(&self) -> String {
        format!("ws://{}/ws", self.business)
    }
    /// 只读子进程 PID。
    pub(super) fn pid(&self) -> u32 {
        self.child.id()
    }
    /// 截取有界日志用于失败定位，只有假配置会进入此子进程。
    pub(super) fn log_tail(&self) -> String {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }
    /// 发送真实 SIGTERM。
    pub(super) fn terminate(&mut self) -> Result<()> {
        check(
            Command::new("/bin/kill")
                .args(["-TERM", &self.pid().to_string()])
                .status()?
                .success(),
            "SIGTERM 发送失败",
        )
    }
    /// 发送不可捕获的强制终止。
    pub(super) fn kill(&mut self) -> Result<()> {
        self.child.kill()?;
        Ok(())
    }
    /// 按实际 wait 状态确认进程已回收，不用固定等待推断退出。
    pub(super) async fn exited(&mut self) -> Result<std::process::ExitStatus> {
        let deadline = Instant::now() + Duration::from_secs(6);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if let Some(reader) = self.reader.take() {
                    reader.join().unwrap();
                }
                return Ok(status);
            }
            check(
                Instant::now() < deadline,
                format!("子进程未按预算退出：{}", self.log_tail()),
            )?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    /// 读取一次管理响应，禁用代理且限制采样正文大小。
    pub(super) async fn get(&self, path: &str) -> Result<(u16, String)> {
        get(self.admin, path).await
    }
    /// 读取全部固定指标。
    pub(super) async fn metrics(&self) -> Result<Metrics> {
        metrics(self.admin).await
    }
    /// 管理采样地址，用于父进程并发监测。
    pub(super) fn admin(&self) -> SocketAddr {
        self.admin
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            eprintln!(
                "[validation] 清理未退出子进程，最近日志：\n{}",
                self.log_tail()
            );
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

/// 固定指标键到数值的映射，不保存请求或事件正文。
pub(super) type Metrics = HashMap<String, f64>;

/// 读取管理端口的一次响应。
pub(super) async fn get(address: SocketAddr, path: &str) -> Result<(u16, String)> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()?;
    let response = client.get(format!("http://{address}{path}")).send().await?;
    let status = response.status().as_u16();
    let bytes = response.bytes().await?;
    check(bytes.len() <= 65536, "管理采样响应过大")?;
    Ok((status, String::from_utf8(bytes.to_vec())?))
}

/// 从已完成的响应提取数值。
pub(super) async fn metrics(address: SocketAddr) -> Result<Metrics> {
    let (status, text) = get(address, "/metrics").await?;
    check(status == 200, "指标不可读")?;
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let (name, value) = line.rsplit_once(' ').ok_or("指标没有数值")?;
            Ok((name.to_owned(), value.parse()?))
        })
        .collect()
}

/// 必须存在的指标，缺失不能当作零计入验收。
pub(super) fn value(metrics: &Metrics, key: &str) -> f64 {
    *metrics.get(key).unwrap_or_else(|| panic!("缺少指标 {key}"))
}

/// RSS（KiB）和实际数字文件描述符数量。
pub(super) async fn os_resources(pid: u32) -> Result<(u64, usize)> {
    if cfg!(target_os = "linux") {
        let text = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        let rss = text
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .ok_or("缺少 RSS")?
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()?;
        let fds = std::fs::read_dir(format!("/proc/{pid}/fd"))?.count();
        return Ok((rss, fds));
    }
    check(cfg!(target_os = "macos"), "OS 采样只支持 Linux / macOS")?;
    let rss = timeout(
        Duration::from_secs(3),
        tokio::process::Command::new("/bin/ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    check(rss.status.success(), "ps 采样失败")?;
    let rss = String::from_utf8(rss.stdout)?.trim().parse()?;
    let descriptors = timeout(
        Duration::from_secs(3),
        tokio::process::Command::new("/usr/sbin/lsof")
            .args(["-nP", "-a", "-p", &pid.to_string(), "-Ff"])
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    check(descriptors.status.success(), "lsof 采样失败")?;
    let fds = String::from_utf8(descriptors.stdout)?
        .lines()
        .filter(|line| {
            line.strip_prefix('f')
                .is_some_and(|fd| fd.parse::<u64>().is_ok())
        })
        .count();
    check(fds > 0, "文件描述符采样为空")?;
    Ok((rss, fds))
}
