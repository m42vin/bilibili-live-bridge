//! Bilibili 直播开放平台客户端与进程内事件桥。
//!
//! 通常使用 [`session::Manager`] 管理互动场次，通过 [`session::Subscription`] 接收
//! [`open_live::ws::LiveEvent`]。同一个管理器及其克隆实例共享上游会话和批量项目心跳。
//! 需要自行控制场次时，可以直接使用 [`open_live::api::Client`] 和
//! [`open_live::ws::Connection`]，并负责项目心跳与场次结束。
//!
//! 配置只读取进程环境变量，不自动加载 `.env`。库不自动初始化日志；
//! 可执行入口可以调用 [`logging::init`]，也可以使用自己的 tracing subscriber。
//! [`downstream::serve`] 提供下游 WebSocket 服务，`server` 程序组装配置和退出流程。
//!
//! # 接入示例
//!
//! 需要设置配置环境变量和 `AUTH_CODE`。接入完成后，示例在会话结束或 Ctrl-C 时释放订阅，
//! 并在接收流程返回错误时仍等待管理器清理。编译文档时不会运行或访问平台。
//!
//! ```no_run
//! use bilibili_live_bridge::{
//!     config::Config,
//!     logging,
//!     session::{Manager, RecvError},
//! };
//!
//! type Error = Box<dyn std::error::Error + Send + Sync>;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     logging::init()?;
//!     let config = Config::from_env()?;
//!     let code = std::env::var("AUTH_CODE")?;
//!     let manager = Manager::from_config(&config)?;
//!
//!     let result = receive(&manager, &code).await;
//!     manager.shutdown().await;
//!     result
//! }
//!
//! async fn receive(manager: &Manager, code: &str) -> Result<(), Error> {
//!     let mut subscription = manager.attach(code).await?;
//!     loop {
//!         tokio::select! {
//!             signal = tokio::signal::ctrl_c() => {
//!                 signal?;
//!                 return Ok(());
//!             }
//!             incoming = subscription.recv() => match incoming {
//!                 Ok(event) => {
//!                     tracing::info!(cmd = event.cmd(), "收到直播间事件");
//!                     if event.ends_push() {
//!                         return Ok(());
//!                     }
//!                 }
//!                 Err(RecvError::Lagged { skipped }) => {
//!                     tracing::warn!(skipped, "订阅落后，部分事件已被覆盖");
//!                 }
//!                 Err(RecvError::Closed) => return Ok(()),
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! `Lagged` 表示缓冲中的未读事件被覆盖，当前没有持久化补发。订阅析构只触发异步清理；
//! 进程退出前应等待 [`session::Manager::shutdown`]，并保持 Tokio runtime 存活。

pub mod config;
pub mod downstream;
pub mod logging;
pub mod open_live;
pub mod session;
