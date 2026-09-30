//! 直播间会话。
//!
//! 同一个 [`Manager`] 及其克隆实例按房间复用一场官方互动和一条官方 WebSocket。
//! 全部活动场次由统一任务发送批量项目心跳，超过单批上限时分批。
//! 调用方用身份码接入；同一个码或开启后返回同一个房间的后来者共享已解析的事件。
//! 独立构造的管理器及不同进程不共享会话。
//!
//! 最后一个订阅者离开并超过配置的闲置宽限期、收到互动结束、上游断开或项目心跳达到关闭条件时，
//! 会尝试调用 `/v2/app/end`。退出前等待 [`Manager::shutdown`]；
//! 清理失败写入日志，运行中的会话断开后不会自动重连。
//!
//! 身份码只用来开启或加入会话，不会出现在调试输出里。

mod error;
mod manager;
mod platform;

pub use error::SessionError;
pub use manager::{Manager, ManagerOptions, RecvError, Subscription};
