//! 直播间会话。
//!
//! 一个房间在同一项目下只保持一场官方互动：一次 `/v2/app/start`、一条官方 WebSocket。
//! 全部场次共用一次 `/v2/app/batchHeartbeat`。下游用身份码接入；同一个码或同一个房间的后来者
//! 挂到这场会话上，共享已经解析好的直播间事件。最后一个下游离开、收到互动结束、上游断开，
//! 或项目心跳被平台拒绝时，调用 `/v2/app/end`。
//!
//! 身份码只用来开启或加入会话，不会出现在调试输出里。

mod error;
mod manager;
mod platform;

pub use error::SessionError;
pub use manager::{Manager, RecvError, Subscription};
