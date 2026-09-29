//! 官方 WebSocket 长连接。
//!
//! 用 [`api::StartResult`](super::api::StartResult) 里的 `wss_link` 和 `auth_body` 建立连接，
//! 发送鉴权包。持续轮询 [`Connection::recv`] 才会驱动 WebSocket 心跳和 Ping/Pong，
//! 本模块不启动独立的心跳任务，也不在断开后自动重连。
//!
//! 项目心跳需要另外调用 [`api::Client::heartbeat`](super::api::Client::heartbeat) 或批量 API；
//! [`crate::session::Manager`] 会统一维护。事件数据结构描述官方推送，
//! 当前未定义面向外部客户端的消息封装。

mod cmd;
mod conn;
mod error;
mod packet;

pub use cmd::{
    BlindGift, ComboInfo, Danmaku, DanmakuMirror, Gift, Guard, InteractionEnd, Like, LiveBoundary,
    LiveEvent, RoomEnter, SuperChat, SuperChatDelete, UserProfile,
};
pub use conn::Connection;
pub use error::WsError;
