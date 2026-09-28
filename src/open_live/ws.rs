//! 官方 WebSocket 长连接。
//!
//! 用 [`api::StartResult`](super::api::StartResult) 里的 `wss_link` 和 `auth_body` 建立连接，
//! 发送鉴权包，并维持 WebSocket 心跳。项目心跳仍然调用 [`api::Client::heartbeat`](super::api::Client::heartbeat)。

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
