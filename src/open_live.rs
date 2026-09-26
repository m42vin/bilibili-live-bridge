//! 哔哩哔哩直播开放平台客户端。
//!
//! 直播桥以 client 模式调用应用 API，并用返回的长连接信息维持一场官方 WebSocket。

pub mod api;
mod auth;
pub mod error;
pub mod ws;
