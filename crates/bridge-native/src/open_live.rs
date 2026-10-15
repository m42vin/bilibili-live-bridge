//! 哔哩哔哩直播开放平台客户端。
//!
//! 直播桥以 client 模式调用应用 API，并用返回的长连接信息维持一场官方 WebSocket。
//! [`api::Client::start`] 只开启场次，[`ws::Connection::recv`] 驱动长连接读取和心跳；
//! 直接使用本模块时，调用方还要维护项目心跳并在退出时调用 [`api::Client::end`]。
//! 可通过 [`crate::session::Manager`] 统一管理这些操作。

pub mod api;
mod auth;
pub mod error;
pub mod ws;
