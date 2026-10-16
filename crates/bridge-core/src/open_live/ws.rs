//! 官方长连接的事件模型与二进制协议，不创建连接。

pub mod cmd;
pub mod error;
pub mod packet;
pub use cmd::*;
pub use error::ProtocolError;
