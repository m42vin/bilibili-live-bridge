//! 与运行时无关的直播桥协议、签名和数据模型。
//! 时间戳、随机数、网络连接与任务调度由运行时适配器提供。

pub mod delivery;
pub mod downstream;
pub mod open_live;
