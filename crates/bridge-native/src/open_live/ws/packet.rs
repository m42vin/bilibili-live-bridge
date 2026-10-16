//! 将共享协议错误映射到原生连接错误。

use bridge_core::open_live::ws::packet;

use super::error::WsError;
#[cfg(test)]
pub(super) use packet::decode_packets;
pub(super) use packet::{FrameItem, MAX_PACKET_LEN, Operation};

/// 编码官方长链数据包，并映射为原生错误类型。
pub(super) fn encode(operation: Operation, body: &[u8]) -> Result<Vec<u8>, WsError> {
    packet::encode(operation, body).map_err(Into::into)
}

/// 展开官方长链帧，并映射为原生错误类型。
pub(super) fn decode_frame(input: &[u8]) -> Result<Vec<FrameItem>, WsError> {
    packet::decode_frame(input).map_err(Into::into)
}

/// 按共享兼容规则检查鉴权回复，并映射错误类型。
pub(super) fn auth_accepted(body: &[u8]) -> Result<(), WsError> {
    packet::auth_accepted(body).map_err(Into::into)
}
