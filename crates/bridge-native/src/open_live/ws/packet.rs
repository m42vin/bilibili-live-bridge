//! 将共享协议错误映射到原生连接错误。
use super::error::WsError;
use bridge_core::open_live::ws::packet;
#[cfg(test)]
pub(super) use packet::decode_packets;
pub(super) use packet::{FrameItem, MAX_PACKET_LEN, Operation};
pub(super) fn encode(operation: Operation, body: &[u8]) -> Result<Vec<u8>, WsError> {
    packet::encode(operation, body).map_err(Into::into)
}
pub(super) fn decode_frame(input: &[u8]) -> Result<Vec<FrameItem>, WsError> {
    packet::decode_frame(input).map_err(Into::into)
}
pub(super) fn auth_accepted(body: &[u8]) -> Result<(), WsError> {
    packet::auth_accepted(body).map_err(Into::into)
}
