//! 官方长连接的二进制包。
//!
//! 包头 16 字节，大端。`version = 0` 的正文是原文；`version = 2` 的正文是 zlib，
//! 解压后仍是一串完整的包，可能再套一层压缩。
//! 解码不跨帧缓存残包；长度、解压输出、包数量和嵌套深度的限制用于约束异常输入。

use std::io::{Cursor, Read};

use flate2::read::ZlibDecoder;
use serde_json::Value;

use super::cmd::{LiveEvent, parse_command};
use super::error::WsError;

/// 包头长度。开放平台固定为 16。
pub(super) const HEADER_LEN: usize = 16;
/// 单个包的上限，含包头。用来挡住异常的长度字段。
pub(super) const MAX_PACKET_LEN: usize = 1 << 20;

const MAX_DECOMPRESSED: usize = 8 << 20;
const MAX_PACKETS: usize = 4096;
const MAX_DEPTH: u8 = 4;

/// 长连接操作码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub(super) enum Operation {
    /// 客户端心跳。正文为空。
    Heartbeat = 2,
    /// 服务端心跳回复。读循环忽略这个操作码，变体留给协议对照。
    #[allow(dead_code)]
    HeartbeatReply = 3,
    /// 服务端推送的业务消息。
    Notification = 5,
    /// 客户端鉴权。正文是 `auth_body`。
    Auth = 7,
    /// 服务端鉴权回复。
    AuthReply = 8,
}

/// 已验证包头并拆出的正文，压缩内容尚未展开。
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RawPacket {
    pub(super) version: u16,
    pub(super) operation: u32,
    pub(super) body: Vec<u8>,
}

/// 一帧里解析出的内容。心跳回复会被丢掉。
#[derive(Debug, PartialEq)]
pub(super) enum FrameItem {
    Event(Box<LiveEvent>),
    AuthReply(Vec<u8>),
}

/// 编码一个 version 为 0 的完整包，供鉴权和心跳发送使用。
pub(super) fn encode(operation: Operation, body: &[u8]) -> Result<Vec<u8>, WsError> {
    encode_parts(0, operation as u32, body)
}

/// 拆出缓冲区中的完整包序列；残缺包或非法长度直接报错，不留到下一次解码。
pub(super) fn decode_packets(input: &[u8]) -> Result<Vec<RawPacket>, WsError> {
    let mut packets = Vec::new();
    let mut rest = input;
    while !rest.is_empty() {
        if rest.len() < HEADER_LEN {
            return Err(WsError::protocol("长连接包不完整"));
        }
        let packet_len = read_u32(rest) as usize;
        let header_len = read_u16(&rest[4..]) as usize;
        let version = read_u16(&rest[6..]);
        let operation = read_u32(&rest[8..]);
        if !(HEADER_LEN..=MAX_PACKET_LEN).contains(&packet_len) {
            return Err(WsError::protocol("长连接包长度无效"));
        }
        if header_len != HEADER_LEN {
            return Err(WsError::protocol("长连接包头长度不是 16"));
        }
        if rest.len() < packet_len {
            return Err(WsError::protocol("长连接包不完整"));
        }
        packets.push(RawPacket {
            version,
            operation,
            body: rest[HEADER_LEN..packet_len].to_vec(),
        });
        rest = &rest[packet_len..];
    }
    Ok(packets)
}

/// 展开一帧中的包和嵌套压缩，解析事件或鉴权回复，忽略心跳回复和未知操作码。
pub(super) fn decode_frame(input: &[u8]) -> Result<Vec<FrameItem>, WsError> {
    let mut count = 0;
    let mut items = Vec::new();
    for packet in decode_packets(input)? {
        items.extend(expand(packet, 0, &mut count)?);
    }
    Ok(items)
}

/// 鉴权回复里的 `code` 为 0、缺省或正文不是 JSON 时视为成功。非 0 则拒绝。
pub(super) fn auth_accepted(body: &[u8]) -> Result<(), WsError> {
    if body.is_empty() {
        return Ok(());
    }
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return Ok(());
    };
    match value.get("code") {
        None => Ok(()),
        Some(Value::Number(code)) if code.as_i64() == Some(0) => Ok(()),
        Some(code) => Err(WsError::protocol(format!("官方长连接鉴权失败：{code}"))),
    }
}

fn expand(packet: RawPacket, depth: u8, count: &mut usize) -> Result<Vec<FrameItem>, WsError> {
    *count += 1;
    if *count > MAX_PACKETS {
        return Err(WsError::protocol("单个帧里的长连接包过多"));
    }
    if packet.version == 2 {
        if depth >= MAX_DEPTH {
            return Err(WsError::protocol("长连接包压缩嵌套过深"));
        }
        let plain = inflate(&packet.body)?;
        let mut items = Vec::new();
        for inner in decode_packets(&plain)? {
            items.extend(expand(inner, depth + 1, count)?);
        }
        return Ok(items);
    }
    if packet.version != 0 {
        return Err(WsError::protocol(format!(
            "不支持的长连接版本 {}",
            packet.version
        )));
    }
    match packet.operation {
        operation if operation == Operation::Notification as u32 => Ok(vec![FrameItem::Event(
            Box::new(parse_command(&packet.body)?),
        )]),
        operation if operation == Operation::AuthReply as u32 => {
            Ok(vec![FrameItem::AuthReply(packet.body)])
        }
        _ => Ok(Vec::new()),
    }
}

fn inflate(data: &[u8]) -> Result<Vec<u8>, WsError> {
    if data.is_empty() {
        return Err(WsError::protocol("压缩的长连接包是空的"));
    }
    let mut decoder = ZlibDecoder::new(Cursor::new(data));
    let mut output = Vec::new();
    let mut chunk = [0_u8; 8192];
    loop {
        let read = decoder
            .read(&mut chunk)
            .map_err(|_| WsError::protocol("解压长连接包失败"))?;
        if read == 0 {
            break;
        }
        if output.len().saturating_add(read) > MAX_DECOMPRESSED {
            return Err(WsError::protocol("解压后的长连接包超过上限"));
        }
        output.extend_from_slice(&chunk[..read]);
    }
    Ok(output)
}

fn encode_parts(version: u16, operation: u32, body: &[u8]) -> Result<Vec<u8>, WsError> {
    let Some(packet_len) = HEADER_LEN.checked_add(body.len()) else {
        return Err(WsError::protocol("长连接包长度无效"));
    };
    if packet_len > MAX_PACKET_LEN {
        return Err(WsError::protocol("长连接包长度无效"));
    }
    let mut encoded = Vec::with_capacity(packet_len);
    encoded.extend_from_slice(&(packet_len as u32).to_be_bytes());
    encoded.extend_from_slice(&(HEADER_LEN as u16).to_be_bytes());
    encoded.extend_from_slice(&version.to_be_bytes());
    encoded.extend_from_slice(&operation.to_be_bytes());
    encoded.extend_from_slice(&0_u32.to_be_bytes());
    encoded.extend_from_slice(body);
    Ok(encoded)
}

fn read_u16(input: &[u8]) -> u16 {
    u16::from_be_bytes([input[0], input[1]])
}

fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes([input[0], input[1], input[2], input[3]])
}

#[cfg(test)]
#[path = "packet_test.rs"]
mod packet_test;
