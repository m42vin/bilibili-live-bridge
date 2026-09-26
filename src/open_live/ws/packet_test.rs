use std::io::Write;

use flate2::Compression;
use flate2::write::ZlibEncoder;

use super::super::cmd::LiveEvent;
use super::*;

#[test]
fn auth_packet_matches_the_big_endian_layout() {
    let packet = encode(Operation::Auth, b"{}").unwrap();

    assert_eq!(
        packet,
        [0, 0, 0, 18, 0, 16, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, b'{', b'}',]
    );
}

#[test]
fn heartbeat_packet_has_an_empty_body() {
    let packet = encode(Operation::Heartbeat, b"").unwrap();
    let decoded = decode_packets(&packet).unwrap();

    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].operation, Operation::Heartbeat as u32);
    assert!(decoded[0].body.is_empty());
    assert!(decode_frame(&packet).unwrap().is_empty());
}

#[test]
fn reads_two_packets_from_one_buffer() {
    let mut buffer = encode(Operation::Notification, &dm("first")).unwrap();
    buffer.extend(encode(Operation::Notification, &dm("second")).unwrap());

    let items = decode_frame(&buffer).unwrap();

    assert_eq!(items.len(), 2);
    assert_eq!(event_msg(&items[0]), "first");
    assert_eq!(event_msg(&items[1]), "second");
}

#[test]
fn inflates_a_version_two_bundle_and_skips_heartbeat_replies() {
    let mut plain = encode(Operation::HeartbeatReply, b"").unwrap();
    plain.extend(encode(Operation::Notification, &dm("inside")).unwrap());
    plain.extend(encode(Operation::AuthReply, br#"{"code":0}"#).unwrap());
    let packet = encode_parts(2, Operation::Notification as u32, &compress(&plain)).unwrap();

    let items = decode_frame(&packet).unwrap();

    assert_eq!(items.len(), 2);
    assert_eq!(event_msg(&items[0]), "inside");
    assert_eq!(items[1], FrameItem::AuthReply(br#"{"code":0}"#.to_vec()));
}

#[test]
fn rejects_truncated_short_header_and_unsupported_version() {
    let truncated = {
        let mut packet = encode(Operation::Auth, b"{}").unwrap();
        packet[3] = 32;
        packet
    };
    assert_eq!(
        decode_packets(&truncated).unwrap_err().to_string(),
        "长连接包不完整"
    );

    let mut short_header = encode(Operation::Auth, b"{}").unwrap();
    short_header[5] = 15;
    assert_eq!(
        decode_packets(&short_header).unwrap_err().to_string(),
        "长连接包头长度不是 16"
    );

    let mut version_one = encode(Operation::Notification, &dm("x")).unwrap();
    version_one[7] = 1;
    assert_eq!(
        decode_frame(&version_one).unwrap_err().to_string(),
        "不支持的长连接版本 1"
    );

    let mut huge = [0_u8; HEADER_LEN];
    huge[..4].copy_from_slice(&((MAX_PACKET_LEN as u32) + 1).to_be_bytes());
    huge[4..6].copy_from_slice(&(HEADER_LEN as u16).to_be_bytes());
    assert_eq!(
        decode_packets(&huge).unwrap_err().to_string(),
        "长连接包长度无效"
    );
}

#[test]
fn rejects_compression_nested_too_deeply() {
    let mut body = encode(Operation::Notification, &dm("deep")).unwrap();
    for _ in 0..5 {
        body = encode_parts(2, Operation::Notification as u32, &compress(&body)).unwrap();
    }

    assert_eq!(
        decode_frame(&body).unwrap_err().to_string(),
        "长连接包压缩嵌套过深"
    );
}

#[test]
fn auth_reply_accepts_zero_and_rejects_platform_code() {
    assert!(auth_accepted(b"").is_ok());
    assert!(auth_accepted(br#"{"code":0}"#).is_ok());
    assert!(auth_accepted(br#"{"message":"ok"}"#).is_ok());
    assert!(auth_accepted(b"not-json").is_ok());
    assert_eq!(
        auth_accepted(br#"{"code":7007}"#).unwrap_err().to_string(),
        "官方长连接鉴权失败：7007"
    );
}

fn dm(msg: &str) -> Vec<u8> {
    format!(r#"{{"cmd":"LIVE_OPEN_PLATFORM_DM","data":{{"msg":"{msg}"}}}}"#).into_bytes()
}

fn event_msg(item: &FrameItem) -> &str {
    match item {
        FrameItem::Event(event) => match event.as_ref() {
            LiveEvent::Danmaku(danmaku) => danmaku.msg.as_str(),
            other => panic!("expected danmaku, got {other:?}"),
        },
        other => panic!("expected danmaku, got {other:?}"),
    }
}

fn compress(plain: &[u8]) -> Vec<u8> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(plain).unwrap();
    encoder.finish().unwrap()
}
