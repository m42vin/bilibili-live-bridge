//! 开放平台统一鉴权签名。
//!
//! 抽取 `x-bili-` 请求头，按字典序拼成待签名字符串，再用 Access Key Secret 做 HMAC-SHA256。
//! 字符串末尾没有换行。签名结果是小写十六进制。

use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;

pub(super) const SIGNATURE_METHOD: &str = "HMAC-SHA256";
pub(super) const SIGNATURE_VERSION: &str = "1.0";

type HmacSha256 = Hmac<Sha256>;

pub(super) struct Signature {
    pub(super) content_md5: String,
    pub(super) timestamp: String,
    pub(super) nonce: String,
    pub(super) authorization: String,
}

pub(super) fn sign(
    access_key_id: &str,
    access_key_secret: &str,
    body: &str,
    timestamp: u64,
    nonce: &str,
) -> Signature {
    let content_md5 = content_md5(body);
    let timestamp = timestamp.to_string();
    let pairs = [
        ("x-bili-accesskeyid", access_key_id),
        ("x-bili-content-md5", content_md5.as_str()),
        ("x-bili-signature-method", SIGNATURE_METHOD),
        ("x-bili-signature-nonce", nonce),
        ("x-bili-signature-version", SIGNATURE_VERSION),
        ("x-bili-timestamp", timestamp.as_str()),
    ];
    let canonical = canonical_string(&pairs);
    let authorization = hmac_sha256_hex(access_key_secret, &canonical);
    Signature {
        content_md5,
        timestamp,
        nonce: nonce.to_owned(),
        authorization,
    }
}

pub(super) fn canonical_string(pairs: &[(&str, &str)]) -> String {
    let mut pairs: Vec<_> = pairs
        .iter()
        .copied()
        .filter(|(name, _)| name.starts_with("x-bili-"))
        .collect();
    pairs.sort_unstable_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(super) fn content_md5(body: &str) -> String {
    hex_encode(Md5::digest(body.as_bytes()).as_ref())
}

pub(super) fn hmac_sha256_hex(secret: &str, message: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC-SHA256 accepts any key length");
    mac.update(message.as_bytes());
    hex_encode(mac.finalize().into_bytes().as_ref())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
#[path = "auth_test.rs"]
mod auth_test;
