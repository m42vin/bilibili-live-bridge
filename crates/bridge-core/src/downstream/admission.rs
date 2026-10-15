//! 与部署平台无关的 HTTP 接入协议。接入成功仅授予连接资格，ready 才表示订阅成功。
use serde::{Deserialize, Serialize};

/// POST JSON 正文，不含 WebSocket 的 type 字段；使用 validate 校验。
pub type AdmissionRequest = super::protocol::Subscribe;

/// 成功响应（HTTP 200）。URL 含短期不透明凭证，禁止记录完整 URL。
#[derive(Deserialize, Serialize)]
pub struct AdmissionResponse {
    /// 完整的 ws/wss 地址；连接后不再发送 subscribe。
    pub websocket_url: String,
    /// 从服务端生成响应起的凭证有效时长（毫秒），必须为正。
    /// 客户端应扣除整个 HTTP 请求耗时，过期后重新接入。
    pub expires_in_ms: u64,
}

/// HTTP 失败正文，与 WebSocket 错误使用相同的 type 和业务含义。
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdmissionFailure {
    /// 拒绝接入；暂时错误可退避重试。
    Error {
        /// server_busy、rate_limited、subscription_limit 或终止性错误码。
        code: String,
        /// 非敏感的错误说明。
        message: String,
        /// 重试建议，不保证届时可以接入。
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    /// 不能完整续传；客户端不得自动清除游标或新开场次。
    ResumeUnavailable {
        /// 与交付模型相同的失败原因。
        reason: String,
        /// 最早可续传的位置。
        earliest_after: Option<u64>,
        /// 桥已记录的最大序号。
        latest_seq: Option<u64>,
    },
}

/// HTTP 与 WebSocket 接入共用的可重试业务错误分类。
pub fn is_retryable_code(code: &str) -> bool {
    matches!(code, "server_busy" | "rate_limited" | "subscription_limit")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_and_websocket_share_validation() {
        for (body, valid) in [
            (r#"{"code":" code "}"#, true),
            (r#"{"code":" "}"#, false),
            (r#"{"code":"code","ack_window":1}"#, false),
            (r#"{"code":"code","ack":true,"ack_window":0}"#, false),
            (
                r#"{"code":"code","resume_after":{"stream_id":"s","seq":1}}"#,
                false,
            ),
            (
                r#"{"code":"code","ack":true,"resume_after":{"stream_id":"s","seq":1}}"#,
                true,
            ),
        ] {
            let http = serde_json::from_str::<AdmissionRequest>(body)
                .unwrap()
                .validate();
            let ws = super::super::protocol::parse_subscribe(&format!(
                r#"{{"type":"subscribe",{}"#,
                &body[1..]
            ));
            assert_eq!(http.is_ok(), valid);
            assert_eq!(http, ws);
        }
    }

    #[test]
    fn keepalive_has_no_delivery_cursor() {
        use super::super::protocol::{ClientMessage, ServerMessage};
        assert_eq!(
            serde_json::to_string(&ServerMessage::Ping { nonce: "n" }).unwrap(),
            r#"{"type":"ping","nonce":"n"}"#
        );
        let pong: ClientMessage = serde_json::from_str(r#"{"type":"pong","nonce":"n"}"#).unwrap();
        assert!(matches!(pong, ClientMessage::Pong { nonce } if nonce == "n"));
    }
}
