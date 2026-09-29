use std::io::{ErrorKind, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;
use tracing::instrument::WithSubscriber;

use super::super::auth::{
    SIGNATURE_METHOD, SIGNATURE_VERSION, canonical_string, content_md5, hmac_sha256_hex,
};
use super::super::error::{ApiError, ErrorCode};
use super::*;

#[test]
fn platform_error_ignores_the_success_payload() {
    let error = success_value(
        r#"{"code":7007,"message":"身份码错误","request_id":"req-1","data":null}"#.as_bytes(),
    )
    .unwrap_err();

    assert_eq!(error.platform_code(), Some(ErrorCode::IdentityCode));
    assert_eq!(
        error.to_string(),
        "开放平台返回 7007：身份码错误（request_id: req-1）"
    );
}

#[test]
fn parses_start_result_and_redacts_auth_body() {
    let started = start_result_from_value(
        success_value(
            br#"{
            "code": 0,
            "message": "ok",
            "request_id": "req-1",
            "data": {
                "game_info": { "game_id": "game-1" },
                "websocket_info": {
                    "auth_body": "secret-auth-body",
                    "wss_link": ["wss://a.example", "", "wss://b.example"]
                },
                "anchor_info": { "room_id": 42 }
            }
        }"#,
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(started.game_id(), "game-1");
    assert_eq!(started.auth_body(), "secret-auth-body");
    assert_eq!(
        started.wss_link(),
        ["wss://a.example".to_owned(), "wss://b.example".to_owned()]
    );
    assert_eq!(started.anchor().room_id, 42);
    assert_eq!(started.anchor().union_id, "");
    let rendered = format!("{started:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("secret-auth-body"));
    assert!(rendered.contains("game-1"));
}

#[test]
fn rejects_incomplete_start_payloads() {
    let missing_data = start_result_from_value(
        success_value(br#"{"code":0,"message":"ok","data":null}"#).unwrap(),
    )
    .unwrap_err();
    assert_eq!(missing_data.to_string(), "开放平台响应缺少 data");

    let missing_room = start_result_from_value(
        success_value(
            br#"{
            "code": 0,
            "message": "ok",
            "data": {
                "game_info": { "game_id": "game-1" },
                "websocket_info": { "auth_body": "auth", "wss_link": ["wss://a"] },
                "anchor_info": { "room_id": 0 }
            }
        }"#,
        )
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(missing_room.to_string(), "开放平台响应缺少房间号");
}

#[test]
fn parses_batch_heartbeat_failures() {
    let result = batch_result_from_value(
        success_value(br#"{"code":0,"message":"ok","data":{"failed_game_ids":["gone"]}}"#).unwrap(),
    )
    .unwrap();

    assert_eq!(result.failed_game_ids(), ["gone".to_owned()]);
    assert!(!result.all_succeeded());

    let empty = batch_result_from_value(Value::Null).unwrap();
    assert!(empty.all_succeeded());
}

#[test]
fn rejects_blank_credentials_and_non_positive_app_id() {
    let error = Client::new("  ", "secret", 1).unwrap_err();
    assert_eq!(error.to_string(), "Access Key Id 不能为空");

    let error = Client::new("key-id", " ", 1).unwrap_err();
    assert_eq!(error.to_string(), "Access Key Secret 不能为空");

    let error = Client::new("key-id", "secret", 0).unwrap_err();
    assert_eq!(error.to_string(), "app_id 必须是大于 0 的 i64");
}

#[test]
fn debug_output_redacts_the_access_key_secret() {
    let client = Client::new("key-id", "super-secret", 42).unwrap();
    let rendered = format!("{client:?}");

    assert!(!rendered.contains("super-secret"));
    assert!(rendered.contains("<redacted>"));
    assert!(rendered.contains("key-id"));
    assert!(rendered.contains(API_ORIGIN));
    assert_eq!(client.app_id(), 42);
}

#[tokio::test]
async fn rejects_invalid_calls_before_sending() {
    let client = Client::new("key-id", "super-secret", 1).unwrap();

    assert_eq!(
        client.start("  ").await.unwrap_err().to_string(),
        "身份码不能为空"
    );
    assert_eq!(
        client.heartbeat(" ").await.unwrap_err().to_string(),
        "game_id 不能为空"
    );
    assert_eq!(
        client.end("").await.unwrap_err().to_string(),
        "game_id 不能为空"
    );
    assert_eq!(
        client
            .batch_heartbeat(Vec::<String>::new())
            .await
            .unwrap_err()
            .to_string(),
        "game_ids 不能为空"
    );
    assert_eq!(
        client
            .batch_heartbeat(["a", "b", "a"])
            .await
            .unwrap_err()
            .to_string(),
        "game_ids 存在重复"
    );

    let ids: Vec<String> = (0..=MAX_BATCH_HEARTBEAT)
        .map(|index| index.to_string())
        .collect();
    assert_eq!(
        client.batch_heartbeat(&ids).await.unwrap_err().to_string(),
        "game_ids 单次不能超过 200 个"
    );
}

#[tokio::test]
async fn start_signs_the_posted_json_and_parses_the_session() {
    let (dispatch, logs) = crate::logging::logging_test::capture("trace");
    let (origin, request) = stub(
        r#"{
            "code": 0,
            "message": "ok",
            "request_id": "req-1",
            "data": {
                "game_info": { "game_id": "game-1" },
                "websocket_info": {
                    "auth_body": "secret-auth-body",
                    "wss_link": ["wss://live.example/ws"]
                },
                "anchor_info": {
                    "room_id": 42,
                    "uname": "anchor",
                    "uface": "https://example/face.jpg",
                    "uid": 7,
                    "open_id": "open-1",
                    "union_id": "U_1"
                }
            }
        }"#,
    );
    let client =
        Client::with_origin("key-id", "super-secret", 165_032_067_539_475, &origin).unwrap();
    let started = tokio::time::timeout(Duration::from_secs(3), client.start(" CODE123 "))
        .with_subscriber(dispatch)
        .await
        .expect("start timed out")
        .unwrap();

    assert_eq!(started.game_id(), "game-1");
    assert_eq!(started.anchor().open_id, "open-1");
    assert_eq!(started.anchor().union_id, "U_1");
    let raw = request.recv().unwrap();
    assert!(raw.starts_with("POST /v2/app/start HTTP/1.1\r\n"));
    assert_signed(&raw, "key-id", "super-secret");
    let (_, body) = split_http(&raw);
    assert_eq!(body, r#"{"code":"CODE123","app_id":165032067539475}"#);
    let output = logs.output();
    assert!(output.contains("path=\"/v2/app/start\""), "{output}");
    assert!(output.contains("game_id=\"game-1\""), "{output}");
    assert!(output.contains("room_id=42"), "{output}");
    let (headers, _) = split_http(&raw);
    for secret in [
        "key-id",
        "super-secret",
        "CODE123",
        "secret-auth-body",
        header(&headers, "authorization"),
    ] {
        assert!(!output.contains(secret), "{output}");
    }
}

#[tokio::test]
async fn heartbeat_end_and_batch_use_their_paths() {
    let ok = r#"{"code":0,"message":"ok","request_id":"req-2","data":{}}"#;
    let (origin, heartbeat_request) = stub(ok);
    let client = Client::with_origin("key-id", "super-secret", 42, &origin).unwrap();
    tokio::time::timeout(Duration::from_secs(3), client.heartbeat(" game-1 "))
        .await
        .expect("heartbeat timed out")
        .unwrap();
    let raw = heartbeat_request.recv().unwrap();
    assert!(raw.starts_with("POST /v2/app/heartbeat HTTP/1.1\r\n"));
    assert_signed(&raw, "key-id", "super-secret");
    assert_eq!(split_http(&raw).1, r#"{"game_id":"game-1"}"#);

    let (origin, end_request) = stub(ok);
    let client = Client::with_origin("key-id", "super-secret", 42, &origin).unwrap();
    tokio::time::timeout(Duration::from_secs(3), client.end("game-1"))
        .await
        .expect("end timed out")
        .unwrap();
    let raw = end_request.recv().unwrap();
    assert!(raw.starts_with("POST /v2/app/end HTTP/1.1\r\n"));
    assert_eq!(split_http(&raw).1, r#"{"app_id":42,"game_id":"game-1"}"#);

    let (origin, batch_request) =
        stub(r#"{"code":0,"message":"ok","data":{"failed_game_ids":["game-2"]}}"#);
    let client = Client::with_origin("key-id", "super-secret", 42, &origin).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.batch_heartbeat([" game-1 ", "game-2"]),
    )
    .await
    .expect("batch heartbeat timed out")
    .unwrap();
    assert_eq!(result.failed_game_ids(), ["game-2".to_owned()]);
    let raw = batch_request.recv().unwrap();
    assert!(raw.starts_with("POST /v2/app/batchHeartbeat HTTP/1.1\r\n"));
    assert_eq!(split_http(&raw).1, r#"{"game_ids":["game-1","game-2"]}"#);
}

#[tokio::test]
async fn batch_heartbeat_treats_null_failures_as_success() {
    let (origin, _request) = stub(r#"{"code":0,"message":"ok","data":{"failed_game_ids":null}}"#);
    let client = Client::with_origin("key-id", "super-secret", 42, &origin).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), client.batch_heartbeat(["game-1"]))
        .await
        .expect("batch heartbeat timed out")
        .unwrap();
    assert!(result.all_succeeded());
}

#[tokio::test]
async fn http_error_status_is_not_parsed_as_a_platform_code() {
    let (dispatch, logs) = crate::logging::logging_test::capture("warn");
    let (origin, _request) = stub_response(
        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    let client = Client::with_origin("key-id", "super-secret", 1, &origin).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), client.heartbeat("game-1"))
        .with_subscriber(dispatch)
        .await
        .expect("heartbeat timed out")
        .unwrap_err();

    assert!(matches!(error, ApiError::Status { status: 500 }));
    assert_eq!(error.platform_code(), None);
    assert_eq!(error.to_string(), "开放平台返回 HTTP 500");
    let output = logs.output();
    assert!(output.contains("WARN"), "{output}");
    assert!(output.contains("/v2/app/heartbeat"), "{output}");
    assert!(output.contains("HTTP 500"), "{output}");
}

fn assert_signed(raw: &str, access_key_id: &str, secret: &str) {
    let (headers, body) = split_http(raw);
    let sent_md5 = header(&headers, "x-bili-content-md5");
    assert_eq!(sent_md5, content_md5(body));
    assert_eq!(header(&headers, "content-type"), "application/json");
    assert_eq!(header(&headers, "accept"), "application/json");
    assert_eq!(header(&headers, "x-bili-accesskeyid"), access_key_id);
    assert_eq!(
        header(&headers, "x-bili-signature-method"),
        SIGNATURE_METHOD
    );
    assert_eq!(
        header(&headers, "x-bili-signature-version"),
        SIGNATURE_VERSION
    );
    let canonical = canonical_string(&[
        ("x-bili-accesskeyid", access_key_id),
        ("x-bili-content-md5", sent_md5),
        ("x-bili-signature-method", SIGNATURE_METHOD),
        (
            "x-bili-signature-nonce",
            header(&headers, "x-bili-signature-nonce"),
        ),
        ("x-bili-signature-version", SIGNATURE_VERSION),
        ("x-bili-timestamp", header(&headers, "x-bili-timestamp")),
    ]);
    assert_eq!(
        header(&headers, "authorization"),
        hmac_sha256_hex(secret, &canonical)
    );
    assert!(!raw.contains('?'));
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .unwrap_or_else(|| panic!("missing header {name}"))
}

fn split_http(raw: &str) -> (Vec<(String, String)>, &str) {
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("incomplete request: {raw:?}"));
    let mut lines = head.split("\r\n");
    let _request_line = lines.next().unwrap();
    let headers = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_owned(), value.trim().to_owned())
        })
        .collect();
    (headers, body)
}

fn stub(body: &'static str) -> (String, Receiver<String>) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stub_response(response)
}

fn stub_response(response: impl Into<String>) -> (String, Receiver<String>) {
    let response = response.into();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        let _ = sender.send(request);
        stream.write_all(response.as_bytes()).unwrap();
    });
    (format!("http://{address}"), receiver)
}

fn read_request(stream: &mut std::net::TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 2048];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(size) => {
                buffer.extend_from_slice(&chunk[..size]);
                if request_complete(&buffer) {
                    break;
                }
            }
            Err(error)
                if error.kind() == ErrorKind::WouldBlock || error.kind() == ErrorKind::TimedOut =>
            {
                break;
            }
            Err(error) => panic!("read request: {error}"),
        }
    }
    String::from_utf8(buffer).unwrap()
}

fn request_complete(buffer: &[u8]) -> bool {
    let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let head = std::str::from_utf8(&buffer[..position]).unwrap_or("");
    let Some(length) = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    }) else {
        return false;
    };
    buffer.len() >= position + 4 + length
}
