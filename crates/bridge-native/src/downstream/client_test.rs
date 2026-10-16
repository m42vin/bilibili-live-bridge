//! 使用下游 WebSocket 桩验证示例，无需平台凭据。

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async;

use super::*;

type Peer = WebSocketStream<TcpStream>;

async fn fixture() -> (TcpListener, Settings) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = Settings {
        url: format!("ws://{}/ws", listener.local_addr().unwrap()),
        code: "example-code".into(),
    };
    (listener, settings)
}

async fn subscriber(listener: &TcpListener, after: Option<u64>) -> Peer {
    let (stream, _) = listener.accept().await.unwrap();
    let mut peer = accept_async(stream).await.unwrap();
    let Some(Ok(Message::Text(text))) = peer.next().await else {
        panic!("客户端应发送订阅文本");
    };
    let mut expected = json!({"type":"subscribe","code":"example-code","ack":true,"ack_window":64});
    if let Some(seq) = after {
        expected["resume_after"] = json!({"stream_id":"stream-1","seq":seq});
    }
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), expected);
    peer
}

async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::text(value.to_string())).await.unwrap();
}

fn ready(seq: u64, resumed: bool) -> Value {
    json!({"type":"ready", "room_id":123, "game_id":"game-1", "stream_id":"stream-1", "seq":seq,"resumed":resumed,"ack_window":64})
}

#[tokio::test]
async fn receives_status_events_and_pong_then_resubscribes_after_disconnect() {
    let (listener, settings) = fixture().await;
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(8), async {
        let mut peer = subscriber(&listener,None).await;
        send(&mut peer, ready(0,false)).await;
        send(&mut peer, json!({"type":"status", "state":"reconnecting"})).await;
        send(&mut peer, json!({"type":"status", "state":"connected"})).await;
        send(
            &mut peer,
            json!({"type":"event", "room_id":123, "stream_id":"stream-1", "seq":1, "cmd":"FUTURE_EVENT", "data":{"value":1}}),
        )
        .await;
        let ack = peer.next().await.unwrap().unwrap();
        let Message::Text(ack) = ack else { panic!("应确认处理完成的事件"); };
        assert_eq!(serde_json::from_str::<Value>(&ack).unwrap(),json!({"type":"ack","stream_id":"stream-1","seq":1}));
        send(&mut peer, json!({"type":"future_message", "extra":true})).await;
        peer.send(Message::Ping(vec![1, 2, 3].into()))
            .await
            .unwrap();
        assert_eq!(
            peer.next().await.unwrap().unwrap(),
            Message::Pong(vec![1, 2, 3].into())
        );
        // 不发送 closed，模拟传输层意外断开。
        drop(peer);
        let mut peer = subscriber(&listener,Some(1)).await;
        send(&mut peer, ready(1,true)).await;
        send(&mut peer, json!({"type":"closed"})).await;
        assert_eq!(client.await.unwrap(), Ok(()));
        // closed 后任务已经返回，没有再次重连。
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn server_error_stops_instead_of_retrying_invalid_code() {
    let (listener, settings) = fixture().await;
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(4), async {
        let mut peer = subscriber(&listener, None).await;
        send(
            &mut peer,
            json!({"type":"error", "code":"attach_failed", "message":"private details"}),
        )
        .await;
        assert!(client.await.unwrap().unwrap_err().contains("接入失败"));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interrupt_closes_an_active_subscription() {
    let (listener, settings) = fixture().await;
    let (stop, stopped) = oneshot::channel();
    let client = tokio::spawn(async move {
        run_client(&settings, async {
            stopped.await.unwrap();
            Ok(())
        })
        .await
    });
    timeout(Duration::from_secs(4), async {
        let mut peer = subscriber(&listener, None).await;
        send(&mut peer, ready(0, false)).await;
        stop.send(()).unwrap();
        assert!(matches!(
            peer.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        let _ = peer.flush().await;
        assert_eq!(client.await.unwrap(), Ok(()));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn interrupt_is_observed_while_bridge_is_unavailable() {
    let (listener, settings) = fixture().await;
    drop(listener);
    // 无服务可接入；退出期限远小于首次两秒退避。
    let result = timeout(
        Duration::from_millis(500),
        run_client(&settings, async {
            sleep(Duration::from_millis(50)).await;
            Ok(())
        }),
    )
    .await
    .unwrap();
    assert_eq!(result, Ok(()));
}

#[tokio::test]
async fn event_before_ready_and_resume_failure_stop_the_client() {
    for message in [
        json!({"type":"event","room_id":123,"stream_id":"stream-1","seq":1,"cmd":"DM","data":null}),
        json!({"type":"resume_unavailable","reason":"cursor_expired","earliest_after":10,"latest_seq":20}),
    ] {
        let (listener, settings) = fixture().await;
        let client =
            tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
        timeout(Duration::from_secs(4), async {
            let mut peer = subscriber(&listener, None).await;
            send(&mut peer, message).await;
            assert!(client.await.unwrap().is_err());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn duplicate_processed_event_is_acknowledged_and_sequence_gap_is_rejected() {
    let (listener, settings) = fixture().await;
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(4), async {
        let mut peer = subscriber(&listener, None).await;
        send(&mut peer, ready(0, false)).await;
        let event = json!({
            "type": "event", "room_id": 123, "stream_id": "stream-1",
            "seq": 1, "cmd": "DM", "data": null,
        });
        for _ in 0..2 {
            send(&mut peer, event.clone()).await;
            let Message::Text(text) = peer.next().await.unwrap().unwrap() else {
                panic!("应发送 ACK");
            };
            assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["seq"], 1);
        }
        send(
            &mut peer,
            json!({
                "type": "event", "room_id": 123, "stream_id": "stream-1",
                "seq": 3, "cmd": "DM", "data": null,
            }),
        )
        .await;
        assert!(client.await.unwrap().unwrap_err().contains("不连续"));
    })
    .await
    .unwrap();
}

#[test]
fn retry_advice_and_jitter_keep_a_positive_bounded_backoff() {
    assert_eq!(
        retry_delay(Duration::from_secs(2), None, 0),
        Duration::from_secs(2)
    );
    assert_eq!(
        retry_delay(Duration::from_secs(2), Some(3000), 20),
        Duration::from_millis(3600)
    );
    assert!(
        retry_delay(Duration::from_secs(2), Some(u64::MAX), 20)
            <= Duration::from_secs(2 * 24 * 60 * 60)
    );
}

#[tokio::test]
async fn temporary_resource_errors_retry_without_discarding_the_processed_cursor() {
    for (code, advice) in [
        ("server_busy", Some(1)),
        ("rate_limited", None),
        ("subscription_limit", Some(0)),
    ] {
        let (listener, settings) = fixture().await;
        let client =
            tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
        timeout(Duration::from_secs(12), async {
            let mut peer = subscriber(&listener, None).await;
            send(&mut peer, ready(0, false)).await;
            send(
                &mut peer,
                json!({
                    "type": "event", "room_id": 123, "stream_id": "stream-1",
                    "seq": 1, "cmd": "TEST", "data": null,
                }),
            )
            .await;
            let Message::Text(ack) = peer.next().await.unwrap().unwrap() else {
                panic!("应在重试前确认业务处理");
            };
            assert_eq!(serde_json::from_str::<Value>(&ack).unwrap()["seq"], 1);

            let mut reply = json!({"type": "error", "code": code, "message": "busy"});
            if let Some(ms) = advice {
                reply["retry_after_ms"] = json!(ms);
            }
            send(&mut peer, reply).await;
            // 应答关闭，使客户端不用等待清理超时；随后直接等待重连请求。
            while let Some(Ok(message)) = peer.next().await {
                if matches!(message, Message::Close(_)) {
                    let _ = peer.flush().await;
                    break;
                }
            }

            let mut peer = subscriber(&listener, Some(1)).await;
            send(&mut peer, ready(1, true)).await;
            send(&mut peer, json!({"type": "closed"})).await;
            assert_eq!(client.await.unwrap(), Ok(()));
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn http_busy_response_retries_without_service_access_credentials() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (listener, settings) = fixture().await;
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(12), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        let _ = stream.read(&mut buffer).await.unwrap();
        stream.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 0\r\nConnection: close\r\n\r\n",
        ).await.unwrap();
        drop(stream);

        let mut peer = subscriber(&listener, None).await;
        send(&mut peer, ready(0, false)).await;
        send(&mut peer, json!({"type": "closed"})).await;
        assert_eq!(client.await.unwrap(), Ok(()));
    })
    .await
    .unwrap();
}

// 完整读取 POST 正文，避免 TCP 分包使接入测试偶发通过或失败。
async fn http_reply(listener: &TcpListener, status: &str, body: Value) -> Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut bytes = Vec::new();
    let (header_end, length) = loop {
        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            assert!(headers.starts_with("POST /admit HTTP/1.1"));
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let mut buffer = [0; 1024];
        let n = stream.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    let request = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
    let body = body.to_string();
    let reply = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(reply.as_bytes()).await.unwrap();
    request
}

#[tokio::test]
async fn http_admission_reconnects_with_cursor_and_answers_application_ping() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = Settings {
        url: format!("http://{}/admit", http.local_addr().unwrap()),
        code: "example-code".into(),
    };
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(12), async {
        for attempt in 0..2 {
            let request = http_reply(&http, "200 OK", json!({
                "websocket_url": format!("ws://{}/ws?ticket={attempt}", ws.local_addr().unwrap()),
                "expires_in_ms": 10000,
            })).await;
            assert_eq!(request["code"], "example-code");
            assert_eq!(request["ack"], true);
            assert!(request.get("type").is_none());
            if attempt == 0 {
                assert!(request["resume_after"].is_null());
            } else {
                assert_eq!(
                    request["resume_after"],
                    json!({"stream_id":"stream-1","seq":1})
                );
            }

            let (stream, _) = ws.accept().await.unwrap();
            let mut peer = accept_async(stream).await.unwrap();
            // ready 前也允许保活；第一条客户端消息必须是 pong，不能重复 subscribe。
            send(&mut peer, json!({"type":"ping","nonce":"probe"})).await;
            let Message::Text(pong) = peer.next().await.unwrap().unwrap() else {
                panic!("expected pong");
            };
            assert_eq!(
                serde_json::from_str::<Value>(&pong).unwrap(),
                json!({"type":"pong","nonce":"probe"})
            );
            send(&mut peer, ready(attempt, attempt != 0)).await;
            if attempt == 0 {
                send(
                    &mut peer,
                    json!({
                        "type": "event", "room_id": 123, "stream_id": "stream-1",
                        "seq": 1, "cmd": "DM", "data": null,
                    }),
                )
                .await;
                let Message::Text(ack) = peer.next().await.unwrap().unwrap() else {
                    panic!("expected ack");
                };
                assert_eq!(serde_json::from_str::<Value>(&ack).unwrap()["seq"], 1);
                drop(peer);
            } else {
                send(&mut peer, json!({"type":"closed"})).await;
            }
        }
        assert_eq!(client.await.unwrap(), Ok(()));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn admission_errors_preserve_resume_and_classify_retry() {
    for (status, body, retryable) in [
        (
            "429 Too Many Requests",
            json!({"type":"error","code":"rate_limited","message":"busy","retry_after_ms":1234}),
            true,
        ),
        ("503 Service Unavailable", json!({}), true),
        (
            "409 Conflict",
            json!({"type":"resume_unavailable","reason":"cursor_expired","earliest_after":10,"latest_seq":20}),
            false,
        ),
        (
            "403 Forbidden",
            json!({"type":"error","code":"attach_failed","message":"secret"}),
            false,
        ),
        ("302 Found", json!({}), false),
        (
            "200 OK",
            json!({"websocket_url":"https://example.com","expires_in_ms":1000}),
            false,
        ),
        (
            "200 OK",
            json!({"websocket_url":"ws://example.com","expires_in_ms":0}),
            false,
        ),
        (
            "200 OK",
            json!({"websocket_url":"ws://example.com","expires_in_ms":1000,"padding":"x".repeat(8192)}),
            false,
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let settings = Settings {
            url: format!("http://{}/admit", listener.local_addr().unwrap()),
            code: "example-code".into(),
        };
        let after = EventCursor {
            stream_id: "stream-1".into(),
            seq: 7,
        };
        timeout(Duration::from_secs(3), async {
            let (result, request) = tokio::join!(
                admit(&settings, Some(after)),
                http_reply(&listener, status, body)
            );
            assert_eq!(request["resume_after"]["seq"], 7);
            assert!(result.is_err());
            assert_eq!(matches!(result, Err(ConnectFailure::Retry(_))), retryable);
            if let Err(ConnectFailure::Fatal(message)) = result {
                assert!(!message.contains("secret"));
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn interrupt_cancels_pending_http_admission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = Settings {
        url: format!("http://{}/admit", listener.local_addr().unwrap()),
        code: "example-code".into(),
    };
    let (stop, stopped) = oneshot::channel();
    let client = tokio::spawn(async move {
        run_client(&settings, async {
            stopped.await.unwrap();
            Ok(())
        })
        .await
    });
    timeout(Duration::from_secs(2), async {
        let (_stream, _) = listener.accept().await.unwrap();
        stop.send(()).unwrap();
        assert_eq!(client.await.unwrap(), Ok(()));
    })
    .await
    .unwrap();
}

#[test]
fn endpoint_validation_accepts_both_modes_without_embedded_credentials() {
    for url in [
        "http://localhost/admit",
        "https://example.com/admit",
        "ws://localhost/ws",
        "wss://example.com/ws",
    ] {
        assert!(validate_endpoint(url).is_ok());
    }
    for url in [
        "file:///tmp/x",
        "https://user:password@example.com",
        "wss://example.com/#fragment",
        "invalid",
    ] {
        assert!(validate_endpoint(url).is_err());
    }
}
