//! 使用下游 WebSocket 桩验证示例，无需平台凭据。

use super::*;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async;

type Peer = WebSocketStream<TcpStream>;

async fn fixture() -> (TcpListener, Settings) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let settings = Settings {
        url: format!("ws://{}/ws", listener.local_addr().unwrap()),
        code: "example-code".into(),
    };
    (listener, settings)
}

async fn subscriber(listener: &TcpListener) -> Peer {
    let (stream, _) = listener.accept().await.unwrap();
    let mut peer = accept_async(stream).await.unwrap();
    let Some(Ok(Message::Text(text))) = peer.next().await else {
        panic!("客户端应发送订阅文本");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({
            "type": "subscribe", "code": "example-code"
        })
    );
    peer
}

async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::text(value.to_string())).await.unwrap();
}

fn ready() -> Value {
    json!({"type":"ready", "room_id":123, "game_id":"game-1"})
}

#[tokio::test]
async fn receives_status_events_and_pong_then_resubscribes_after_disconnect() {
    let (listener, settings) = fixture().await;
    let client = tokio::spawn(async move { run_client(&settings, std::future::pending()).await });
    timeout(Duration::from_secs(8), async {
        let mut peer = subscriber(&listener).await;
        send(&mut peer, ready()).await;
        send(&mut peer, json!({"type":"status", "state":"reconnecting"})).await;
        send(&mut peer, json!({"type":"status", "state":"connected"})).await;
        send(
            &mut peer,
            json!({"type":"event", "room_id":123, "cmd":"FUTURE_EVENT", "data":{"value":1}}),
        )
        .await;
        send(&mut peer, json!({"type":"lagged", "skipped":4})).await;
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
        let mut peer = subscriber(&listener).await;
        send(&mut peer, ready()).await;
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
        let mut peer = subscriber(&listener).await;
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
        let mut peer = subscriber(&listener).await;
        send(&mut peer, ready()).await;
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

#[test]
fn event_before_ready_and_malformed_messages_are_rejected() {
    let mut ready_at = None;
    let event = ServerMessage::Event {
        room_id: 123,
        cmd: "DM".into(),
        data: Value::Null,
    };
    assert!(
        handle_message(event, &mut ready_at)
            .unwrap_err()
            .contains("ready")
    );
    assert!(serde_json::from_str::<ServerMessage>(r#"{"type":"ready","room_id":"bad"}"#).is_err());
    let unknown = serde_json::from_str::<ServerMessage>(r#"{"type":"future","extra":1}"#).unwrap();
    assert!(!handle_message(unknown, &mut ready_at).unwrap());
}
