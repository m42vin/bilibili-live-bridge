use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use super::super::cmd::LiveEvent;
use super::super::packet::{Operation, decode_packets, encode};
use super::*;

#[tokio::test]
async fn rejects_invalid_arguments_before_connecting() {
    let error = Connection::connect(["ws://127.0.0.1:9"], "  ", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "auth_body 不能为空");

    let error = Connection::connect(["ws://127.0.0.1:9"], "auth", Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "心跳间隔必须大于 0");

    let error = Connection::connect(["", "  "], "auth", Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "没有可用的官方长连接地址");
}

#[tokio::test]
async fn authenticates_reads_bundled_events_and_sends_heartbeats() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let auth = next_binary(&mut socket).await;
        let packets = decode_packets(&auth).unwrap();
        assert_eq!(packets[0].operation, Operation::Auth as u32);
        assert_eq!(packets[0].body, br#"{"key":"secret-auth"}"#.as_slice());

        socket
            .send(Message::Ping(b"ping".as_slice().into()))
            .await
            .unwrap();
        let pong = next_binary_or_pong(&mut socket).await;
        assert_eq!(pong, Message::Pong(b"ping".as_slice().into()));

        socket
            .send(Message::binary(
                encode(Operation::AuthReply, br#"{"code":0}"#).unwrap(),
            ))
            .await
            .unwrap();
        let mut bundled = encode(Operation::Notification, &dm("one")).unwrap();
        bundled.extend(encode(Operation::Notification, &dm("two")).unwrap());
        socket.send(Message::binary(bundled)).await.unwrap();

        let heartbeat = next_binary(&mut socket).await;
        let packets = decode_packets(&heartbeat).unwrap();
        assert_eq!(packets[0].operation, Operation::Heartbeat as u32);
        assert!(packets[0].body.is_empty());
        socket.send(Message::Close(None)).await.unwrap();
    });

    let client = tokio::spawn(async move {
        let mut connection = timeout(
            Duration::from_secs(2),
            Connection::connect(
                [format!("ws://{address}")],
                r#"{"key":"secret-auth"}"#,
                Duration::from_millis(50),
            ),
        )
        .await
        .expect("connect timed out")
        .unwrap();
        let rendered = format!("{connection:?}");
        assert!(!rendered.contains("secret-auth"));
        assert!(rendered.contains(&format!("ws://{address}")));

        let first = connection.recv().await.unwrap().unwrap();
        let second = connection.recv().await.unwrap().unwrap();
        let third = timeout(Duration::from_secs(2), connection.recv())
            .await
            .expect("close timed out")
            .unwrap();
        assert!(third.is_none());
        (first, second)
    });

    let (server, client) = timeout(Duration::from_secs(3), async {
        tokio::join!(server, client)
    })
    .await
    .expect("websocket exchange timed out");
    server.unwrap();
    let (first, second) = client.unwrap();
    assert_eq!(danmaku_msg(first), "one");
    assert_eq!(danmaku_msg(second), "two");
}

#[tokio::test]
async fn fails_over_a_closed_socket_and_stops_after_auth_rejection() {
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closed_address = closed.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = closed.accept().await.unwrap();
        drop(stream);
    });
    let open = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let open_address = open.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = open.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let _auth = next_binary(&mut socket).await;
        socket
            .send(Message::binary(
                encode(Operation::AuthReply, br#"{"code":0}"#).unwrap(),
            ))
            .await
            .unwrap();
        socket.send(Message::Close(None)).await.unwrap();
    });

    let mut connection = timeout(
        Duration::from_secs(2),
        Connection::connect(
            [
                format!("ws://{closed_address}"),
                format!("ws://{open_address}"),
            ],
            "auth-body",
            Duration::from_secs(20),
        ),
    )
    .await
    .expect("failover timed out")
    .unwrap();
    assert_eq!(connection.endpoint(), format!("ws://{open_address}"));
    assert!(connection.recv().await.unwrap().is_none());

    let rejecting = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rejecting_address = rejecting.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = rejecting.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let _auth = next_binary(&mut socket).await;
        socket
            .send(Message::binary(
                encode(Operation::AuthReply, br#"{"code":7007}"#).unwrap(),
            ))
            .await
            .unwrap();
    });
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unused_address = unused.local_addr().unwrap();
    let accepted = tokio::spawn(async move { unused.accept().await });

    let error = timeout(
        Duration::from_secs(2),
        Connection::connect(
            [
                format!("ws://{rejecting_address}"),
                format!("ws://{unused_address}"),
            ],
            "auth-body",
            Duration::from_secs(20),
        ),
    )
    .await
    .expect("auth rejection timed out")
    .unwrap_err();
    assert_eq!(error.to_string(), "官方长连接鉴权失败：7007");
    assert!(
        timeout(Duration::from_millis(100), accepted).await.is_err(),
        "auth rejection must not dial the next cluster"
    );
}

async fn next_binary(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Vec<u8> {
    match next_binary_or_pong(socket).await {
        Message::Binary(bytes) => bytes.to_vec(),
        other => panic!("expected binary, got {other:?}"),
    }
}

async fn next_binary_or_pong(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Message {
    timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("socket read timed out")
        .expect("socket closed")
        .expect("socket error")
}

fn dm(msg: &str) -> Vec<u8> {
    format!(r#"{{"cmd":"LIVE_OPEN_PLATFORM_DM","data":{{"msg":"{msg}"}}}}"#).into_bytes()
}

fn danmaku_msg(event: LiveEvent) -> String {
    match event {
        LiveEvent::Danmaku(danmaku) => danmaku.msg,
        other => panic!("expected danmaku, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn close_frame_send_times_out_when_the_writer_never_flushes() {
    // 固定阻塞 flush，避免依赖操作系统 TCP 缓冲区大小制造背压。
    let writer = futures_util::sink::unfold((), |(), message| async move {
        assert_eq!(message, Message::Close(None));
        std::future::pending::<Result<(), SocketError>>().await
    });
    let mut writer = std::pin::pin!(writer);
    let started = tokio::time::Instant::now();
    let error = timeout(
        CLOSE_TIMEOUT + Duration::from_secs(1),
        send_close(&mut writer),
    )
    .await
    .expect("close-frame write did not respect its timeout")
    .unwrap_err();
    assert_eq!(started.elapsed(), CLOSE_TIMEOUT);
    assert!(matches!(error, WsError::Transport { .. }));
    assert!(error.to_string().contains("关闭官方长连接超时"));
}

#[tokio::test]
async fn close_frame_send_tolerates_an_already_closed_writer() {
    let writer = futures_util::sink::unfold((), |(), _message| async {
        Err::<(), _>(SocketError::AlreadyClosed)
    });
    let mut writer = std::pin::pin!(writer);
    send_close(&mut writer).await.unwrap();
}
