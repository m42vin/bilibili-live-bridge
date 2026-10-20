use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;

use futures_util::FutureExt;
use tokio::net::TcpListener;
use tokio::time::{advance, pause};
use tokio_tungstenite::accept_async;

use super::*;

struct ControlledSink {
    ready: Arc<AtomicBool>,
    flush: Arc<AtomicBool>,
    sent: Arc<Mutex<Vec<Message>>>,
}

impl ControlledSink {
    fn new(ready: bool, flush: bool) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(ready)),
            flush: Arc::new(AtomicBool::new(flush)),
            sent: Default::default(),
        }
    }
}

impl Sink<Message> for ControlledSink {
    type Error = SocketError;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), SocketError>> {
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), SocketError> {
        self.sent.lock().unwrap().push(message);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), SocketError>> {
        if self.flush.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SocketError>> {
        self.poll_flush(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn write_deadline_covers_preparation_and_flush() {
    for ready in [false, true] {
        let mut sink = ControlledSink::new(ready, false);
        let mut write = PendingWrite::default();
        let origin = Instant::now();
        write.queue(Message::Ping(vec![1].into()), Duration::from_secs(5));
        let error = write
            .drive(&mut sink, origin + Duration::from_secs(60))
            .await
            .unwrap_err();
        assert_eq!(origin.elapsed(), Duration::from_secs(5));
        assert!(error.to_string().contains("发送超时"));
        assert_eq!(sink.sent.lock().unwrap().len(), usize::from(ready));
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_send_preserves_frame_flush_and_deadline() {
    let mut sink = ControlledSink::new(false, false);
    let mut write = PendingWrite::default();
    let activity = Instant::now() + Duration::from_secs(60);
    let message = Message::Ping(vec![1, 2].into());
    write.queue(message.clone(), Duration::from_secs(5));
    let deadline = write.deadline;
    assert!(write.drive(&mut sink, activity).now_or_never().is_none());
    assert_eq!(write.message, Some(message.clone()));
    advance(Duration::from_secs(2)).await;
    sink.ready.store(true, Ordering::SeqCst);
    assert!(write.drive(&mut sink, activity).now_or_never().is_none());
    assert!(write.flushing);
    advance(Duration::from_secs(2)).await;
    assert!(write.drive(&mut sink, activity).now_or_never().is_none());
    assert_eq!(write.deadline, deadline);
    sink.flush.store(true, Ordering::SeqCst);
    write.drive(&mut sink, activity).await.unwrap();
    assert_eq!(*sink.sent.lock().unwrap(), vec![message]);
    assert!(write.deadline.is_none());
}

#[tokio::test(start_paused = true)]
async fn expired_write_cannot_be_revived_by_resuming_or_ready_sink() {
    let mut sink = ControlledSink::new(false, true);
    let mut write = PendingWrite::default();
    let activity = Instant::now() + Duration::from_secs(60);
    write.queue(Message::Ping(vec![1].into()), Duration::from_secs(5));
    assert!(write.drive(&mut sink, activity).now_or_never().is_none());
    advance(Duration::from_secs(5)).await;
    sink.ready.store(true, Ordering::SeqCst);
    assert!(
        write
            .drive(&mut sink, activity)
            .await
            .unwrap_err()
            .to_string()
            .contains("发送超时")
    );
    assert!(sink.sent.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn activity_deadline_interrupts_a_blocked_write() {
    let mut sink = ControlledSink::new(true, false);
    let mut write = PendingWrite::default();
    let origin = Instant::now();
    write.queue(Message::Pong(vec![1].into()), Duration::from_secs(5));
    assert!(
        write
            .drive(&mut sink, origin + Duration::from_secs(2))
            .await
            .unwrap_err()
            .to_string()
            .contains("存活超时")
    );
    assert_eq!(origin.elapsed(), Duration::from_secs(2));
}

async fn pair() -> (Connection, WebSocketStream<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::binary(
                packet::encode(Operation::AuthReply, br#"{"code":0}"#).unwrap(),
            ))
            .await
            .unwrap();
        socket
    };
    let (client, server) = tokio::join!(
        Connection::connect([url], "auth", Duration::from_secs(20)),
        server
    );
    (client.unwrap(), server)
}

#[tokio::test(start_paused = true)]
async fn heartbeats_on_the_wire_keep_an_eventless_connection_alive() {
    wire_keepalive_round().await;
}

#[tokio::test(start_paused = true)]
#[ignore = "重复验证暂停时钟与真实线缆 I/O，见 docs/validation.md"]
async fn heartbeat_repeatability() {
    for _ in 0..100 {
        wire_keepalive_round().await;
    }
}

async fn wire_keepalive_round() {
    let (mut connection, mut server) = wire_progress(pair()).await;
    connection.health.activity_timeout = Duration::from_millis(100);
    connection.activity_deadline = Instant::now() + connection.health.activity_timeout;
    connection.heartbeat_every = Duration::from_millis(20);
    connection.heartbeat_at = Instant::now() + connection.heartbeat_every;
    for _ in 0..8 {
        advance(Duration::from_millis(20)).await;
        let frame = wire_progress(async {
            tokio::select! {
                result = connection.recv() => panic!("无事件连接不应结束：{result:?}"),
                frame = server.next() => frame.unwrap().unwrap(),
            }
        })
        .await;
        assert!(matches!(frame, Message::Binary(_)));
        wire_progress(server.send(Message::binary(
            packet::encode(Operation::HeartbeatReply, b"").unwrap(),
        )))
        .await
        .unwrap();
        let expected = Instant::now() + connection.health.activity_timeout;
        wire_progress(std::future::poll_fn(|cx| {
            let result = {
                let future = connection.recv();
                std::pin::pin!(future).poll(cx)
            };
            assert!(result.is_pending(), "心跳不能结束事件读取：{result:?}");
            if connection.activity_deadline == expected {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }))
        .await;
    }
    wire_progress(server.send(Message::Close(None)))
        .await
        .unwrap();
    assert!(wire_progress(connection.recv()).await.unwrap().is_none());
}

// 暂停时钟下持续轮询真实 I/O，避免运行时空闲自动把虚拟时间推进到活动截止时间。
// 墙钟只限制测试等待，不参与被测期限；不以 sleep 猜测内核数据已经可读。
async fn wire_progress<F: std::future::Future>(future: F) -> F::Output {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut future = std::pin::pin!(future);
    loop {
        let result = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
        if let Poll::Ready(value) = result {
            return value;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "本地线缆 I/O 未在墙钟保护期限内完成"
        );
        tokio::task::yield_now().await;
    }
}

fn incoming(connection: &mut Connection, operation: Operation) {
    let body: &[u8] = if operation == Operation::Notification {
        br#"{"cmd":"new-command","data":{"v":1}}"#
    } else {
        b""
    };
    connection
        .consume(Some(Ok(Message::binary(
            packet::encode(operation, body).unwrap(),
        ))))
        .unwrap();
}

#[tokio::test]
async fn valid_events_or_heartbeat_replies_keep_quiet_connection_alive() {
    let (mut connection, _server) = pair().await;
    pause();
    for operation in [Operation::HeartbeatReply, Operation::Notification] {
        for _ in 0..4 {
            advance(Duration::from_secs(20)).await;
            incoming(&mut connection, operation);
            assert_eq!(
                connection.activity_deadline,
                Instant::now() + Duration::from_secs(60)
            );
        }
    }
    assert_eq!(connection.pending_events.len(), 4);
}

#[tokio::test]
async fn control_frames_unknown_operations_and_cached_events_do_not_refresh_health() {
    let (mut connection, _server) = pair().await;
    pause();
    incoming(&mut connection, Operation::Notification);
    incoming(&mut connection, Operation::Notification);
    let deadline = connection.activity_deadline;
    advance(Duration::from_secs(30)).await;
    connection
        .consume(Some(Ok(Message::Pong(vec![1].into()))))
        .unwrap();
    connection
        .consume(Some(Ok(Message::Ping(vec![1].into()))))
        .unwrap();
    assert_eq!(connection.activity_deadline, deadline);
    connection.pending_write = PendingWrite::default();
    incoming(&mut connection, Operation::Heartbeat); // 未知入站操作码。
    assert_eq!(connection.activity_deadline, deadline);
    connection.heartbeat_at = Instant::now() + Duration::from_secs(120);
    assert!(connection.recv().await.unwrap().is_some());
    assert_eq!(connection.activity_deadline, deadline);
    advance(Duration::from_secs(30)).await;
    assert!(
        connection
            .recv()
            .await
            .unwrap_err()
            .to_string()
            .contains("存活超时")
    );
    assert!(connection.write.is_none() && connection.read.is_none());
    assert!(connection.recv().await.unwrap().is_none());
}

#[tokio::test]
async fn cancellation_does_not_reset_activity_timeout() {
    let (mut connection, _server) = pair().await;
    pause();
    connection.heartbeat_at = Instant::now() + Duration::from_secs(120);
    let deadline = connection.activity_deadline;
    assert!(connection.recv().now_or_never().is_none());
    advance(Duration::from_secs(59)).await;
    assert!(connection.recv().now_or_never().is_none());
    assert_eq!(connection.activity_deadline, deadline);
    advance(Duration::from_secs(1)).await;
    assert!(matches!(
        connection.recv().await,
        Err(WsError::Transport { .. })
    ));
}

#[tokio::test]
async fn queued_events_do_not_starve_due_heartbeat() {
    let (mut connection, mut server) = pair().await;
    pause();
    incoming(&mut connection, Operation::Notification);
    advance(Duration::from_secs(20)).await;
    assert!(connection.recv().await.unwrap().is_some());
    let frame = server.next().await.unwrap().unwrap();
    let Message::Binary(frame) = frame else {
        panic!("expected heartbeat");
    };
    assert_eq!(
        packet::decode_packets(&frame).unwrap()[0].operation,
        Operation::Heartbeat as u32
    );
}

#[test]
fn health_policy_and_terminal_close_codes_are_checked() {
    assert!(
        HealthPolicy::default()
            .validate(Duration::from_secs(20))
            .is_ok()
    );
    assert!(
        HealthPolicy::default()
            .validate(Duration::from_secs(55))
            .is_err()
    );
    assert!(
        HealthPolicy {
            write_timeout: Duration::ZERO,
            ..HealthPolicy::default()
        }
        .validate(Duration::from_secs(20))
        .is_err()
    );
    assert!(
        crate::policy::validate_health(
            HealthPolicy {
                activity_timeout: Duration::from_secs(u64::MAX),
                ..HealthPolicy::default()
            },
            Duration::from_secs(20)
        )
        .is_err()
    );
    for code in [
        CloseCode::Protocol,
        CloseCode::Unsupported,
        CloseCode::Invalid,
        CloseCode::Policy,
    ] {
        assert!(fatal_close(code));
    }
    assert!(!fatal_close(CloseCode::Normal));
}
