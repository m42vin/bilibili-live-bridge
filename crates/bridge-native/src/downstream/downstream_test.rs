use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{MaybeTlsStream, accept_async, connect_async};

use super::*;
use crate::open_live::api::Client;
use crate::open_live::ws::LiveEvent;
use crate::resource::{RatePolicy, ResourcePolicy};
use crate::session::{DeliveryPolicy, HeartbeatPolicy, ManagerOptions, UpstreamPolicy};

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type UpstreamMessage = (Value, oneshot::Sender<()>);
const CODE: &str = "private-identity-code";
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn two_clients_share_one_upstream_and_receive_events_in_order() {
    let fixture = Fixture::new().await;
    let (mut first, ready) = fixture.subscribe(CODE).await;
    assert_eq!(
        ready,
        json!({"type":"ready", "room_id":7, "game_id":"game-1"})
    );
    let (mut second, _) = fixture.subscribe(CODE).await;
    for message in ["one", "two"] {
        fixture
            .state
            .emit(
                7,
                json!({"cmd":"LIVE_OPEN_PLATFORM_DM","data":{"msg":message,"room_id":7}}),
            )
            .await;
        let left = next_json(&mut first).await;
        let right = next_json(&mut second).await;
        assert_eq!(left, right);
        assert_eq!(left["data"]["msg"], message);
    }
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop(first);
    fixture
        .state
        .emit(7, json!({"cmd":"FUTURE", "data":{"extra":true}}))
        .await;
    assert_eq!(next_json(&mut second).await["data"], json!({"extra":true}));
    assert!(fixture.state.ends.borrow().is_empty());
    drop(second);
    fixture.stop().await;
}

#[tokio::test]
async fn reconnect_reports_status_before_events_and_keeps_the_same_game() {
    let fixture = Fixture::new().await;
    let (mut first, ready) = fixture.subscribe("code-1").await;
    let room = ready["room_id"].as_i64().unwrap();
    fixture.state.socket_gate.send_replace(false);
    fixture.state.emit(room, Value::Null).await;
    assert_eq!(
        next_json(&mut first).await,
        json!({"type":"status","state":"reconnecting"})
    );
    let (mut second, second_ready) = fixture.subscribe("code-1").await;
    assert_eq!(ready["game_id"], second_ready["game_id"]);
    assert_eq!(
        next_json(&mut second).await,
        json!({"type":"status","state":"reconnecting"})
    );
    *fixture.state.early_event.lock().unwrap() =
        Some(json!({"cmd":"LIVE_OPEN_PLATFORM_DM","data":{"msg":"bundled-after-reconnect"}}));
    fixture.state.socket_gate.send_replace(true);
    assert_eq!(
        next_json(&mut first).await,
        json!({"type":"status","state":"connected"})
    );
    assert_eq!(
        next_json(&mut second).await,
        json!({"type":"status","state":"connected"})
    );
    assert_eq!(
        next_json(&mut first).await["data"]["msg"],
        "bundled-after-reconnect"
    );
    assert_eq!(
        next_json(&mut second).await["data"]["msg"],
        "bundled-after-reconnect"
    );
    fixture
        .state
        .emit(
            room,
            json!({"cmd":"LIVE_OPEN_PLATFORM_DM","data":{"msg":"restored"}}),
        )
        .await;
    assert_eq!(next_json(&mut first).await["data"]["msg"], "restored");
    assert_eq!(next_json(&mut second).await["data"]["msg"], "restored");
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    assert_eq!(*fixture.state.ws_connects.borrow(), 2);
    assert!(fixture.state.ends.borrow().is_empty());
    fixture
        .state
        .emit(
            room,
            json!({"cmd":"LIVE_OPEN_PLATFORM_INTERACTION_END","data":{}}),
        )
        .await;
    assert_eq!(next_json(&mut first).await["type"], "event");
    assert_eq!(next_json(&mut first).await["type"], "closed");
    assert_eq!(next_json(&mut second).await["type"], "event");
    assert_eq!(next_json(&mut second).await["type"], "closed");
    assert!(matches!(next_frame(&mut first).await, Message::Close(_)));
    assert!(matches!(next_frame(&mut second).await, Message::Close(_)));
    drop(first);
    drop(second);
    fixture.stop().await;
}

#[tokio::test]
async fn ready_precedes_events_bundled_with_upstream_authentication() {
    let fixture = Fixture::new().await;
    *fixture.state.early_event.lock().unwrap() =
        Some(json!({"cmd":"EARLY", "data":{"message":"first"}}));
    let (mut client, _) = fixture.subscribe(CODE).await;
    assert_eq!(next_json(&mut client).await["cmd"], "EARLY");
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn different_rooms_do_not_mix_events() {
    let fixture = Fixture::new().await;
    let (mut first, _) = fixture.subscribe(CODE).await;
    let (mut second, ready) = fixture.subscribe("other-room").await;
    assert_eq!(ready["room_id"], 8);
    fixture
        .state
        .emit(8, json!({"cmd":"ROOM_8", "data":8}))
        .await;
    fixture
        .state
        .emit(7, json!({"cmd":"ROOM_7", "data":7}))
        .await;
    assert_eq!(next_json(&mut first).await["cmd"], "ROOM_7");
    assert_eq!(next_json(&mut second).await["cmd"], "ROOM_8");
    assert_eq!(fixture.state.starts.borrow().len(), 2);
    drop((first, second));
    fixture.stop().await;
}

#[tokio::test]
async fn reconnect_reuses_game_and_live_end_keeps_pushing() {
    let fixture = Fixture::new().await;
    let (first, before) = fixture.subscribe(CODE).await;
    drop(first);
    let (mut second, after) = fixture.subscribe(CODE).await;
    assert_eq!(before, after);
    fixture
        .state
        .emit(
            7,
            json!({"cmd":"LIVE_OPEN_PLATFORM_LIVE_END", "data":{"room_id":7}}),
        )
        .await;
    assert_eq!(
        next_json(&mut second).await["cmd"],
        "LIVE_OPEN_PLATFORM_LIVE_END"
    );
    fixture
        .state
        .emit(7, json!({"cmd":"STILL_OPEN", "data":null}))
        .await;
    assert_eq!(next_json(&mut second).await["cmd"], "STILL_OPEN");
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop(second);
    fixture.stop().await;
}

#[tokio::test]
async fn interaction_end_is_delivered_before_closed_and_cleans_once() {
    let fixture = Fixture::new().await;
    let (mut client, _) = fixture.subscribe(CODE).await;
    fixture
        .state
        .emit(
            7,
            json!({"cmd":"LIVE_OPEN_PLATFORM_INTERACTION_END", "data":{"game_id":"game-1"}}),
        )
        .await;
    assert_eq!(
        next_json(&mut client).await["cmd"],
        "LIVE_OPEN_PLATFORM_INTERACTION_END"
    );
    assert_eq!(next_json(&mut client).await, json!({"type":"closed"}));
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    fixture.state.wait_ends(1).await;
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn rejects_invalid_first_messages_and_repeated_subscriptions() {
    let fixture = Fixture::new().await;
    for message in [
        Message::text("bad-json"),
        Message::text(r#"{"type":"subscribe","code":" "}"#),
        Message::binary(vec![1, 2, 3]),
    ] {
        let mut client = fixture.connect().await;
        client.send(message).await.unwrap();
        assert_eq!(next_json(&mut client).await["code"], "invalid_request");
        assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    }
    assert!(fixture.state.starts.borrow().is_empty());
    let (mut client, _) = fixture.subscribe(CODE).await;
    client
        .send(Message::text(
            json!({"type":"subscribe","code":"other-room"}).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(next_json(&mut client).await["code"], "invalid_request");
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn rejects_wrong_path_and_oversized_messages() {
    let fixture = Fixture::new().await;
    let wrong_path = fixture.url.replace("/ws", "/other");
    let error = connect_async(wrong_path).await.unwrap_err();
    assert!(
        matches!(error, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::NOT_FOUND)
    );
    let mut client = fixture.connect().await;
    client
        .send(Message::text("x".repeat(MAX_CLIENT_MESSAGE + 1)))
        .await
        .unwrap();
    let result = timeout(TEST_TIMEOUT, client.next()).await.unwrap();
    assert!(!matches!(result, Some(Ok(Message::Text(_)))));
    assert!(fixture.state.starts.borrow().is_empty());
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn answers_ping_before_subscription_and_hides_platform_error_details() {
    let fixture = Fixture::new().await;
    fixture.state.fail_start.store(true, Ordering::SeqCst);
    let mut client = fixture.connect().await;
    client
        .send(Message::Ping(b"check".as_slice().into()))
        .await
        .unwrap();
    assert_eq!(
        next_frame(&mut client).await,
        Message::Pong(b"check".as_slice().into())
    );
    client
        .send(Message::text(
            json!({"type":"subscribe","code":CODE}).to_string(),
        ))
        .await
        .unwrap();
    let error = next_json(&mut client).await;
    assert_eq!(error["code"], "attach_failed");
    assert!(!error.to_string().contains(CODE));
    assert!(!error.to_string().contains("private-platform-error"));
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    assert!(fixture.state.ends.borrow().is_empty());
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn disconnect_during_start_finishes_attach_and_shutdown_waits_for_cleanup() {
    let fixture = Fixture::new().await;
    fixture.state.start_gate.send_replace(false);
    let mut client = fixture.connect().await;
    client
        .send(Message::text(
            json!({"type":"subscribe","code":CODE}).to_string(),
        ))
        .await
        .unwrap();
    fixture.state.wait_starts(1).await;
    drop(client);
    fixture.state.start_gate.send_replace(true);
    let state = fixture.state.clone();
    fixture.stop().await;
    assert_eq!(*state.ends.borrow(), ["game-1"]);
}

#[tokio::test]
async fn shutdown_closes_active_clients_and_rejects_new_attaches() {
    let mut fixture = Fixture::new().await;
    let (mut client, _) = fixture.subscribe(CODE).await;
    fixture.shutdown.take().unwrap().send(()).unwrap();
    assert_eq!(next_json(&mut client).await, json!({"type":"closed"}));
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    drop(client);
    let manager = fixture.manager.clone();
    fixture.stop().await;
    assert!(matches!(
        manager.attach(CODE).await,
        Err(crate::session::SessionError::ShuttingDown)
    ));
}

#[tokio::test]
async fn shutdown_during_start_waits_for_end_and_notifies_the_client() {
    let mut fixture = Fixture::new().await;
    fixture.state.start_gate.send_replace(false);
    let mut client = fixture.connect().await;
    client
        .send(Message::text(
            json!({"type":"subscribe", "code":CODE}).to_string(),
        ))
        .await
        .unwrap();
    fixture.state.wait_starts(1).await;
    fixture.shutdown.take().unwrap().send(()).unwrap();
    timeout(TEST_TIMEOUT, async {
        while !fixture.manager.is_shutting_down() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fixture.state.ends.borrow().is_empty());
    fixture.state.start_gate.send_replace(true);
    assert_eq!(next_json(&mut client).await, json!({"type":"closed"}));
    assert!(matches!(next_frame(&mut client).await, Message::Close(_)));
    fixture.state.wait_ends(1).await;
    assert_eq!(*fixture.state.ends.borrow(), ["game-1"]);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn forward_reports_lagged_then_continues_with_retained_events() {
    let fixture = Fixture::new().await;
    let mut subscription = fixture.manager.attach(CODE).await.unwrap();
    let mut probe = fixture.manager.attach(CODE).await.unwrap();
    for index in 0..300 {
        fixture
            .state
            .emit(7, json!({"cmd":"INDEX", "data":index}))
            .await;
    }
    loop {
        match probe.recv().await {
            Ok(event) if matches!(&*event, LiveEvent::Unknown { data, .. } if *data == json!(299)) =>
            {
                break;
            }
            Ok(_) | Err(RecvError::Lagged { .. }) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    let (mut sink, mut messages) = recording_sink();
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let pongs = Pongs::new();
    let (shutdown, mut signal) = watch::channel(false);
    let task = tokio::spawn(async move {
        forward(&mut sink, &mut subscription, &mut peer, &pongs, &mut signal).await
    });
    let lagged = recorded_json(&mut messages).await;
    assert_eq!(lagged, json!({"type":"lagged", "skipped":44}));
    assert_eq!(recorded_json(&mut messages).await["data"], 44);
    shutdown.send_replace(true);
    // 信号在单次发送期间到达时会取消写入并直接断开；空闲时返回 End。
    assert!(matches!(task.await.unwrap(), Exit::End | Exit::Failed));
    drop(probe);
    fixture.stop().await;
}

#[tokio::test(start_paused = true)]
async fn first_subscription_times_out_and_stops_on_shutdown() {
    let (_first_tx, first) = oneshot::channel();
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let (stop_tx, mut stop) = watch::channel(false);
    let before = Instant::now();
    assert_eq!(
        receive_code(first, &mut peer, &mut stop).await,
        Err(Exit::Invalid("等待订阅消息超时"))
    );
    assert_eq!(Instant::now() - before, IO_TIMEOUT);
    let (_first_tx, first) = oneshot::channel();
    stop_tx.send_replace(true);
    assert_eq!(
        receive_code(first, &mut peer, &mut stop).await,
        Err(Exit::End)
    );
}

#[tokio::test(start_paused = true)]
async fn sends_timeout_without_retrying_a_cancelled_sink() {
    struct PendingSink;
    impl Sink<Message> for PendingSink {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _: Message) -> io::Result<()> {
            panic!("not ready")
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let (_stop_tx, mut stop) = watch::channel(false);
    let before = Instant::now();
    assert_eq!(
        send_frame(
            &mut PendingSink,
            Message::text("event"),
            &mut peer,
            &mut stop
        )
        .await,
        Err(Exit::Failed)
    );
    assert_eq!(Instant::now() - before, IO_TIMEOUT);
}

#[tokio::test]
async fn matching_pong_survives_unsolicited_pong_and_missing_next_pong_ends_it() {
    // 本测试只推进下游 Ping/Pong 的虚拟时钟；本地 HTTP I/O 不跟随虚拟时间，
    // 因此让项目心跳在观察窗口之外，避免其请求超时/恢复期限抢先结束订阅。
    let fixture = Fixture::with_app_heartbeat(Duration::from_secs(3600)).await;
    let mut subscription = fixture.manager.attach(CODE).await.unwrap();
    let (mut sink, mut messages) = recording_sink();
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let pongs = Arc::new(Pongs::new());
    let task_pongs = Arc::clone(&pongs);
    let (_stop_tx, mut stop) = watch::channel(false);
    tokio::time::pause();
    let task = tokio::spawn(async move {
        forward(
            &mut sink,
            &mut subscription,
            &mut peer,
            &task_pongs,
            &mut stop,
        )
        .await
    });
    let first_ping = messages.recv().await.unwrap();
    let Message::Ping(payload) = first_ping else {
        panic!("expected ping");
    };
    // 读端可能连续读完这些帧，写端只得到一次调度机会。
    pongs.acknowledge(b"unsolicited");
    pongs.acknowledge(&payload);
    pongs.acknowledge(b"unsolicited");
    let next_ping = messages.recv().await.unwrap();
    assert!(matches!(next_ping, Message::Ping(_)));
    // 上一轮的 Pong 也不能解除新一轮的超时。
    pongs.acknowledge(&payload);
    assert_eq!(task.await.unwrap(), Exit::Failed);
    tokio::time::resume();
    fixture.stop().await;
}

struct RecordingSink(mpsc::UnboundedSender<Message>);

fn recording_sink() -> (RecordingSink, mpsc::UnboundedReceiver<Message>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (RecordingSink(tx), rx)
}

impl Sink<Message> for RecordingSink {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> io::Result<()> {
        self.0
            .send(message)
            .map_err(|_| io::ErrorKind::BrokenPipe.into())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn recorded_json(messages: &mut mpsc::UnboundedReceiver<Message>) -> Value {
    let Message::Text(text) = timeout(TEST_TIMEOUT, messages.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected text");
    };
    serde_json::from_str(&text).unwrap()
}

async fn next_frame(client: &mut ClientSocket) -> Message {
    timeout(TEST_TIMEOUT, client.next())
        .await
        .expect("client read timed out")
        .unwrap()
        .unwrap()
}

async fn next_json(client: &mut ClientSocket) -> Value {
    loop {
        match next_frame(client).await {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("expected text, got {other:?}"),
        }
    }
}

struct Fixture {
    url: String,
    manager: Manager,
    state: Arc<PlatformState>,
    shutdown: Option<oneshot::Sender<()>>,
    server: JoinHandle<io::Result<()>>,
    platform_stop: watch::Sender<bool>,
    platform_tasks: Vec<JoinHandle<()>>,
}

struct PlatformState {
    ws_url: String,
    socket_gate: watch::Sender<bool>,
    ws_connects: watch::Sender<usize>,
    starts: watch::Sender<Vec<String>>,
    ends: watch::Sender<Vec<String>>,
    start_gate: watch::Sender<bool>,
    fail_start: AtomicBool,
    heartbeats: AtomicUsize,
    early_event: std::sync::Mutex<Option<Value>>,
    sockets: std::sync::Mutex<HashMap<i64, mpsc::UnboundedSender<UpstreamMessage>>>,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_app_heartbeat(Duration::from_secs(20)).await
    }

    async fn with_app_heartbeat(app_heartbeat: Duration) -> Self {
        Self::with_delivery(app_heartbeat, DeliveryPolicy::default()).await
    }

    async fn with_delivery(app_heartbeat: Duration, delivery_policy: DeliveryPolicy) -> Self {
        Self::with_budget(app_heartbeat, delivery_policy, ResourceBudget::for_tests()).await
    }

    async fn with_resources(policy: ResourcePolicy) -> Self {
        Self::with_budget(
            Duration::from_secs(20),
            DeliveryPolicy::default(),
            ResourceBudget::new(policy).unwrap(),
        )
        .await
    }

    async fn with_budget(
        app_heartbeat: Duration,
        delivery_policy: DeliveryPolicy,
        budget: ResourceBudget,
    ) -> Self {
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let official = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let state = Arc::new(PlatformState {
            ws_url: format!("ws://{}", official.local_addr().unwrap()),
            socket_gate: watch::channel(true).0,
            ws_connects: watch::channel(0).0,
            starts: watch::channel(Vec::new()).0,
            ends: watch::channel(Vec::new()).0,
            start_gate: watch::channel(true).0,
            fail_start: AtomicBool::new(false),
            heartbeats: AtomicUsize::new(0),
            early_event: Default::default(),
            sockets: Default::default(),
        });
        let manager = Manager::with_policies_and_delivery(
            Client::with_origin(
                "fake-key",
                "fake-secret",
                1,
                &format!("http://{}", api.local_addr().unwrap()),
            )
            .unwrap(),
            Duration::from_secs(20),
            app_heartbeat,
            ManagerOptions {
                idle_grace: Duration::from_secs(30),
            },
            HeartbeatPolicy::default(),
            UpstreamPolicy::default(),
            delivery_policy,
        )
        .unwrap()
        .with_resource_budget(budget)
        .unwrap();
        let url = format!("ws://{}/ws", downstream.local_addr().unwrap());
        let (shutdown, signal) = oneshot::channel();
        let server_manager = manager.clone();
        let server = tokio::spawn(serve(downstream, server_manager, async {
            let _ = signal.await;
        }));
        let (platform_stop, _) = watch::channel(false);
        let platform_tasks = vec![
            tokio::spawn(stub(api, state.clone(), platform_stop.subscribe(), false)),
            tokio::spawn(stub(
                official,
                state.clone(),
                platform_stop.subscribe(),
                true,
            )),
        ];
        Self {
            url,
            manager,
            state,
            shutdown: Some(shutdown),
            server,
            platform_stop,
            platform_tasks,
        }
    }

    async fn connect(&self) -> ClientSocket {
        timeout(TEST_TIMEOUT, connect_async(&self.url))
            .await
            .unwrap()
            .unwrap()
            .0
    }

    async fn subscribe(&self, code: &str) -> (ClientSocket, Value) {
        let mut client = self.connect().await;
        client
            .send(Message::text(
                json!({"type":"subscribe", "code":code}).to_string(),
            ))
            .await
            .unwrap();
        let ready = next_json(&mut client).await;
        assert_eq!(ready["type"], "ready");
        (client, ready)
    }

    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        timeout(TEST_TIMEOUT, self.server)
            .await
            .expect("server cleanup timed out")
            .unwrap()
            .unwrap();
        assert!(self.manager.is_shutting_down());
        self.platform_stop.send_replace(true);
        for task in self.platform_tasks {
            timeout(TEST_TIMEOUT, task).await.unwrap().unwrap();
        }
    }
}

impl PlatformState {
    async fn emit(&self, room: i64, value: Value) {
        let sender = self.sockets.lock().unwrap().get(&room).unwrap().clone();
        let (done, completed) = oneshot::channel();
        sender.send((value, done)).unwrap();
        timeout(TEST_TIMEOUT, completed).await.unwrap().unwrap();
    }

    async fn wait_starts(&self, count: usize) {
        let mut starts = self.starts.subscribe();
        timeout(
            TEST_TIMEOUT,
            starts.wait_for(|starts| starts.len() >= count),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn wait_ends(&self, count: usize) {
        let mut ends = self.ends.subscribe();
        timeout(TEST_TIMEOUT, ends.wait_for(|ends| ends.len() >= count))
            .await
            .unwrap()
            .unwrap();
    }
}

async fn stub(
    listener: TcpListener,
    state: Arc<PlatformState>,
    mut stop: watch::Receiver<bool>,
    official: bool,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = stopped(&mut stop) => break,
            incoming = listener.accept() => {
                let (stream, _) = incoming.unwrap();
                let state = state.clone();
                let mut stop = stop.clone();
                tasks.spawn(async move {
                    tokio::select! {
                        _ = stopped(&mut stop) => {}
                        _ = async {
                            if official { official_socket(stream, state).await; }
                            else { api_request(stream, state).await; }
                        } => {}
                    }
                });
            }
            result = tasks.join_next(), if !tasks.is_empty() => { result.unwrap().unwrap(); }
        }
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

async fn api_request(mut stream: TcpStream, state: Arc<PlatformState>) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let (header_end, length) = loop {
        let Ok(count) = stream.read(&mut buffer).await else {
            return;
        };
        if count == 0 {
            return;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            let length: usize = header
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
        assert!(bytes.len() < 16 * 1024);
    };
    while bytes.len() < header_end + length {
        let count = stream.read(&mut buffer).await.unwrap();
        if count == 0 {
            return;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let path = header.split_whitespace().nth(1).unwrap();
    let body: Value = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
    let response = match path {
        "/v2/app/start" => {
            let code = body["code"].as_str().unwrap();
            state.starts.send_modify(|starts| starts.push(code.into()));
            let game_id = format!("game-{}", state.starts.borrow().len());
            let mut gate = state.start_gate.subscribe();
            gate.wait_for(|open| *open).await.unwrap();
            if state.fail_start.load(Ordering::SeqCst) {
                json!({"code":7007, "message":format!("private-platform-error: {code}")})
            } else {
                let room_id = if code == "other-room" { 8 } else { 7 };
                json!({"code":0, "data": {
                    "game_info":{"game_id":game_id},
                    "websocket_info":{"auth_body":json!({"room_id":room_id}).to_string(), "wss_link":[state.ws_url]},
                    "anchor_info":{"room_id":room_id}
                }})
            }
        }
        "/v2/app/end" => {
            state.ends.send_modify(|ends| ends.push(body["game_id"].as_str().unwrap().into()));
            json!({"code":0})
        }
        "/v2/app/batchHeartbeat" => json!({"code":0,"data":{"failed_game_ids":[]}}),
        _ => panic!("unexpected API path {path}"),
    }.to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
        response.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn official_socket(stream: TcpStream, state: Arc<PlatformState>) {
    let mut socket = accept_async(stream).await.unwrap();
    let Message::Binary(auth) = socket.next().await.unwrap().unwrap() else {
        panic!("expected auth");
    };
    assert_eq!(u32::from_be_bytes(auth[8..12].try_into().unwrap()), 7);
    let auth: Value = serde_json::from_slice(&auth[16..]).unwrap();
    let room_id = auth["room_id"].as_i64().unwrap();
    state.ws_connects.send_modify(|count| *count += 1);
    state
        .socket_gate
        .subscribe()
        .wait_for(|open| *open)
        .await
        .unwrap();
    let (sender, mut events) = mpsc::unbounded_channel::<UpstreamMessage>();
    state.sockets.lock().unwrap().insert(room_id, sender);
    let mut reply = packet(8, br#"{"code":0}"#);
    if let Some(event) = state.early_event.lock().unwrap().take() {
        reply.extend(packet(5, event.to_string().as_bytes()));
    }
    if socket.send(Message::binary(reply)).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            event = events.recv() => {
                let Some((value, done)) = event else { break; };
                if value.is_null() {
                    let _ = socket.send(Message::Close(None)).await;
                    let _ = done.send(());
                    break;
                }
                if socket.send(Message::binary(packet(5, value.to_string().as_bytes()))).await.is_err() { break; }
                let _ = done.send(());
            }
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Binary(body))) => {
                    if u32::from_be_bytes(body[8..12].try_into().unwrap()) == 2 {
                        state.heartbeats.fetch_add(1, Ordering::SeqCst);
                        if socket.send(Message::binary(packet(3, b""))).await.is_err() { break; }
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            }
        }
    }
    let _ = socket.flush().await;
}

fn packet(operation: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
    bytes.extend_from_slice(&16_u16.to_be_bytes());
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&operation.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

impl Fixture {
    async fn subscribe_ack(&self, code: &str, cursor: Option<&Value>) -> (ClientSocket, Value) {
        let mut client = self.connect().await;
        let mut request = json!({"type":"subscribe","code":code,"ack":true});
        if let Some(cursor) = cursor {
            request["resume_after"] = cursor.clone();
        }
        client
            .send(Message::text(request.to_string()))
            .await
            .unwrap();
        let reply = next_json(&mut client).await;
        (client, reply)
    }

    async fn emit_logged(&self, data: Value) {
        let mut probe = self.manager.attach(CODE).await.unwrap();
        self.state.emit(7, json!({"cmd":"SEQ","data":data})).await;
        probe.recv().await.unwrap();
    }
}

async fn ack(client: &mut ClientSocket, ready: &Value, seq: u64) {
    client
        .send(Message::text(
            json!({"type":"ack","stream_id":ready["stream_id"],"seq":seq}).to_string(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn ack_window_pauses_events_but_keeps_ping_and_resumes_on_cumulative_ack() {
    let fixture = Fixture::with_delivery(
        Duration::from_secs(20),
        DeliveryPolicy {
            ack_window: 2,
            ..DeliveryPolicy::default()
        },
    )
    .await;
    let (mut client, ready) = fixture.subscribe_ack(CODE, None).await;
    assert_eq!(ready["ack_window"], 2);
    for seq in 1..=3 {
        fixture.emit_logged(json!(seq)).await;
    }
    for seq in 1..=2 {
        assert_eq!(next_json(&mut client).await["seq"], seq);
    }
    assert!(
        timeout(Duration::from_millis(30), client.next())
            .await
            .is_err()
    );
    client.send(Message::Ping(vec![9].into())).await.unwrap();
    assert_eq!(next_frame(&mut client).await, Message::Pong(vec![9].into()));
    ack(&mut client, &ready, 1).await;
    assert_eq!(next_json(&mut client).await["seq"], 3);
    ack(&mut client, &ready, 3).await;
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn disconnect_replays_unacked_and_offline_events_before_new_live_events() {
    let fixture = Fixture::new().await;
    let (mut first, ready) = fixture.subscribe_ack(CODE, None).await;
    fixture.emit_logged(json!(1)).await;
    assert_eq!(next_json(&mut first).await["seq"], 1);
    // 业务已完成但 ACK 丢失；恢复游标由客户端保存的位置决定。
    drop(first);
    fixture.emit_logged(json!(2)).await;
    fixture.emit_logged(json!(3)).await;
    let cursor = json!({"stream_id":ready["stream_id"],"seq":0});
    let (mut resumed, reply) = fixture.subscribe_ack(CODE, Some(&cursor)).await;
    assert_eq!(reply["resumed"], true);
    assert_eq!(reply["game_id"], ready["game_id"]);
    for seq in 1..=3 {
        assert_eq!(next_json(&mut resumed).await["seq"], seq);
    }
    fixture.emit_logged(json!(4)).await;
    assert_eq!(next_json(&mut resumed).await["seq"], 4);
    ack(&mut resumed, &reply, 4).await;
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop(resumed);
    fixture.stop().await;
}

#[tokio::test]
async fn ended_stream_replays_tail_without_starting_a_new_game_and_waits_for_ack() {
    let fixture = Fixture::new().await;
    let (mut first, ready) = fixture.subscribe_ack(CODE, None).await;
    fixture.emit_logged(json!(1)).await;
    assert_eq!(next_json(&mut first).await["seq"], 1);
    fixture
        .state
        .emit(
            7,
            json!({"cmd":"LIVE_OPEN_PLATFORM_INTERACTION_END","data":{"game_id":"game-1"}}),
        )
        .await;
    assert_eq!(next_json(&mut first).await["seq"], 2);
    fixture.state.wait_ends(1).await;
    assert!(
        timeout(Duration::from_millis(30), first.next())
            .await
            .is_err()
    );
    drop(first);
    let cursor = json!({"stream_id":ready["stream_id"],"seq":1});
    let (mut client, reply) = fixture.subscribe_ack(CODE, Some(&cursor)).await;
    assert_eq!(reply["type"], "ready");
    assert_eq!(
        next_json(&mut client).await["cmd"],
        "LIVE_OPEN_PLATFORM_INTERACTION_END"
    );
    ack(&mut client, &reply, 2).await;
    assert_eq!(next_json(&mut client).await, json!({"type":"closed"}));
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn expired_unacked_events_report_gap_while_window_is_full() {
    let fixture = Fixture::with_delivery(
        Duration::from_secs(20),
        DeliveryPolicy {
            ack_window: 1,
            max_bytes: 400,
            ..DeliveryPolicy::default()
        },
    )
    .await;
    let (mut client, _) = fixture.subscribe_ack(CODE, None).await;
    fixture.emit_logged(json!("x".repeat(100))).await;
    assert_eq!(next_json(&mut client).await["seq"], 1);
    fixture.emit_logged(json!("y".repeat(100))).await;
    let gap = next_json(&mut client).await;
    assert_eq!(gap["type"], "resume_unavailable");
    assert_eq!(gap["reason"], "cursor_expired");
    assert_eq!(gap["earliest_after"], 1);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn invalid_resume_never_starts_and_invalid_acks_close_the_connection() {
    let fixture = Fixture::new().await;
    let cursor = json!({"stream_id":"missing","seq":0});
    let (_, reply) = fixture.subscribe_ack(CODE, Some(&cursor)).await;
    assert_eq!(reply["type"], "resume_unavailable");
    assert!(fixture.state.starts.borrow().is_empty());
    for wrong_stream in [false, true] {
        let (mut client, ready) = fixture.subscribe_ack(CODE, None).await;
        let id = if wrong_stream {
            json!("other")
        } else {
            ready["stream_id"].clone()
        };
        client
            .send(Message::text(
                json!({"type":"ack","stream_id":id,"seq":1}).to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(next_json(&mut client).await["code"], "invalid_request");
    }
    fixture.stop().await;
}

#[test]
fn ack_validation_accepts_duplicate_and_rejects_regression_future_and_wrong_stream() {
    let acks = Acks::new();
    assert!(acks.acknowledge("one", 0).is_err());
    acks.start("one", 0);
    acks.reserve(3);
    assert!(acks.acknowledge("one", 2).is_ok());
    assert!(acks.acknowledge("one", 2).is_ok());
    for (id, seq) in [("other", 2), ("one", 1), ("one", 4)] {
        assert!(acks.acknowledge(id, seq).is_err());
    }
    assert_eq!(acks.snapshot().acked, 2);
    assert!(
        parse_subscribe(
            r#"{"type":"subscribe","code":"code","resume_after":{"stream_id":"one","seq":0}}"#
        )
        .is_err()
    );
}

#[tokio::test]
async fn requested_ack_window_is_bounded_by_service_limit() {
    let fixture = Fixture::with_delivery(
        Duration::from_secs(20),
        DeliveryPolicy {
            ack_window: 3,
            ..DeliveryPolicy::default()
        },
    )
    .await;
    for (requested, actual) in [(1, 1), (10, 3)] {
        let mut client = fixture.connect().await;
        client
            .send(Message::text(
                json!({"type":"subscribe","code":CODE,"ack":true,"ack_window":requested})
                    .to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(next_json(&mut client).await["ack_window"], actual);
        drop(client);
    }
    fixture.stop().await;
    for request in [
        r#"{"type":"subscribe","code":"code","ack":true,"ack_window":0}"#,
        r#"{"type":"subscribe","code":"code","ack_window":1}"#,
    ] {
        assert!(parse_subscribe(request).is_err());
    }
}

#[tokio::test]
async fn slow_ack_client_does_not_block_another_clients_progress() {
    let fixture = Fixture::with_delivery(
        Duration::from_secs(20),
        DeliveryPolicy {
            ack_window: 1,
            ..DeliveryPolicy::default()
        },
    )
    .await;
    let (mut slow, _) = fixture.subscribe_ack(CODE, None).await;
    let (mut fast, ready) = fixture.subscribe_ack(CODE, None).await;
    fixture.emit_logged(json!(1)).await;
    assert_eq!(next_json(&mut slow).await["seq"], 1);
    assert_eq!(next_json(&mut fast).await["seq"], 1);
    ack(&mut fast, &ready, 1).await;
    fixture.emit_logged(json!(2)).await;
    assert_eq!(next_json(&mut fast).await["seq"], 2);
    assert!(
        timeout(Duration::from_millis(30), slow.next())
            .await
            .is_err()
    );
    drop(slow);
    drop(fast);
    fixture.stop().await;
}

// 在 ACK 快照后的固定位置安排交错；仍运行真实的日志校验、淘汰和发送循环。
// sink 只在真正发送消息后触发退出，断言同时检查交付结果，避免只验证没有报错。
struct StopAfterSend {
    output: mpsc::UnboundedSender<Message>,
    stop: watch::Sender<bool>,
}

impl Sink<Message> for StopAfterSend {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> io::Result<()> {
        self.output.send(message).unwrap();
        self.stop.send_replace(true);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn delivery_log_fixture(
    policy: DeliveryPolicy,
) -> (Arc<crate::session::EventLog>, watch::Sender<UpstreamStatus>) {
    let log = crate::session::EventLog::new(policy);
    let (status, _) = watch::channel(UpstreamStatus::Connected);
    log.register("one", 7, "game-1", vec!["code".into()], status.subscribe())
        .unwrap();
    (log, status)
}

fn sized_event(n: usize) -> LiveEvent {
    LiveEvent::Unknown {
        cmd: "TEST".into(),
        data: json!({"n":n,"text":"x".repeat(100)}),
    }
}

fn stop_after_send_sink() -> (
    StopAfterSend,
    mpsc::UnboundedReceiver<Message>,
    watch::Receiver<bool>,
) {
    let (output, messages) = mpsc::unbounded_channel();
    let (stop, signal) = watch::channel(false);
    (StopAfterSend { output, stop }, messages, signal)
}

#[tokio::test]
async fn delivery_interleaving_ack_advances_before_eviction() {
    let (log, _status) = delivery_log_fixture(DeliveryPolicy {
        max_bytes: 400,
        ..DeliveryPolicy::default()
    });
    log.append("one", &sized_event(1)).unwrap();
    let mut subscription = log.test_reader("one", "code", Some(0), None).unwrap();
    assert_eq!(subscription.recv().await.unwrap().unwrap().seq, 1);
    let acks = Acks::new();
    acks.start("one", 0);
    acks.reserve(1);
    let once = AtomicBool::new(false);
    let hook_log = Arc::clone(&log);
    *acks.after_snapshot.lock().unwrap() = Some(Box::new(move |acks| {
        if !once.swap(true, Ordering::SeqCst) {
            acks.acknowledge("one", 1).unwrap();
            hook_log.append("one", &sized_event(2)).unwrap();
        }
    }));
    let (mut sink, mut messages, mut stop) = stop_after_send_sink();
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let result = timeout(
        TEST_TIMEOUT,
        forward_delivery(
            &mut sink,
            &mut subscription,
            &mut peer,
            &Pongs::new(),
            &acks,
            1,
            &mut stop,
        ),
    )
    .await
    .expect("发送循环应完成");
    assert_eq!(acks.snapshot().acked, 1);
    assert!(
        subscription.validate_after(1).is_ok(),
        "当前 ACK 可继续读取事件 2"
    );
    assert_eq!(
        result,
        Exit::End,
        "应交付事件 2 后退出；不能使用过期的 ACK=0 快照报错"
    );
    let Message::Text(text) = messages.try_recv().unwrap() else {
        panic!("应发送事件 2");
    };
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["seq"], 2);
    assert_eq!(acks.snapshot().sent, 2);
}

#[tokio::test]
async fn delivery_interleaving_finished_metadata_eviction() {
    // 同时覆盖快照已包含最终 ACK，以及最终 ACK 在快照后才到达的情况。
    for late_ack in [false, true] {
        let (log, status) = delivery_log_fixture(DeliveryPolicy {
            max_streams: 1,
            ..DeliveryPolicy::default()
        });
        log.append("one", &sized_event(1)).unwrap();
        log.finish("one");
        let mut subscription = log.test_reader("one", "code", Some(0), None).unwrap();
        subscription.recv().await.unwrap().unwrap();
        let acks = Acks::new();
        acks.start("one", 0);
        acks.reserve(1);
        let visits = AtomicUsize::new(0);
        let hook_log = Arc::clone(&log);
        *acks.after_snapshot.lock().unwrap() = Some(Box::new(move |acks| {
            let turn = visits.fetch_add(1, Ordering::SeqCst);
            // 早到 ACK：满窗口唤醒、读到 EOF，再于下一快照后淘汰流。
            // 晚到 ACK：先读到 EOF，于结束检查的快照后同时推进 ACK 并淘汰。
            if (!late_ack && turn == 0) || (late_ack && turn == 1) {
                acks.acknowledge("one", 1).unwrap();
            }
            if (!late_ack && turn == 2) || (late_ack && turn == 1) {
                hook_log
                    .register("two", 8, "game-2", vec!["other".into()], status.subscribe())
                    .unwrap();
            }
        }));
        let (mut sink, mut messages, mut stop) = stop_after_send_sink();
        let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
        let result = timeout(
            TEST_TIMEOUT,
            forward_delivery(
                &mut sink,
                &mut subscription,
                &mut peer,
                &Pongs::new(),
                &acks,
                if late_ack { 2 } else { 1 },
                &mut stop,
            ),
        )
        .await
        .expect("结束流应完成");
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(
            log.test_reader("one", "code", Some(1), None)
                .err()
                .unwrap()
                .reason,
            "stream_unavailable",
            "必须实际发生元数据淘汰"
        );
        assert_eq!(
            result,
            Exit::End,
            "全部事件已交付并确认，淘汰历史不能把正常结束变成续传失败"
        );
    }
}

#[tokio::test]
async fn delivery_interleaving_busy_room_notifications() {
    let (log, status) = delivery_log_fixture(DeliveryPolicy::default());
    log.register(
        "busy",
        8,
        "game-2",
        vec!["other".into()],
        status.subscribe(),
    )
    .unwrap();
    log.append("one", &sized_event(1)).unwrap();
    let mut subscription = log.test_reader("one", "code", Some(0), None).unwrap();
    let acks = Acks::new();
    acks.start("one", 0);
    let (mut sink, mut messages, mut stop) = stop_after_send_sink();
    let stopper = sink.stop.clone();
    let visits = Arc::new(AtomicUsize::new(0));
    let hook_visits = Arc::clone(&visits);
    *acks.after_snapshot.lock().unwrap() = Some(Box::new(move |_| {
        let turn = hook_visits.fetch_add(1, Ordering::SeqCst);
        if turn == 1000 {
            stopper.send_replace(true);
        }
        log.append("busy", &sized_event(turn)).unwrap();
    }));
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let result = timeout(
        TEST_TIMEOUT,
        forward_delivery(
            &mut sink,
            &mut subscription,
            &mut peer,
            &Pongs::new(),
            &acks,
            1,
            &mut stop,
        ),
    )
    .await
    .expect("就绪事件不能被通知饿死");
    assert!(
        visits.load(Ordering::SeqCst) > 0,
        "必须实际插入其他房间通知"
    );
    assert_eq!(result, Exit::End);
    let message = messages
        .try_recv()
        .expect("窗口空闲、事件已就绪，连续 1000 次其他房间通知不能阻止一次发送");
    let Message::Text(text) = message else {
        panic!("应发送事件文本");
    };
    assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["seq"], 1);
}

#[tokio::test]
async fn delivery_interleaving_ack_advance_does_not_hide_a_real_gap() {
    let (log, _status) = delivery_log_fixture(DeliveryPolicy {
        max_bytes: 400,
        ..DeliveryPolicy::default()
    });
    log.append("one", &sized_event(1)).unwrap();
    let mut subscription = log.test_reader("one", "code", Some(0), None).unwrap();
    subscription.recv().await.unwrap().unwrap();
    let acks = Acks::new();
    acks.start("one", 0);
    acks.reserve(1);
    let once = AtomicBool::new(false);
    *acks.after_snapshot.lock().unwrap() = Some(Box::new(move |acks| {
        if !once.swap(true, Ordering::SeqCst) {
            acks.acknowledge("one", 1).unwrap();
            log.append("one", &sized_event(2)).unwrap();
            log.append("one", &sized_event(3)).unwrap();
        }
    }));
    let (mut sink, mut messages, mut stop) = stop_after_send_sink();
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let result = timeout(
        TEST_TIMEOUT,
        forward_delivery(
            &mut sink,
            &mut subscription,
            &mut peer,
            &Pongs::new(),
            &acks,
            1,
            &mut stop,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        Exit::Resume(ResumeUnavailable {
            reason: "cursor_expired",
            earliest_after: Some(2),
            latest_seq: Some(3)
        })
    );
    assert!(
        matches!(messages.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "不能跳过已丢失的事件 2 而交付事件 3"
    );
}

#[tokio::test]
async fn delivery_interleaving_busy_room_notifications_keep_status_when_window_is_full() {
    let (log, status) = delivery_log_fixture(DeliveryPolicy::default());
    log.register(
        "busy",
        8,
        "game-2",
        vec!["other".into()],
        status.subscribe(),
    )
    .unwrap();
    log.append("one", &sized_event(1)).unwrap();
    let mut subscription = log.test_reader("one", "code", Some(0), None).unwrap();
    subscription.recv().await.unwrap().unwrap();
    let acks = Acks::new();
    acks.start("one", 0);
    acks.reserve(1);
    let (mut sink, mut messages, mut stop) = stop_after_send_sink();
    let stopper = sink.stop.clone();
    let visits = AtomicUsize::new(0);
    *acks.after_snapshot.lock().unwrap() = Some(Box::new(move |_| {
        let turn = visits.fetch_add(1, Ordering::SeqCst);
        if turn == 0 {
            status.send_replace(UpstreamStatus::Reconnecting);
        }
        if turn == 1000 {
            stopper.send_replace(true);
        }
        log.append("busy", &sized_event(turn)).unwrap();
    }));
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let result = timeout(
        TEST_TIMEOUT,
        forward_delivery(
            &mut sink,
            &mut subscription,
            &mut peer,
            &Pongs::new(),
            &acks,
            1,
            &mut stop,
        ),
    )
    .await
    .unwrap();
    assert_eq!(result, Exit::End);
    let Message::Text(text) = messages.try_recv().expect("满窗口时也必须交付上游状态")
    else {
        panic!("应发送状态文本");
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({"type":"status","state":"reconnecting"})
    );
    assert_eq!(acks.snapshot().sent, 1, "状态不能绕过窗口发送业务事件");
}

async fn quota_occupied(fixture: &Fixture, index: usize, n: usize) {
    timeout(TEST_TIMEOUT, async {
        while fixture.manager.resource_budget().snapshot().occupied[index] != n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("资源占用未达到预期");
}

#[tokio::test]
async fn resource_connection_and_handshake_limits_refuse_without_spawning_waiters() {
    for handshake in [false, true] {
        let fixture = Fixture::with_resources(ResourcePolicy {
            max_connections: if handshake { 2 } else { 1 },
            max_handshakes: 1,
            ..Default::default()
        })
        .await;
        if handshake {
            let raw = TcpStream::connect(
                fixture
                    .url
                    .trim_start_matches("ws://")
                    .trim_end_matches("/ws"),
            )
            .await
            .unwrap();
            quota_occupied(&fixture, 1, 1).await;
            assert!(
                timeout(TEST_TIMEOUT, connect_async(&fixture.url))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(
                fixture.manager.resource_budget().snapshot().rejected[Resource::Handshake as usize],
                1
            );
            drop(raw);
        } else {
            let client = fixture.connect().await;
            quota_occupied(&fixture, 1, 0).await;
            assert_eq!(fixture.manager.resource_budget().snapshot().occupied[0], 1);
            assert!(
                timeout(TEST_TIMEOUT, connect_async(&fixture.url))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert_eq!(
                fixture.manager.resource_budget().snapshot().rejected
                    [Resource::Connection as usize],
                1
            );
            drop(client);
        }
        quota_occupied(&fixture, 0, 0).await;
        let client = fixture.connect().await;
        drop(client);
        assert!(fixture.state.starts.borrow().is_empty());
        fixture.stop().await;
    }
}

#[tokio::test]
async fn resource_message_limit_counts_duplicate_acks_before_json_and_exempts_control_frames() {
    let fixture = Fixture::with_resources(ResourcePolicy {
        client_message: RatePolicy {
            per_sec: 1,
            burst: 3,
        },
        ..Default::default()
    })
    .await;
    fixture.manager.resource_budget().freeze_message_clock();
    let (mut client, ready) = fixture.subscribe_ack(CODE, None).await;
    for _ in 0..4 {
        client.send(Message::Ping(vec![1, 2].into())).await.unwrap();
        assert_eq!(
            next_frame(&mut client).await,
            Message::Pong(vec![1, 2].into())
        );
    }
    ack(&mut client, &ready, 0).await;
    ack(&mut client, &ready, 0).await;
    // 第四条业务消息在解析之前被限流，不能变成 invalid_request。
    client.send(Message::text("not-json")).await.unwrap();
    let reply = next_json(&mut client).await;
    assert_eq!(reply["code"], "rate_limited");
    assert_eq!(reply["retry_after_ms"], 1000);
    drop(client);
    quota_occupied(&fixture, 0, 0).await;
    fixture.stop().await;
}

#[tokio::test]
async fn resource_ws_pending_and_room_refusals_preserve_the_active_stream() {
    let fixture = Fixture::with_resources(ResourcePolicy {
        max_pending_attaches: 1,
        max_starting_sessions: 1,
        max_subscribers_per_room: 1,
        ..Default::default()
    })
    .await;
    fixture.state.start_gate.send_replace(false);
    let mut first = fixture.connect().await;
    first
        .send(Message::text(
            json!({"type":"subscribe","code":CODE,"ack":true}).to_string(),
        ))
        .await
        .unwrap();
    quota_occupied(&fixture, 2, 1).await;
    let (rejected, reply) = fixture.subscribe_ack(CODE, None).await;
    assert_eq!(reply["code"], "server_busy");
    assert!(reply["retry_after_ms"].is_number());
    drop(rejected);
    drop(first);
    quota_occupied(&fixture, 2, 0).await;
    assert_eq!(
        fixture.manager.resource_budget().snapshot().occupied[3..],
        [1, 1]
    );
    // 等待者断开不取消实际 start，开场结果仍可被新的等待者复用。
    fixture.state.start_gate.send_replace(true);
    let (client, ready) = fixture.subscribe_ack(CODE, None).await;
    assert_eq!(ready["type"], "ready");
    let cursor = json!({"stream_id":ready["stream_id"],"seq":ready["seq"]});
    let (other, reply) = fixture.subscribe_ack(CODE, Some(&cursor)).await;
    assert_eq!(reply["code"], "subscription_limit");
    assert_eq!(fixture.state.starts.borrow().len(), 1);
    drop((client, other));
    fixture.stop().await;
}
