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
use crate::open_live::ws::{Danmaku, LiveEvent};
use crate::session::ManagerOptions;

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type UpstreamMessage = (Value, oneshot::Sender<()>);
const CODE: &str = "private-identity-code";
const TEST_TIMEOUT: Duration = Duration::from_secs(3);

#[test]
fn protocol_preserves_known_and_unknown_data() {
    let event = LiveEvent::Danmaku(Danmaku {
        msg: "hello".to_owned(),
        room_id: 7,
        ..Danmaku::default()
    });
    let value = serde_json::to_value(ServerMessage::event(7, &event)).unwrap();
    assert_eq!(value["type"], "event");
    assert_eq!(value["cmd"], "LIVE_OPEN_PLATFORM_DM");
    assert_eq!(value["room_id"], 7);
    assert_eq!(value["data"]["msg"], "hello");
    assert_eq!(value["data"]["uid"], 0);
    for data in [json!({"future_field": [1, true, null]}), Value::Null] {
        let event = LiveEvent::Unknown {
            cmd: "FUTURE_EVENT".into(),
            data: data.clone(),
        };
        assert_eq!(
            serde_json::to_value(ServerMessage::event(7, &event)).unwrap(),
            json!({"type":"event", "room_id":7, "cmd":"FUTURE_EVENT", "data":data})
        );
    }
    let events = [
        LiveEvent::DanmakuMirror(Default::default()),
        LiveEvent::Gift(Default::default()),
        LiveEvent::SuperChat(Default::default()),
        LiveEvent::SuperChatDelete(Default::default()),
        LiveEvent::Guard(Default::default()),
        LiveEvent::Like(Default::default()),
        LiveEvent::RoomEnter(Default::default()),
        LiveEvent::LiveStart(Default::default()),
        LiveEvent::LiveEnd(Default::default()),
        LiveEvent::InteractionEnd(Default::default()),
    ];
    for event in events {
        let value = serde_json::to_value(ServerMessage::event(7, &event)).unwrap();
        assert_eq!(value["cmd"], event.cmd());
        assert!(value["data"].is_object());
    }
    assert_eq!(
        serde_json::to_value(ServerMessage::Closed).unwrap(),
        json!({"type":"closed"})
    );
    assert_eq!(
        subscription_code(r#"{"type":"subscribe","code":"  abc  "}"#),
        Ok("abc".into())
    );
    for invalid in [
        "no-json",
        "{}",
        r#"{"type":"subscribe","code":" "}"#,
        r#"{"type":"subscribe","code":1}"#,
        r#"{"type":"unsubscribe"}"#,
    ] {
        assert!(subscription_code(invalid).is_err());
    }
}

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
    let fixture = Fixture::new().await;
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
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let official = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let state = Arc::new(PlatformState {
            ws_url: format!("ws://{}", official.local_addr().unwrap()),
            starts: watch::channel(Vec::new()).0,
            ends: watch::channel(Vec::new()).0,
            start_gate: watch::channel(true).0,
            fail_start: AtomicBool::new(false),
            heartbeats: AtomicUsize::new(0),
            early_event: Default::default(),
            sockets: Default::default(),
        });
        let manager = Manager::with_options(
            Client::with_origin(
                "fake-key",
                "fake-secret",
                1,
                &format!("http://{}", api.local_addr().unwrap()),
            )
            .unwrap(),
            Duration::from_secs(20),
            Duration::from_secs(20),
            ManagerOptions {
                idle_grace: Duration::from_secs(30),
            },
        )
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
                if socket.send(Message::binary(packet(5, value.to_string().as_bytes()))).await.is_err() { break; }
                let _ = done.send(());
            }
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Binary(body))) => {
                    if u32::from_be_bytes(body[8..12].try_into().unwrap()) == 2 {
                        state.heartbeats.fetch_add(1, Ordering::SeqCst);
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
