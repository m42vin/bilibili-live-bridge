//! 有界本地 HTTP / WebSocket 平台桩，故障仅作用于明确选中的操作。

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::*;
use bridge_core::open_live::ws::packet::{self, Operation};

struct Game {
    active: bool,
    heartbeat: Instant,
    ends: usize,
}
struct State {
    origin: Instant,
    games: Mutex<HashMap<String, Game>>,
    sockets: Mutex<HashMap<String, (usize, mpsc::Sender<Command>)>>,
    next_socket: AtomicUsize,
    starts: AtomicUsize,
    emitted: AtomicUsize,
    timeout_start: AtomicBool,
    limited_heartbeat: AtomicBool,
    hold_end: AtomicBool,
    silent: AtomicBool,
    ignored_heartbeats: AtomicUsize,
    auth_gate: watch::Sender<bool>,
    ws_url: String,
}

enum Command {
    Frame(Vec<u8>, oneshot::Sender<()>),
    Drop,
}

/// 生命周期由父进程拥有的模拟平台，丢弃时中止全部本地桩任务。
pub(super) struct Platform {
    origin: String,
    state: Arc<State>,
    tasks: JoinSet<()>,
}

impl Platform {
    /// 在回环临时端口启动 HTTP、上游与远端心跳过期模型。
    pub(super) async fn start() -> Self {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", http.local_addr().unwrap());
        let state = Arc::new(State {
            origin: Instant::now(),
            games: Mutex::new(HashMap::new()),
            sockets: Mutex::new(HashMap::new()),
            next_socket: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            emitted: AtomicUsize::new(0),
            timeout_start: AtomicBool::new(false),
            limited_heartbeat: AtomicBool::new(false),
            hold_end: AtomicBool::new(false),
            silent: AtomicBool::new(false),
            ignored_heartbeats: AtomicUsize::new(0),
            auth_gate: watch::channel(true).0,
            ws_url: format!("ws://{}", ws.local_addr().unwrap()),
        });
        let mut tasks = JoinSet::new();
        for (listener, websocket) in [(http, false), (ws, true)] {
            let state = state.clone();
            tasks.spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        incoming = listener.accept() => {
                            let (stream, _) = incoming.unwrap();
                            let state = state.clone();
                            assert!(connections.len() < 256, "模拟平台并发上限");
                            connections.spawn(async move {
                                if websocket { let _ = socket(stream, state).await; }
                                else { let _ = api(stream, state).await; }
                            });
                        }
                        result = connections.join_next(), if !connections.is_empty() => { result.unwrap().unwrap(); }
                    }
                }
            });
        }
        let expiry = state.clone();
        tasks.spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_millis(50));
            loop {
                timer.tick().await;
                for game in expiry.games.lock().unwrap().values_mut() {
                    if game.heartbeat.elapsed() >= Duration::from_secs(5) {
                        game.active = false;
                    }
                }
            }
        });
        Self {
            origin,
            state,
            tasks,
        }
    }

    /// 只读本地源站地址。
    pub(super) fn origin(&self) -> &str {
        &self.origin
    }
    /// 提供延迟测量共用的父进程单调时间原点。
    pub(super) fn clock(&self) -> Instant {
        self.state.origin
    }
    /// 返回当前已发出的数据索引。
    pub(super) fn emitted(&self) -> usize {
        self.state.emitted.load(Ordering::SeqCst)
    }
    /// 返回实际 start 尝试数量。
    pub(super) fn starts(&self) -> usize {
        self.state.starts.load(Ordering::SeqCst)
    }
    /// 返回场次 end 调用次数。
    pub(super) fn ends(&self, game: &str) -> usize {
        self.state
            .games
            .lock()
            .unwrap()
            .get(game)
            .map_or(0, |game| game.ends)
    }
    /// 当前模拟平台未结束或过期的场次数。
    pub(super) fn active(&self) -> usize {
        self.state
            .games
            .lock()
            .unwrap()
            .values()
            .filter(|game| game.active)
            .count()
    }
    /// 下一次 start 等待客户端超时断开，不生成场次。
    pub(super) fn timeout_start(&self) {
        self.state.timeout_start.store(true, Ordering::SeqCst);
    }
    /// 控制项目心跳返回 HTTP 429。
    pub(super) fn limit_heartbeat(&self, value: bool) {
        self.state.limited_heartbeat.store(value, Ordering::SeqCst);
    }
    /// 控制 end 在记录请求后等待客户端断开。
    pub(super) fn hold_end(&self, value: bool) {
        self.state.hold_end.store(value, Ordering::SeqCst);
    }
    /// 控制上游忽略协议心跳回复。
    pub(super) fn silence(&self, value: bool) {
        self.state.silent.store(value, Ordering::SeqCst);
    }
    /// 控制新上游鉴权回复，固定恢复中的可观测窗口。
    pub(super) fn auth_gate(&self, open: bool) {
        self.state.auth_gate.send_replace(open);
    }
    /// 确认静默故障确实处理过上游心跳。
    pub(super) fn ignored_heartbeats(&self) -> usize {
        self.state.ignored_heartbeats.load(Ordering::SeqCst)
    }

    /// 确认桩自身未 panic，防止把失效的故障注入当成通过。
    pub(super) fn check_tasks(&mut self) {
        if let Some(result) = self.tasks.try_join_next() {
            result.unwrap();
            panic!("平台监听任务意外结束");
        }
    }

    /// 断开指定场次的当前上游 TCP。
    pub(super) async fn disconnect(&self, game: &str) {
        let sender = self
            .state
            .sockets
            .lock()
            .unwrap()
            .get(game)
            .unwrap()
            .1
            .clone();
        sender.send(Command::Drop).await.unwrap();
    }

    /// 按目标速率批量发送事件，等待实际写入，父进程不积累无界消息。
    pub(super) async fn emit(
        &self,
        game: &str,
        count: usize,
        rate: usize,
        payload: usize,
    ) -> Result<()> {
        let started = Instant::now();
        let sender = self
            .state
            .sockets
            .lock()
            .unwrap()
            .get(game)
            .ok_or("上游尚未就绪")?
            .1
            .clone();
        let text = "x".repeat(payload);
        let mut sent = 0;
        while sent < count {
            tokio::time::sleep_until(started + Duration::from_secs_f64(sent as f64 / rate as f64))
                .await;
            let mut frame = Vec::new();
            for _ in 0..32.min(count - sent) {
                let index = self.state.emitted.fetch_add(1, Ordering::SeqCst) + 1;
                let value = json!({"cmd":"VALIDATION_EVENT","data":{"index":index,"sent_us":self.state.origin.elapsed().as_micros() as u64,"payload":text}});
                frame.extend(packet::encode(
                    Operation::Notification,
                    value.to_string().as_bytes(),
                )?);
                sent += 1;
            }
            let (done, flushed) = oneshot::channel();
            sender.send(Command::Frame(frame, done)).await?;
            timeout(Duration::from_secs(10), flushed).await??;
        }
        Ok(())
    }
}

async fn api(mut stream: TcpStream, state: Arc<State>) -> Result<()> {
    let (path, body) = timeout(Duration::from_secs(3), read_http(&mut stream)).await??;
    let mut status = "200 OK";
    let response = match path.as_str() {
        "/v2/app/start" => {
            let number = state.starts.fetch_add(1, Ordering::SeqCst) + 1;
            if state.timeout_start.swap(false, Ordering::SeqCst) {
                let mut buffer = [0; 1];
                let _ = timeout(Duration::from_secs(15), stream.read(&mut buffer)).await;
                return Ok(());
            }
            let game = format!("validation-{number}");
            state.games.lock().unwrap().insert(
                game.clone(),
                Game {
                    active: true,
                    heartbeat: Instant::now(),
                    ends: 0,
                },
            );
            json!({"code":0,"data":{"game_info":{"game_id":game},"websocket_info":{"auth_body":json!({"game":game}).to_string(),"wss_link":[state.ws_url]},"anchor_info":{"room_id":1}}})
        }
        "/v2/app/end" => {
            let game = body["game_id"].as_str().unwrap();
            state.games.lock().unwrap().get_mut(game).unwrap().ends += 1;
            if state.hold_end.load(Ordering::SeqCst) {
                let mut buffer = [0; 1];
                let _ = timeout(Duration::from_secs(5), stream.read(&mut buffer)).await;
                return Ok(());
            }
            state.games.lock().unwrap().get_mut(game).unwrap().active = false;
            json!({"code":0})
        }
        "/v2/app/batchHeartbeat" => {
            if state.limited_heartbeat.load(Ordering::SeqCst) {
                status = "429 Too Many Requests";
            } else {
                for id in body["game_ids"].as_array().unwrap() {
                    if let Some(game) = state.games.lock().unwrap().get_mut(id.as_str().unwrap()) {
                        game.heartbeat = Instant::now();
                    }
                }
            }
            json!({"code":0,"data":{"failed_game_ids":[]}})
        }
        _ => return Err("未知模拟平台路径".into()),
    };
    let body = response.to_string();
    stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    Ok(())
}

async fn read_http(stream: &mut TcpStream) -> Result<(String, Value)> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        let count = stream.read(&mut buffer).await?;
        check(
            count > 0 && bytes.len() + count <= 32768,
            "无效平台请求大小",
        )?;
        bytes.extend_from_slice(&buffer[..count]);
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);
        if let httparse::Status::Complete(offset) = request.parse(&bytes)? {
            let length: usize = request
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("content-length"))
                .ok_or("缺少请求长度")?
                .value
                .iter()
                .map(|byte| *byte as char)
                .collect::<String>()
                .parse()?;
            if bytes.len() >= offset + length {
                return Ok((
                    request.path.unwrap().to_owned(),
                    serde_json::from_slice(&bytes[offset..offset + length])?,
                ));
            }
        }
    }
}

async fn socket(stream: TcpStream, state: Arc<State>) -> Result<()> {
    let mut socket = accept_async(stream).await?;
    let Message::Binary(auth) = timeout(Duration::from_secs(3), socket.next())
        .await?
        .ok_or("鉴权断开")??
    else {
        return Err("需要二进制鉴权".into());
    };
    let game = serde_json::from_slice::<Value>(&auth[16..])?["game"]
        .as_str()
        .ok_or("缺少场次")?
        .to_owned();
    let id = state.next_socket.fetch_add(1, Ordering::SeqCst);
    let (sender, mut commands) = mpsc::channel(64);
    state
        .sockets
        .lock()
        .unwrap()
        .insert(game.clone(), (id, sender));
    state.auth_gate.subscribe().wait_for(|open| *open).await?;
    socket
        .send(Message::binary(packet::encode(
            Operation::AuthReply,
            br#"{"code":0}"#,
        )?))
        .await?;
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Frame(bytes, done)) => { socket.send(Message::binary(bytes)).await?; let _ = done.send(()); }
                _ => break,
            },
            message = socket.next() => match message {
                Some(Ok(Message::Binary(bytes))) if bytes.len() >= 16 => {
                    if state.silent.load(Ordering::SeqCst) { state.ignored_heartbeats.fetch_add(1, Ordering::SeqCst); }
                    else { socket.send(Message::binary(packet::encode(Operation::HeartbeatReply, b"")?)).await?; }
                }
                Some(Ok(Message::Ping(_))) => { socket.flush().await?; }
                Some(Ok(Message::Close(_))) => { let _ = socket.flush().await; break; }
                Some(Ok(_)) => {},
                _ => break,
            }
        }
    }
    let mut sockets = state.sockets.lock().unwrap();
    if sockets
        .get(&game)
        .is_some_and(|(current, _)| *current == id)
    {
        sockets.remove(&game);
    }
    Ok(())
}
