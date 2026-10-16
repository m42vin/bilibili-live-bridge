//! 单房间的场次所有权、下游连接与内存续传。
//! 持久存储只保存清理责任，对象重建不恢复事件日志。

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    time::Duration,
};

use bridge_core::{
    delivery::EventCursor,
    downstream::{
        admission::AdmissionResponse,
        protocol::{ClientMessage, ServerMessage},
    },
    open_live::{
        model::start_result_from_value,
        ws::packet::{self, Operation},
    },
};
use futures_util::lock::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use worker::*;

use super::{api, error, failure, json_body, now, random_id, unavailable};
use crate::delivery::{Log, Window};

#[path = "upstream.rs"]
mod upstream;

#[derive(Clone, Serialize, Deserialize)]
struct Session {
    candidate: Value,
    game: String,
    room: i64,
    stream: String,
}

#[derive(Deserialize)]
struct TicketRequest {
    code_hash: String,
    ack: bool,
    ack_window: Option<u64>,
    resume_after: Option<EventCursor>,
    origin: String,
}

struct Ticket {
    expires: u64,
    ack: bool,
    window: u64,
    after: Option<EventCursor>,
}

struct Peer {
    ws: WebSocket,
    ack: bool,
    window: Window,
    ready: bool,
    ping: Option<(String, u64)>,
}

struct Live {
    initialized: bool,
    session: Option<Session>,
    codes: HashSet<String>,
    tickets: HashMap<String, Ticket>,
    peers: Vec<Peer>,
    log: Log,
    socket: Option<WebSocket>,
    running: bool,
    authenticated: bool,
    idle_at: u64,
    last_activity: u64,
    heartbeat_at: u64,
    ending: bool,
    cleaned: bool,
}

impl Live {
    fn new() -> Self {
        Self {
            initialized: false,
            session: None,
            codes: HashSet::new(),
            tickets: HashMap::new(),
            peers: Vec::new(),
            log: Log::new(),
            socket: None,
            running: false,
            authenticated: false,
            idle_at: 0,
            last_activity: 0,
            heartbeat_at: 0,
            ending: false,
            cleaned: false,
        }
    }
}

struct Inner {
    state: State,
    env: Env,
    // 串行化场次状态与异步存储、网络清理；live 的短期借用不跨 await。
    gate: Mutex<()>,
    live: RefCell<Live>,
}

/// 按房间共享一场互动，管理下游连接并用 alarm 维护心跳与清理。
#[durable_object]
pub struct Room {
    inner: Rc<Inner>,
}

impl DurableObject for Room {
    fn new(state: State, env: Env) -> Self {
        Self {
            inner: Rc::new(Inner {
                state,
                env,
                gate: Mutex::new(()),
                live: RefCell::new(Live::new()),
            }),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let _guard = self.inner.gate.lock().await;
        self.inner.initialize().await?;
        let expired = {
            let live = self.inner.live.borrow();
            live.idle_at != 0 && now() >= live.idle_at
        };
        if expired {
            self.inner.finish().await?;
            *self.inner.live.borrow_mut() = Live {
                initialized: true,
                ..Live::new()
            };
        }

        let path = req.path();
        match (req.method(), path.as_str()) {
            (Method::Post, "/prepare") => {
                let body: Value = json_body(&mut req).await?;
                let started = start_result_from_value(body["candidate"].clone())
                    .map_err(|_| error("invalid candidate"))?;
                let existing = self.inner.live.borrow().session.clone();
                if let Some(existing) = existing {
                    if self.inner.live.borrow().ending {
                        return failure(503, "server_busy");
                    }
                    self.inner.live.borrow_mut().codes.insert(
                        body["code_hash"]
                            .as_str()
                            .ok_or_else(|| error("missing hash"))?
                            .into(),
                    );
                    return Response::from_json(
                        &json!({"owned":existing.game == started.game_id()}),
                    );
                }

                let session = Session {
                    candidate: body["candidate"].clone(),
                    game: started.game_id().into(),
                    room: started.anchor().room_id,
                    stream: random_id(),
                };
                self.inner
                    .state
                    .storage()
                    .set_alarm(Duration::from_secs(10))
                    .await?;
                self.inner.state.storage().put("session", &session).await?;
                let mut live = self.inner.live.borrow_mut();
                live.session = Some(session);
                live.idle_at = now() + 30_000;
                live.codes.insert(
                    body["code_hash"]
                        .as_str()
                        .ok_or_else(|| error("missing hash"))?
                        .into(),
                );
                Response::from_json(&json!({"owned":true}))
            }
            (Method::Post, "/owns") => {
                let body: Value = json_body(&mut req).await?;
                Response::from_json(
                    &json!({"owned":self.inner.live.borrow().session.as_ref().is_some_and(|s| s.game == body["game"])}),
                )
            }
            (Method::Post, "/ticket") => {
                let input: TicketRequest = json_body(&mut req).await?;
                self.inner.ticket(input)
            }
            (Method::Get, _) if path.starts_with("/ws/") => self.upgrade(&req),
            _ => Response::error("not found", 404),
        }
    }

    async fn alarm(&self) -> Result<Response> {
        let _guard = self.inner.gate.lock().await;
        self.inner
            .state
            .storage()
            .set_alarm(Duration::from_secs(10))
            .await?;
        self.inner.initialize().await?;
        self.inner.tick().await?;
        Response::ok("ok")
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let _guard = self.inner.gate.lock().await;
        let mut live = self.inner.live.borrow_mut();
        let stream = live.session.as_ref().map(|s| s.stream.clone());
        let Some(peer) = live.peers.iter_mut().find(|p| p.ws == ws) else {
            return Ok(());
        };
        let text = match message {
            WebSocketIncomingMessage::String(s) if s.len() <= 8192 => s,
            _ => {
                let _ = ws.close(Some(1008), Some("invalid message"));
                return Ok(());
            }
        };

        let valid = match serde_json::from_str::<ClientMessage>(&text) {
            Ok(ClientMessage::Ack { stream_id, seq })
                if peer.ready && peer.ack && Some(stream_id.as_str()) == stream.as_deref() =>
            {
                peer.window.acknowledge(seq).is_ok()
            }
            Ok(ClientMessage::Pong { nonce })
                if peer
                    .ping
                    .as_ref()
                    .is_some_and(|(n, at)| n == &nonce && now() < *at + 10_000) =>
            {
                peer.ping = None;
                true
            }
            _ => false,
        };
        if !valid {
            let _ = ws.send(&ServerMessage::Error {
                code: "invalid_request",
                message: "invalid ACK or pong",
                retry_after_ms: None,
            });
            let _ = ws.close(Some(1008), Some("invalid request"));
        } else {
            pump(&mut live);
        }
        Ok(())
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _clean: bool,
    ) -> Result<()> {
        self.inner.remove(&ws);
        Ok(())
    }

    async fn websocket_error(&self, ws: WebSocket, _err: Error) -> Result<()> {
        self.inner.remove(&ws);
        let _ = ws.close(Some(1011), Some("connection error"));
        Ok(())
    }
}

impl Room {
    fn upgrade(&self, req: &Request) -> Result<Response> {
        if req
            .headers()
            .get("Upgrade")?
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("websocket"))
            != Some(true)
        {
            return failure(400, "invalid_request");
        }

        let token = req
            .url()?
            .query_pairs()
            .find(|(k, _)| k == "ticket")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        let mut live = self.inner.live.borrow_mut();
        if live.peers.len() >= 32 {
            return failure(429, "subscription_limit");
        }
        let Some(ticket) = live.tickets.remove(&token) else {
            return failure(401, "invalid_ticket");
        };
        if ticket.expires <= now() || live.session.is_none() {
            return failure(410, "expired_ticket");
        }

        live.log.trim(now());
        let seq = ticket.after.as_ref().map_or(live.log.tail, |c| c.seq);
        if let Err(reason) = live.log.check(seq) {
            return unavailable(reason, Some(live.log.earliest()), Some(live.log.tail));
        }

        let pair = WebSocketPair::new()?;
        self.inner.state.accept_web_socket(&pair.server);
        live.peers.push(Peer {
            ws: pair.server,
            ack: ticket.ack,
            window: Window {
                acked: seq,
                sent: seq,
                limit: ticket.window,
            },
            ready: false,
            ping: None,
        });
        live.idle_at = 0;
        if live.authenticated {
            ready(&mut live, ticket.after.is_some())?;
            pump(&mut live);
        }

        // 上游任务由对象托管；连接退出后仍通过统一路径履行场次清理责任。
        if !live.running && !live.ending {
            live.running = true;
            let inner = Rc::clone(&self.inner);
            self.inner.state.wait_until(async move {
                inner.run().await;
            });
        }
        Response::from_websocket(pair.client)
    }
}

impl Inner {
    async fn initialize(&self) -> Result<()> {
        if self.live.borrow().initialized {
            return Ok(());
        }
        // 对象重建后仅履行持久化的场次清理责任，不恢复旧内存流。
        if let Some(session) = self.state.storage().get::<Session>("session").await? {
            for ws in self.state.get_websockets() {
                let _ = ws.close(Some(1012), Some("room restarted"));
            }
            self.state
                .storage()
                .set_alarm(Duration::from_secs(10))
                .await?;
            api::end(&self.env, &session.game).await?;
            self.state.storage().delete("session").await?;
        }
        self.live.borrow_mut().initialized = true;
        Ok(())
    }

    fn ticket(&self, input: TicketRequest) -> Result<Response> {
        let mut live = self.live.borrow_mut();
        let Some(session) = live.session.clone() else {
            return Response::empty().map(|r| r.with_status(410));
        };
        if live.ending && input.resume_after.is_none() {
            return failure(503, "server_busy");
        }
        if !live.codes.contains(&input.code_hash) {
            return if input.resume_after.is_some() {
                unavailable("stream_unavailable", None, None)
            } else {
                Response::empty().map(|r| r.with_status(410))
            };
        }

        live.log.trim(now());
        if let Some(cursor) = &input.resume_after {
            if !input.ack || cursor.stream_id != session.stream {
                return unavailable("stream_unavailable", None, None);
            }
            if let Err(reason) = live.log.check(cursor.seq) {
                return unavailable(reason, Some(live.log.earliest()), Some(live.log.tail));
            }
        }
        live.tickets.retain(|_, t| t.expires > now());
        if live.tickets.len() >= 32 || live.peers.len() >= 32 {
            return failure(429, "subscription_limit");
        }
        let token = random_id();
        if live.peers.is_empty() {
            live.idle_at = now() + 30_000;
        }
        live.tickets.insert(
            token.clone(),
            Ticket {
                expires: now() + 30_000,
                ack: input.ack,
                window: input.ack_window.unwrap_or(64).min(64),
                after: input.resume_after,
            },
        );

        let mut url = Url::parse(&input.origin)?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| error("invalid origin"))?;
        url.set_path(&format!("/ws/{}", session.room));
        url.set_query(None);
        url.query_pairs_mut().append_pair("ticket", &token);
        let mut response = Response::from_json(&AdmissionResponse {
            websocket_url: url.to_string(),
            expires_in_ms: 30_000,
        })?;
        response.headers_mut().set("Cache-Control", "no-store")?;
        Ok(response)
    }

    fn remove(&self, ws: &WebSocket) {
        let mut live = self.live.borrow_mut();
        live.peers.retain(|p| p.ws != *ws);
        if live.peers.is_empty() && live.idle_at == 0 {
            live.idle_at = now() + 30_000;
        }
    }

    async fn finish(&self) -> Result<()> {
        let session = self.live.borrow().session.clone();
        {
            let mut live = self.live.borrow_mut();
            live.running = false;
            live.ending = true;
            if let Some(ws) = live.socket.take() {
                let _ = ws.close(Some(1000), Some("session ended"));
            }
            pump(&mut live);
        }

        if self.live.borrow().cleaned {
            return Ok(());
        }
        if let Some(session) = session {
            api::end(&self.env, &session.game).await?;
            self.state.storage().delete("session").await?;
            self.live.borrow_mut().cleaned = true;
        }
        Ok(())
    }

    async fn tick(&self) -> Result<()> {
        let current = now();
        let session = self.live.borrow().session.clone();
        let Some(session) = session else {
            self.state.storage().delete_alarm().await?;
            return Ok(());
        };
        let expired = {
            let live = self.live.borrow();
            live.idle_at != 0 && current >= live.idle_at
        };
        if expired {
            self.finish().await?;
            *self.live.borrow_mut() = Live {
                initialized: true,
                ..Live::new()
            };
            self.state.storage().delete_alarm().await?;
            return Ok(());
        }
        if self.live.borrow().ending {
            return self.finish().await;
        }

        let heartbeat = self.live.borrow().heartbeat_at;
        if current >= heartbeat {
            self.live.borrow_mut().heartbeat_at = current + 20_000;
            if api::heartbeat(&self.env, &session.game).await.is_err() {
                return self.finish().await;
            }
            let live = self.live.borrow();
            if live.authenticated
                && let Some(ws) = &live.socket
            {
                ws.send_with_bytes(
                    packet::encode(Operation::Heartbeat, b"")
                        .map_err(|_| error("heartbeat encode"))?,
                )?;
            }
        }
        let mut live = self.live.borrow_mut();

        live.log.trim(now());
        for peer in &mut live.peers {
            if peer
                .ping
                .as_ref()
                .is_some_and(|(_, at)| now() >= *at + 10_000)
            {
                let _ = peer.ws.close(Some(1001), Some("pong timeout"));
            } else if peer.ping.is_none() {
                let nonce = random_id();
                let _ = peer.ws.send(&ServerMessage::Ping { nonce: &nonce });
                peer.ping = Some((nonce, now()));
            }
        }
        pump(&mut live);
        Ok(())
    }
}

fn ready(live: &mut Live, resumed: bool) -> Result<()> {
    let session = live
        .session
        .as_ref()
        .ok_or_else(|| error("missing session"))?;
    for peer in live.peers.iter_mut().filter(|p| !p.ready) {
        if peer.ack {
            peer.ws.send(&ServerMessage::DeliveryReady {
                room_id: session.room,
                game_id: &session.game,
                stream_id: &session.stream,
                seq: peer.window.sent,
                resumed,
                ack_window: peer.window.limit,
            })?;
        } else {
            peer.ws.send(&ServerMessage::Ready {
                room_id: session.room,
                game_id: &session.game,
            })?;
        }
        peer.ready = true;
    }
    Ok(())
}

fn pump(live: &mut Live) {
    for peer in live.peers.iter_mut().filter(|p| p.ready && p.ack) {
        if let Err(reason) = live.log.check(peer.window.sent) {
            let _ = peer.ws.send(&json!({
                "type": "resume_unavailable",
                "reason": reason,
                "earliest_after": live.log.earliest(),
                "latest_seq": live.log.tail,
            }));
            let _ = peer.ws.close(Some(1008), Some("replay gap"));
            continue;
        }
        while peer.window.available() {
            let Some(text) = live.log.next(peer.window.sent) else {
                break;
            };
            if peer.ws.send_with_str(text).is_err() {
                break;
            }
            peer.window.sent += 1;
        }
    }

    if live.ending {
        for peer in &live.peers {
            if !peer.ack || (peer.window.acked == live.log.tail) {
                let _ = peer.ws.send(&ServerMessage::Closed);
                let _ = peer.ws.close(Some(1000), Some("session ended"));
            }
        }
    }
}
