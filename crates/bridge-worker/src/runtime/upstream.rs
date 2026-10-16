//! 上游连接任务；每次等待后核对流标识，避免旧任务修改替换后的场次。

use std::{rc::Rc, time::Duration};

use bridge_core::downstream::protocol::ServerMessage;
use bridge_core::open_live::{
    model::start_result_from_value,
    ws::{
        cmd::LiveEvent,
        packet::{self, FrameItem, Operation},
    },
};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use serde_json::json;
use worker::*;

use super::super::deadline;
use super::{Inner, Live, Session, api, error, now, pump, ready};

impl Inner {
    /// 驱动当前场次的上游连接，退出时仅清理仍属于本任务的流。
    pub(super) async fn run(self: Rc<Self>) {
        let Some(session) = self.live.borrow().session.clone() else {
            return;
        };

        let result = self.consume(&session).await;
        let _guard = self.gate.lock().await;
        if self
            .live
            .borrow()
            .session
            .as_ref()
            .is_some_and(|s| s.stream == session.stream)
        {
            if result.is_err() {
                for peer in &self.live.borrow().peers {
                    let _ = peer.ws.send(&ServerMessage::Error {
                        code: "attach_failed",
                        message: "upstream disconnected or unavailable",
                        retry_after_ms: None,
                    });
                }
            }
            if self.finish().await.is_err() {
                console_error!("room cleanup pending");
            }
        }
    }

    async fn consume(&self, session: &Session) -> Result<()> {
        let started = start_result_from_value(session.candidate.clone())
            .map_err(|_| error("invalid stored session"))?;

        for link in started.wss_link().iter().take(3) {
            let url = Url::parse(link)?;
            if url.scheme() != "wss"
                && !(option_env!("BRIDGE_WORKER_TEST_API_ORIGIN").is_some() && url.scheme() == "ws")
            {
                continue;
            }

            let socket = match api::connect(url).await {
                Ok(ws) => ws,
                Err(_) => continue,
            };
            let mut events = socket.events()?;
            socket.accept()?;
            {
                let _guard = self.gate.lock().await;
                let mut live = self.live.borrow_mut();
                if !live.running
                    || live
                        .session
                        .as_ref()
                        .is_none_or(|s| s.stream != session.stream)
                {
                    let _ = socket.close(Some(1000), Some("cancelled"));
                    return Ok(());
                }
                live.socket = Some(socket.clone());
            }

            socket.send_with_bytes(
                packet::encode(Operation::Auth, started.auth_body().as_bytes())
                    .map_err(|_| error("auth encode"))?,
            )?;

            let auth = deadline(10_000, async {
                let mut early = Vec::new();

                loop {
                    let event = events
                        .next()
                        .await
                        .ok_or_else(|| error("closed before auth"))??;
                    let WebsocketEvent::Message(message) = event else {
                        return Err(error("closed before auth"));
                    };
                    let bytes = message
                        .bytes()
                        .ok_or_else(|| error("binary frame required"))?;
                    if bytes.len() > 1_048_576 {
                        return Err(error("frame too large"));
                    }
                    let items = packet::decode_frame(&bytes)
                        .map_err(|_| error("invalid upstream frame"))?;
                    let mut accepted = false;
                    for item in items {
                        match item {
                            FrameItem::AuthReply(body) => {
                                packet::auth_accepted(&body).map_err(|_| error("auth rejected"))?;
                                accepted = true;
                            }
                            FrameItem::Event(event) => {
                                early.push(event);
                                if early.len() > 32 {
                                    return Err(error("too many early events"));
                                }
                            }
                            FrameItem::HeartbeatReply => {}
                        }
                    }
                    if accepted {
                        return Ok(early);
                    }
                }
            })
            .await;
            let early = match auth {
                Ok(early) => early,
                Err(e) => {
                    let _ = socket.close(Some(1000), Some("auth failed"));
                    return Err(e);
                }
            };
            {
                let _guard = self.gate.lock().await;
                let mut live = self.live.borrow_mut();
                if !live.running
                    || live
                        .session
                        .as_ref()
                        .is_none_or(|s| s.stream != session.stream)
                {
                    let _ = socket.close(Some(1000), Some("cancelled"));
                    return Ok(());
                }
                live.authenticated = true;
                live.last_activity = now();
                ready(&mut live, false)?;
                for event in early {
                    if publish(&mut live, &event)? {
                        return Ok(());
                    }
                }
            }

            loop {
                let event = match select(
                    Box::pin(events.next()),
                    Box::pin(Delay::from(Duration::from_secs(1))),
                )
                .await
                {
                    Either::Left((event, _)) => Some(event),
                    Either::Right(_) => None,
                };
                let _guard = self.gate.lock().await;
                let mut live = self.live.borrow_mut();
                if !live.running
                    || live
                        .session
                        .as_ref()
                        .is_none_or(|s| s.stream != session.stream)
                {
                    return Ok(());
                }
                if now().saturating_sub(live.last_activity) >= 60_000 {
                    return Err(error("upstream inactive"));
                }
                match event {
                    Some(Some(Ok(WebsocketEvent::Message(message)))) => {
                        let bytes = message
                            .bytes()
                            .ok_or_else(|| error("binary frame required"))?;
                        if bytes.len() > 1_048_576 {
                            return Err(error("frame too large"));
                        }
                        for item in packet::decode_frame(&bytes)
                            .map_err(|_| error("invalid upstream frame"))?
                        {
                            match item {
                                FrameItem::HeartbeatReply => live.last_activity = now(),
                                FrameItem::Event(event) => {
                                    live.last_activity = now();
                                    if publish(&mut live, &event)? {
                                        return Ok(());
                                    }
                                }
                                FrameItem::AuthReply(_) => {}
                            }
                        }
                    }
                    Some(_) => return Err(error("upstream closed")),
                    None => {}
                }
            }
        }
        Err(error("no upstream endpoint available"))
    }
}

fn publish(live: &mut Live, event: &LiveEvent) -> Result<bool> {
    let session = live
        .session
        .as_ref()
        .ok_or_else(|| error("missing session"))?;
    let data = event.data_json().map_err(|_| error("event encode"))?;
    let text = serde_json::to_string(&json!({
        "type": "event",
        "room_id": session.room,
        "stream_id": session.stream,
        "seq": live.log.tail + 1,
        "cmd": event.cmd(),
        "data": data,
    }))?;
    live.log.push(now(), text);
    for peer in live.peers.iter().filter(|p| p.ready && !p.ack) {
        let _ = peer.ws.send(&ServerMessage::event(session.room, event));
    }
    pump(live);
    Ok(event.ends_push())
}
