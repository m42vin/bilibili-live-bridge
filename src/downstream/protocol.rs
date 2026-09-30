//! 下游文本消息封装；官方事件类型保持独立于客户端协议。

use serde::{Deserialize, Serialize, Serializer};

use crate::open_live::ws::LiveEvent;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ClientMessage {
    Subscribe { code: String },
}

pub(super) fn subscription_code(text: &str) -> Result<String, &'static str> {
    let ClientMessage::Subscribe { code } =
        serde_json::from_str(text).map_err(|_| "需要发送包含身份码的 subscribe 文本消息")?;
    let code = code.trim();
    if code.is_empty() {
        return Err("身份码不能为空");
    }
    Ok(code.to_owned())
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ServerMessage<'a> {
    Ready {
        room_id: i64,
        game_id: &'a str,
    },
    Event {
        room_id: i64,
        cmd: &'a str,
        data: EventData<'a>,
    },
    Lagged {
        skipped: u64,
    },
    Error {
        code: &'a str,
        message: &'a str,
    },
    Closed,
}

impl<'a> ServerMessage<'a> {
    pub(super) fn event(room_id: i64, event: &'a LiveEvent) -> Self {
        Self::Event {
            room_id,
            cmd: event.cmd(),
            data: EventData(event),
        }
    }
}

pub(super) struct EventData<'a>(&'a LiveEvent);

impl Serialize for EventData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            LiveEvent::Danmaku(data) => data.serialize(serializer),
            LiveEvent::DanmakuMirror(data) => data.serialize(serializer),
            LiveEvent::Gift(data) => data.serialize(serializer),
            LiveEvent::SuperChat(data) => data.serialize(serializer),
            LiveEvent::SuperChatDelete(data) => data.serialize(serializer),
            LiveEvent::Guard(data) => data.serialize(serializer),
            LiveEvent::Like(data) => data.serialize(serializer),
            LiveEvent::RoomEnter(data) => data.serialize(serializer),
            LiveEvent::LiveStart(data) | LiveEvent::LiveEnd(data) => data.serialize(serializer),
            LiveEvent::InteractionEnd(data) => data.serialize(serializer),
            LiveEvent::Unknown { data, .. } => data.serialize(serializer),
        }
    }
}
