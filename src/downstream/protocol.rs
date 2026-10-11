//! 下游文本消息封装；官方事件类型保持独立于客户端协议。

use serde::{Deserialize, Serialize, Serializer};

use crate::open_live::ws::LiveEvent;
use crate::session::{EventCursor, RecordedEvent, ResumeUnavailable};

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ClientMessage {
    Subscribe {
        code: String,
        #[serde(default)]
        ack: bool,
        ack_window: Option<u64>,
        resume_after: Option<EventCursor>,
    },
    Ack {
        stream_id: String,
        seq: u64,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Subscribe {
    pub code: String,
    pub ack: bool,
    pub ack_window: Option<u64>,
    pub resume_after: Option<EventCursor>,
}

pub(super) fn parse_subscribe(text: &str) -> Result<Subscribe, &'static str> {
    let ClientMessage::Subscribe {
        code,
        ack,
        ack_window,
        resume_after,
    } = serde_json::from_str(text).map_err(|_| "需要有效的 subscribe 文本消息")?
    else {
        return Err("首包必须为 subscribe");
    };
    if code.trim().is_empty() {
        return Err("身份码不能为空");
    }
    if resume_after.is_some() && !ack {
        return Err("续传必须启用 ack");
    }
    if ack_window.is_some_and(|window| window == 0) || (ack_window.is_some() && !ack) {
        return Err("ack_window 必须在 ACK 模式中为正整数");
    }
    Ok(Subscribe {
        code: code.trim().to_owned(),
        ack,
        ack_window,
        resume_after,
    })
}

#[cfg(test)]
pub(super) fn subscription_code(text: &str) -> Result<String, &'static str> {
    parse_subscribe(text).map(|request| request.code)
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(super) enum ServerMessage<'a> {
    Status {
        state: &'static str,
    },
    Ready {
        room_id: i64,
        game_id: &'a str,
    },
    #[serde(rename = "ready")]
    DeliveryReady {
        room_id: i64,
        game_id: &'a str,
        stream_id: &'a str,
        seq: u64,
        resumed: bool,
        ack_window: u64,
    },
    Event {
        room_id: i64,
        cmd: &'a str,
        data: EventData<'a>,
    },
    #[serde(rename = "event")]
    Recorded {
        room_id: i64,
        stream_id: &'a str,
        seq: u64,
        cmd: &'a str,
        data: &'a serde_json::value::RawValue,
    },
    ResumeUnavailable {
        #[serde(flatten)]
        detail: &'a ResumeUnavailable,
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
    pub(super) fn recorded(room_id: i64, stream_id: &'a str, record: &'a RecordedEvent) -> Self {
        Self::Recorded {
            room_id,
            stream_id,
            seq: record.seq,
            cmd: &record.cmd,
            data: &record.data,
        }
    }
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
        self.0
            .data_json()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
