//! 下游文本消息封装；官方事件类型保持独立于客户端协议。

use serde::{Deserialize, Serialize, Serializer};

use crate::delivery::{EventCursor, RecordedEvent, ResumeUnavailable};
use crate::open_live::ws::LiveEvent;

/// 客户端发送的订阅或累计确认消息。
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// 请求订阅，业务约束由 parse_subscribe 校验。
    Subscribe {
        /// 主播身份码。
        code: String,
        /// 是否启用累计确认与续传。
        #[serde(default)]
        ack: bool,
        /// 最多允许的未确认事件数量。
        ack_window: Option<u64>,
        /// 最后连续处理完成的位置；补发从其后开始。
        resume_after: Option<EventCursor>,
    },
    /// 累计确认同一流内已连续处理的事件。
    Ack {
        /// 事件流标识，用于区分不同流的序号。
        stream_id: String,
        /// 桥侧序号。
        seq: u64,
    },
}

/// 经解析与校验的订阅参数。
#[derive(Debug, PartialEq, Eq)]
pub struct Subscribe {
    /// 主播身份码。
    pub code: String,
    /// 是否启用累计确认与续传。
    pub ack: bool,
    /// 最多允许的未确认事件数量。
    pub ack_window: Option<u64>,
    /// 最后连续处理完成的位置；补发从其后开始。
    pub resume_after: Option<EventCursor>,
}

/// 解析首条订阅消息，校验非空身份码、ACK 窗口与续传约束。
pub fn parse_subscribe(text: &str) -> Result<Subscribe, &'static str> {
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

/// 服务端发给下游客户端的 JSON 消息。
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage<'a> {
    /// 上游连接状态变化。
    Status {
        /// 上游状态，如 connected 或 reconnecting。
        state: &'static str,
    },
    /// 实时模式订阅已就绪。
    Ready {
        /// 直播间编号。
        room_id: i64,
        /// 平台互动场次标识。
        game_id: &'a str,
    },
    /// 确认模式订阅已就绪，包含续传位置和窗口。
    #[serde(rename = "ready")]
    DeliveryReady {
        /// 直播间编号。
        room_id: i64,
        /// 平台互动场次标识。
        game_id: &'a str,
        /// 事件流标识，用于区分不同流的序号。
        stream_id: &'a str,
        /// 桥侧序号。
        seq: u64,
        /// 是否成功续传已有事件流。
        resumed: bool,
        /// 最多允许的未确认事件数量。
        ack_window: u64,
    },
    /// 实时模式事件，不含桥侧序号。
    Event {
        /// 直播间编号。
        room_id: i64,
        /// 平台事件命令名。
        cmd: &'a str,
        /// 事件正文。
        data: EventData<'a>,
    },
    /// 确认模式事件，包含流标识和桥侧序号。
    #[serde(rename = "event")]
    Recorded {
        /// 直播间编号。
        room_id: i64,
        /// 事件流标识，用于区分不同流的序号。
        stream_id: &'a str,
        /// 桥侧序号。
        seq: u64,
        /// 平台事件命令名。
        cmd: &'a str,
        /// 事件正文。
        data: &'a serde_json::value::RawValue,
    },
    /// 无法按给定游标继续完整交付。
    ResumeUnavailable {
        /// 续传失败原因与当前日志保留范围。
        #[serde(flatten)]
        detail: &'a ResumeUnavailable,
    },
    /// 实时订阅落后，部分事件已被覆盖。
    Lagged {
        /// 被覆盖而无法交付的事件数量。
        skipped: u64,
    },
    /// 接入或协议错误。
    Error {
        /// 错误分类。
        code: &'a str,
        /// 错误说明，不包含请求凭据。
        message: &'a str,
        /// 暂时错误的可选重试建议；不保证届时可以接入。
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
    /// 会话已经结束。
    Closed,
}

impl<'a> ServerMessage<'a> {
    /// 将日志记录封装成确认模式事件。
    pub fn recorded(room_id: i64, stream_id: &'a str, record: &'a RecordedEvent) -> Self {
        Self::Recorded {
            room_id,
            stream_id,
            seq: record.seq,
            cmd: &record.cmd,
            data: &record.data,
        }
    }
    /// 将直播事件封装成实时模式消息。
    pub fn event(room_id: i64, event: &'a LiveEvent) -> Self {
        Self::Event {
            room_id,
            cmd: event.cmd(),
            data: EventData(event),
        }
    }
}

/// 直播事件正文的序列化适配器。
pub struct EventData<'a>(&'a LiveEvent);

impl Serialize for EventData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .data_json()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod protocol_test;
