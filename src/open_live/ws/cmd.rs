//! 长连接推送的直播间事件。
//!
//! `cmd` 决定变体。文档里列出的字段缺失时用零值或空字符串，未列出的 `cmd` 保留原文。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::WsError;

const CMD_DM: &str = "LIVE_OPEN_PLATFORM_DM";
const CMD_DM_MIRROR: &str = "LIVE_OPEN_PLATFORM_DM_MIRROR";
const CMD_GIFT: &str = "LIVE_OPEN_PLATFORM_SEND_GIFT";
const CMD_SUPER_CHAT: &str = "LIVE_OPEN_PLATFORM_SUPER_CHAT";
const CMD_SUPER_CHAT_DEL: &str = "LIVE_OPEN_PLATFORM_SUPER_CHAT_DEL";
const CMD_GUARD: &str = "LIVE_OPEN_PLATFORM_GUARD";
const CMD_LIKE: &str = "LIVE_OPEN_PLATFORM_LIKE";
const CMD_ROOM_ENTER: &str = "LIVE_OPEN_PLATFORM_LIVE_ROOM_ENTER";
const CMD_LIVE_START: &str = "LIVE_OPEN_PLATFORM_LIVE_START";
const CMD_LIVE_END: &str = "LIVE_OPEN_PLATFORM_LIVE_END";
const CMD_INTERACTION_END: &str = "LIVE_OPEN_PLATFORM_INTERACTION_END";

/// 官方长连接推送的一条直播间事件。
#[derive(Debug, Clone, PartialEq)]
pub enum LiveEvent {
    /// 本房间弹幕。
    Danmaku(Danmaku),
    /// 跨房弹幕。除文档列出的字段外，发送人信息不可用。
    DanmakuMirror(DanmakuMirror),
    /// 送礼。
    Gift(Gift),
    /// 付费留言。
    SuperChat(SuperChat),
    /// 付费留言下线。
    SuperChatDelete(SuperChatDelete),
    /// 上舰。
    Guard(Guard),
    /// 点赞。平台按用户把最近 2 秒聚合成一条。
    Like(Like),
    /// 观众进入房间。
    RoomEnter(RoomEnter),
    /// 开始直播。
    LiveStart(LiveBoundary),
    /// 结束直播。
    LiveEnd(LiveBoundary),
    /// 这条长连接不再推送，对应的 `game_id` 已失效。
    InteractionEnd(InteractionEnd),
    /// 文档尚未列出的命令。`data` 保持平台原文。
    Unknown {
        /// 平台给出的 `cmd`。
        cmd: String,
        /// 平台给出的 `data`。没有该字段时是 `Null`。
        data: Value,
    },
}

impl LiveEvent {
    /// 平台协议里的 `cmd` 字符串。
    #[must_use]
    pub fn cmd(&self) -> &str {
        match self {
            Self::Danmaku(_) => CMD_DM,
            Self::DanmakuMirror(_) => CMD_DM_MIRROR,
            Self::Gift(_) => CMD_GIFT,
            Self::SuperChat(_) => CMD_SUPER_CHAT,
            Self::SuperChatDelete(_) => CMD_SUPER_CHAT_DEL,
            Self::Guard(_) => CMD_GUARD,
            Self::Like(_) => CMD_LIKE,
            Self::RoomEnter(_) => CMD_ROOM_ENTER,
            Self::LiveStart(_) => CMD_LIVE_START,
            Self::LiveEnd(_) => CMD_LIVE_END,
            Self::InteractionEnd(_) => CMD_INTERACTION_END,
            Self::Unknown { cmd, .. } => cmd,
        }
    }

    /// 收到后平台不会再为这场 `game_id` 推送。
    #[must_use]
    pub const fn ends_push(&self) -> bool {
        matches!(self, Self::InteractionEnd(_))
    }
}

/// 弹幕用户或收礼主播的公开资料。
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UserProfile {
    /// 用户 uid。弹幕和礼物里的送礼人 uid 已废弃，固定为 0。
    #[serde(default)]
    pub uid: i64,
    /// 用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 用户在这个开发者下的唯一标识。未开通时为空字符串。
    #[serde(default)]
    pub union_id: String,
    /// 昵称。
    #[serde(default)]
    pub uname: String,
    /// 头像地址。
    #[serde(default)]
    pub uface: String,
}

/// `LIVE_OPEN_PLATFORM_DM`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Danmaku {
    /// 用户昵称。
    #[serde(default)]
    pub uname: String,
    /// 用户 uid。已废弃，固定为 0。
    #[serde(default)]
    pub uid: i64,
    /// 用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 用户在这个开发者下的唯一标识。未开通时为空字符串。
    #[serde(default)]
    pub union_id: String,
    /// 用户头像。
    #[serde(default)]
    pub uface: String,
    /// 弹幕发送时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 接收弹幕的直播间。
    #[serde(default)]
    pub room_id: i64,
    /// 弹幕内容。
    #[serde(default)]
    pub msg: String,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
    /// 这个房间的大航海等级。1 总督，2 提督，3 舰长。
    #[serde(default)]
    pub guard_level: i64,
    /// 是否佩戴这个房间的粉丝勋章。
    #[serde(default)]
    pub fans_medal_wearing_status: bool,
    /// 粉丝勋章名。
    #[serde(default)]
    pub fans_medal_name: String,
    /// 粉丝勋章等级。
    #[serde(default)]
    pub fans_medal_level: i64,
    /// 表情包图片地址。
    #[serde(default)]
    pub emoji_img_url: String,
    /// 弹幕类型。0 普通弹幕，1 表情包弹幕。
    #[serde(default)]
    pub dm_type: i64,
    /// 直播荣耀等级。
    #[serde(default)]
    pub glory_level: i64,
    /// 被 at 用户的唯一标识。
    #[serde(default)]
    pub reply_open_id: String,
    /// 被 at 用户的昵称。
    #[serde(default)]
    pub reply_uname: String,
    /// 发送者是否为房管。1 表示是。
    #[serde(default)]
    pub is_admin: i64,
}

/// `LIVE_OPEN_PLATFORM_DM_MIRROR`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DanmakuMirror {
    /// 弹幕发送时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 当前收到这条弹幕的直播间。
    #[serde(default)]
    pub room_id: i64,
    /// 弹幕内容。
    #[serde(default)]
    pub msg: String,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
    /// 表情包图片地址。
    #[serde(default)]
    pub emoji_img_url: String,
    /// 弹幕类型。0 普通弹幕，1 表情包弹幕。
    #[serde(default)]
    pub dm_type: i64,
}

/// `LIVE_OPEN_PLATFORM_SEND_GIFT`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Gift {
    /// 房间号。演播厅模式是演播厅房间，否则是收礼房间。
    #[serde(default)]
    pub room_id: i64,
    /// 送礼用户 uid。已废弃，固定为 0。
    #[serde(default)]
    pub uid: i64,
    /// 送礼用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 送礼用户在这个开发者下的唯一标识。
    #[serde(default)]
    pub union_id: String,
    /// 送礼用户昵称。
    #[serde(default)]
    pub uname: String,
    /// 送礼用户头像。
    #[serde(default)]
    pub uface: String,
    /// 道具 ID。盲盒时是爆出的道具 ID。
    #[serde(default)]
    pub gift_id: i64,
    /// 道具名。盲盒时是爆出的道具名。
    #[serde(default)]
    pub gift_name: String,
    /// 赠送数量。
    #[serde(default)]
    pub gift_num: i64,
    /// 礼物单价。1000 = 1 元 = 10 电池。盲盒时是爆出道具的价值。
    #[serde(default)]
    pub price: i64,
    /// 实际价值，单位与 `price` 相同。
    #[serde(default)]
    pub r_price: i64,
    /// 是否是付费道具。
    #[serde(default)]
    pub paid: bool,
    /// 送礼人在这个房间的粉丝勋章等级。
    #[serde(default)]
    pub fans_medal_level: i64,
    /// 粉丝勋章名。
    #[serde(default)]
    pub fans_medal_name: String,
    /// 是否佩戴这个房间的粉丝勋章。
    #[serde(default)]
    pub fans_medal_wearing_status: bool,
    /// 大航海等级。
    #[serde(default)]
    pub guard_level: i64,
    /// 收礼时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 收礼主播。
    #[serde(default)]
    pub anchor_info: Option<UserProfile>,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
    /// 道具图标地址。
    #[serde(default)]
    pub gift_icon: String,
    /// 是否是连击道具。
    #[serde(default)]
    pub combo_gift: bool,
    /// 连击信息。
    #[serde(default)]
    pub combo_info: Option<ComboInfo>,
    /// 盲盒信息。
    #[serde(default)]
    pub blind_gift: Option<BlindGift>,
}

/// 连击礼物的一次连击。
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ComboInfo {
    /// 每次连击赠送的道具数量。
    #[serde(default)]
    pub combo_base_num: i64,
    /// 连击次数。
    #[serde(default)]
    pub combo_count: i64,
    /// 连击 ID。
    #[serde(default)]
    pub combo_id: String,
    /// 连击有效期，秒。
    #[serde(default)]
    pub combo_timeout: i64,
}

/// 盲盒礼物。
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BlindGift {
    /// 盲盒 ID。
    #[serde(default)]
    pub blind_gift_id: i64,
    /// 是否是盲盒。
    #[serde(default)]
    pub status: bool,
}

/// `LIVE_OPEN_PLATFORM_SUPER_CHAT`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SuperChat {
    /// 直播间 ID。
    #[serde(default)]
    pub room_id: i64,
    /// 购买用户 uid。已废弃，固定为 0。
    #[serde(default)]
    pub uid: i64,
    /// 购买用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 购买用户在这个开发者下的唯一标识。
    #[serde(default)]
    pub union_id: String,
    /// 购买用户昵称。
    #[serde(default)]
    pub uname: String,
    /// 购买用户头像。
    #[serde(default)]
    pub uface: String,
    /// 留言 ID。风控撤回时用它对应下线通知。
    #[serde(default)]
    pub message_id: i64,
    /// 留言内容。
    #[serde(default)]
    pub message: String,
    /// 支付金额，元。
    #[serde(default)]
    pub rmb: i64,
    /// 赠送时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 生效开始时间，秒级时间戳。
    #[serde(default)]
    pub start_time: i64,
    /// 生效结束时间，秒级时间戳。
    #[serde(default)]
    pub end_time: i64,
    /// 大航海等级。
    #[serde(default)]
    pub guard_level: i64,
    /// 粉丝勋章等级。
    #[serde(default)]
    pub fans_medal_level: i64,
    /// 粉丝勋章名。
    #[serde(default)]
    pub fans_medal_name: String,
    /// 是否佩戴这个房间的粉丝勋章。
    #[serde(default)]
    pub fans_medal_wearing_status: bool,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
}

/// `LIVE_OPEN_PLATFORM_SUPER_CHAT_DEL`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SuperChatDelete {
    /// 直播间 ID。
    #[serde(default)]
    pub room_id: i64,
    /// 下线的留言 ID。
    #[serde(default)]
    pub message_ids: Vec<i64>,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
}

/// `LIVE_OPEN_PLATFORM_GUARD`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Guard {
    /// 上舰用户。
    #[serde(default)]
    pub user_info: Option<UserProfile>,
    /// 大航海等级。1 总督，2 提督，3 舰长。
    #[serde(default)]
    pub guard_level: i64,
    /// 大航海数量。单位不是「月」时以 `guard_unit` 为准。
    #[serde(default)]
    pub guard_num: i64,
    /// 大航海单位。通常是「月」。
    #[serde(default)]
    pub guard_unit: String,
    /// 大航海金瓜子。
    #[serde(default)]
    pub price: i64,
    /// 粉丝勋章等级。
    #[serde(default)]
    pub fans_medal_level: i64,
    /// 粉丝勋章名。
    #[serde(default)]
    pub fans_medal_name: String,
    /// 是否佩戴这个房间的粉丝勋章。
    #[serde(default)]
    pub fans_medal_wearing_status: bool,
    /// 房间号。
    #[serde(default)]
    pub room_id: i64,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
    /// 上舰时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
}

/// `LIVE_OPEN_PLATFORM_LIKE`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Like {
    /// 用户昵称。
    #[serde(default)]
    pub uname: String,
    /// 用户 uid。已废弃，固定为 0。
    #[serde(default)]
    pub uid: i64,
    /// 用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 用户在这个开发者下的唯一标识。
    #[serde(default)]
    pub union_id: String,
    /// 用户头像。
    #[serde(default)]
    pub uface: String,
    /// 时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 发生的直播间。
    #[serde(default)]
    pub room_id: i64,
    /// 点赞文案。
    #[serde(default)]
    pub like_text: String,
    /// 这个用户最近 2 秒的点赞次数。
    #[serde(default)]
    pub like_count: i64,
    /// 是否佩戴这个房间的粉丝勋章。
    #[serde(default)]
    pub fans_medal_wearing_status: bool,
    /// 粉丝勋章名。
    #[serde(default)]
    pub fans_medal_name: String,
    /// 粉丝勋章等级。
    #[serde(default)]
    pub fans_medal_level: i64,
    /// 消息唯一 ID。
    #[serde(default)]
    pub msg_id: String,
}

/// `LIVE_OPEN_PLATFORM_LIVE_ROOM_ENTER`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RoomEnter {
    /// 发生的直播间。
    #[serde(default)]
    pub room_id: i64,
    /// 用户头像。
    #[serde(default)]
    pub uface: String,
    /// 用户昵称。
    #[serde(default)]
    pub uname: String,
    /// 用户在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 用户在这个开发者下的唯一标识。
    #[serde(default)]
    pub union_id: String,
    /// 发生时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
}

/// 开播或下播。`LIVE_OPEN_PLATFORM_LIVE_START` 与 `LIVE_OPEN_PLATFORM_LIVE_END` 字段相同。
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LiveBoundary {
    /// 发生的直播间。
    #[serde(default)]
    pub room_id: i64,
    /// 主播在这个应用下的唯一标识。
    #[serde(default)]
    pub open_id: String,
    /// 主播在这个开发者下的唯一标识。
    #[serde(default)]
    pub union_id: String,
    /// 发生时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
    /// 开播二级分区名称。
    #[serde(default)]
    pub area_name: String,
    /// 开播时刻的直播间标题。
    #[serde(default)]
    pub title: String,
}

/// `LIVE_OPEN_PLATFORM_INTERACTION_END`
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct InteractionEnd {
    /// 停止推送的场次 ID。
    #[serde(default)]
    pub game_id: String,
    /// 发生时间，秒级时间戳。
    #[serde(default)]
    pub timestamp: i64,
}

pub(super) fn parse_command(bytes: &[u8]) -> Result<LiveEvent, WsError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| WsError::decode(format!("开放平台推送不是 JSON：{error}")))?;
    let cmd = value
        .get("cmd")
        .and_then(Value::as_str)
        .filter(|cmd| !cmd.is_empty())
        .ok_or_else(|| WsError::decode("开放平台推送缺少 cmd"))?;
    let data = match value.get("data").cloned() {
        None | Some(Value::Null) => Value::Object(serde_json::Map::new()),
        Some(data) => data,
    };
    let event = match cmd {
        CMD_DM => LiveEvent::Danmaku(parse_data(cmd, data)?),
        CMD_DM_MIRROR => LiveEvent::DanmakuMirror(parse_data(cmd, data)?),
        CMD_GIFT => LiveEvent::Gift(parse_data(cmd, data)?),
        CMD_SUPER_CHAT => LiveEvent::SuperChat(parse_data(cmd, data)?),
        CMD_SUPER_CHAT_DEL => LiveEvent::SuperChatDelete(parse_data(cmd, data)?),
        CMD_GUARD => LiveEvent::Guard(parse_data(cmd, data)?),
        CMD_LIKE => LiveEvent::Like(parse_data(cmd, data)?),
        CMD_ROOM_ENTER => LiveEvent::RoomEnter(parse_data(cmd, data)?),
        CMD_LIVE_START => LiveEvent::LiveStart(parse_data(cmd, data)?),
        CMD_LIVE_END => LiveEvent::LiveEnd(parse_data(cmd, data)?),
        CMD_INTERACTION_END => LiveEvent::InteractionEnd(parse_data(cmd, data)?),
        _ => {
            return Ok(LiveEvent::Unknown {
                cmd: cmd.to_owned(),
                data: value.get("data").cloned().unwrap_or(Value::Null),
            });
        }
    };
    Ok(event)
}

fn parse_data<T: for<'de> Deserialize<'de>>(cmd: &str, data: Value) -> Result<T, WsError> {
    serde_json::from_value(data)
        .map_err(|error| WsError::decode(format!("{cmd} 无法解析：{error}")))
}

#[cfg(test)]
#[path = "cmd_test.rs"]
mod cmd_test;
