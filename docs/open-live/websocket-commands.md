# 直播间数据

> 整理自哔哩哔哩直播开放平台《开放平台-直播&互玩接入文档》。原文：[长链命令](https://open-live.bilibili.com/document/f9ce25be-312e-1f4a-85fd-fef21f1637f8)。
>
> 长连接推送的直播间事件命令与字段。

- 直播间数据
  - 获取弹幕信息
  - 跨房弹幕信息
  - 获取礼物信息
  - 获取付费留言
  - 付费留言下线
  - 付费大航海
  - 点赞信息
  - 进入房间
  - 关于退出房间
  - 开始直播
  - 结束直播
  - 消息推送结束通知

## 获取弹幕信息

- 当该直播间有人发送弹幕时触发

**CMD**：LIVE_OPEN_PLATFORM_DM

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| uname | string | 用户昵称 |
| uid | int64 | 用户UID（已废弃，固定为0） |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uface | string | 用户头像 |
| timestamp | int64 | 弹幕发送时间秒级时间戳 |
| room_id | int64 | 弹幕接收的直播间 |
| msg | string | 弹幕内容 |
| msg_id | string | 消息唯一id |
| guard_level | int64 | 对应房间大航海等级 |
| fans_medal_wearing_status | bool | 该房间粉丝勋章佩戴情况 |
| fans_medal_name | string | 粉丝勋章名 |
| fans_medal_level | int64 | 对应房间勋章信息 |
| emoji_img_url | string | 表情包图片地址 |
| dm_type | int64 | 弹幕类型 0：普通弹幕 1：表情包弹幕 |
| glory_level | int | 直播荣耀等级 |
| reply_open_id | string | 被at用户唯一标识 |
| reply_uname | string | 被at的用户昵称 |
| is_admin | int | 发送弹幕的用户是否是房管，取值范围`0或1`，取值为`1`时是房管 |

> 参考json

```json
{
    "cmd":"LIVE_OPEN_PLATFORM_DM",
    "data":{
        "room_id":1,//弹幕接收的直播间
        "uid":0,//用户UID(已废弃，固定为0)
        "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//用户唯一标识
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "uname":"",//用户昵称
        "msg":"",//弹幕内容
        "msg_id":"",//消息唯一id
        "fans_medal_level":0,//对应房间勋章信息
        "fans_medal_name":"粉丝勋章名",
        "fans_medal_wearing_status": true,//该房间粉丝勋章佩戴情况
        "guard_level":0,//对应房间大航海 1总督 2提督 3舰长
        "timestamp":0,//弹幕发送时间秒级时间戳
        "uface":""//用户头像
        "emoji_img_url": "", //表情包图片地址
        "dm_type": 0,//弹幕类型 0：普通弹幕 1：表情包弹幕
        "glory_level": 39,//直播荣耀等级
        "reply_open_id": "39b8fedb-60a5-4e29-ac75-b16955f7e632",//被at用户唯一标识
        "reply_uname": "",//被at的用户昵称
        "is_admin":1//发送弹幕的用户是否为房管
    }
}
```

## 跨房弹幕信息

- 当该直播间为支持跨房弹幕的直播间模式，其他直播间有人发送弹幕时触发

**请注意**：除下面列出来的字段外，该cmd推送消息中的其他信息不保证可用性，并且推荐和直播间观众观看内容表现一致：**【不显示发送人信息】**

**CMD**：LIVE_OPEN_PLATFORM_DM_MIRROR

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| timestamp | int64 | 弹幕发送时间秒级时间戳 |
| room_id | int64 | 当前收到这条弹幕的直播间 |
| msg | string | 弹幕内容 |
| msg_id | string | 消息唯一id |
| emoji_img_url | string | 表情包图片地址 |
| dm_type | int64 | 弹幕类型 0：普通弹幕 1：表情包弹幕 |

> 参考json

```json
{
  "cmd": "LIVE_OPEN_PLATFORM_DM_MIRROR",
  "data": {
    "room_id": 1, //当前收到这条弹幕的直播间
    "msg": "", //弹幕内容
    "msg_id": "", //消息唯一id
    "timestamp": 0, //弹幕发送时间秒级时间戳
    "emoji_img_url": "", //表情包图片地址
    "dm_type": 0, //弹幕类型 0：普通弹幕 1：表情包弹幕
  }
}
```

## 获取礼物信息

- 当该直播间有人赠送礼物时触发

**CMD**：LIVE_OPEN_PLATFORM_SEND_GIFT

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 房间号 |
| uid | int64 | 用户UID（已废弃，固定为0） |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uname | string | 送礼用户昵称 |
| uface | string | 送礼用户头像 |
| gift_id | int64 | 道具id(盲盒:爆出道具id) |
| gift_name | string | 道具名(盲盒:爆出道具名) |
| gift_num | int64 | 赠送道具数量 |
| price | int64 | 礼物爆出单价，(1000 = 1元 = 10电池),盲盒:爆出道具的价值 |
| r_price | int64 | 实际价值(1000 = 1元 = 10电池),盲盒:爆出道具的价值 |
| paid | bool | 是否是付费道具 |
| fans_medal_level | int64 | 实际送礼人的勋章信息 |
| fans_medal_name | string | 粉丝勋章名 |
| fans_medal_wearing_status | bool | 该房间粉丝勋章佩戴情况 |
| guard_level | int64 | 大航海等级 |
| timestamp | int64 | 收礼时间秒级时间戳 |
| anchor_info | anchor_info结构体 | 主播信息 |
| msg_id | string | 消息唯一id |
| gift_icon | string | 道具icon |
| combo_gift | bool | 是否是combo道具 |
| combo_info | combo_info结构体 | 连击信息 |
| blind_gift | blind_gift结构体 | 盲盒信息 |

| anchor_info结构体 | 类型 | 描述 |
| --- | --- | --- |
| uid | int64 | 收礼主播uid |
| open_id | string | 收礼主播唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uname | string | 收礼主播昵称 |
| uface | string | 收礼主播头像 |

| combo_info | 类型 | 描述 |
| --- | --- | --- |
| combo_base_num | int64 | 每次连击赠送的道具数量 |
| combo_count | int64 | 连击次数 |
| combo_id | string | 连击id |
| combo_timeout | int64 | 连击有效期秒 |

| blind_gift | 类型 | 描述 |
| --- | --- | --- |
| blind_gift_id | int64 | 盲盒id |
| status | bool | 是否是盲盒 |

> 参考json

```json
{
    "cmd":"LIVE_OPEN_PLATFORM_SEND_GIFT",
    "data":{
        "room_id":1,//直播间(演播厅模式则为演播厅直播间,非演播厅模式则为收礼直播间)
        "uid":0,//用户UID(已废弃，固定为0)
        "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//用户唯一标识
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识
        "uname":"",//送礼用户昵称
        "uface":"",//送礼用户头像
        "gift_id":0,//道具id(盲盒:爆出道具id)
        "gift_name":"",//道具名(盲盒:爆出道具名)
        "gift_num":0,//赠送道具数量
        "price":0,//礼物单价(1000 = 1元 = 10电池),盲盒:爆出道具的价值
        "paid":false,//是否是付费道具
        "fans_medal_level":0,//对应房间勋章信息
        "fans_medal_name":"粉丝勋章名", //粉丝勋章名
        "fans_medal_wearing_status": true,//该房间粉丝勋章佩戴情况
        "guard_level":0,//room_id对应的大航海等级
        "timestamp":0,//收礼时间秒级时间戳
        "msg_id":"",//消息唯一id
        "anchor_info":{
            "uid":0,//收礼主播UID(即将废弃)
            "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//主播唯一标识(2024-03-11后上线)
            "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
            "uname":"",//收礼主播昵称
            "uface":"http://i0.hdslb.com/bfs/face/4add3acfc930fcd07d06ea5e10a3a377314141c2.jpg"//收礼主播头像
        },
        "gift_icon":"http://i1.hdslb.com/dksldksldksld.jpg", //道具icon  （新增）
        "combo_gift":true,//是否是combo道具
        "combo_info":{ //ex：连击次数100，每个连击是批量送5个 既  5 * 100
            "combo_base_num": 5,//每次连击赠送的道具数量
            "combo_count":100,//连击次数
            "combo_id":"xxxxxx",//连击id
            "combo_timeout": 3,//连击有效期秒
        },
        "blind_gift":
        {
            "blind_gift_id": 10086,//盲盒id
            "status": true,//是否是盲盒
        }
    }
}
```

## 获取付费留言

- 当该直播间有人发送付费留言时触发

**CMD**：LIVE_OPEN_PLATFORM_SUPER_CHAT

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 直播间id |
| uid | int64 | 用户UID（已废弃，固定为0） |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uname | string | 购买的用户昵称 |
| uface | string | 购买用户头像 |
| message_id | int64 | 留言id(风控场景下撤回留言需要) |
| message | string | 留言内容 |
| rmb | int64 | 支付金额(元) |
| timestamp | int64 | 赠送时间秒级 |
| start_time | int64 | 生效开始时间 |
| end_time | int64 | 生效结束时间 |
| guard_level | int64 | 对应房间大航海等级 |
| fans_medal_level | int64 | 对应房间勋章信息 |
| fans_medal_name | string | 对应房间勋章名字 |
| fans_medal_wearing_status | bool | 该房间粉丝勋章佩戴情况 |
| msg_id | string | 消息唯一id |

> 参考json

```json
{
    "cmd":"LIVE_OPEN_PLATFORM_SUPER_CHAT",
    "data":{
        "room_id":1,//直播间id
        "uid":0,//购买用户UID(已废弃，固定为0)
        "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//购买用户唯一标识
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "uname":"",//购买的用户昵称
        "uface":"",//购买用户头像
        "message_id":0,//留言id(风控场景下撤回留言需要)
        "message":"",//留言内容
        "msg_id":"",//消息唯一id
        "rmb":0,//支付金额(元)
        "timestamp":0,//赠送时间秒级
        "start_time":0,//生效开始时间
        "end_time":0,//生效结束时间
        "guard_level": 2, //对应房间大航海登记    (新增)
        "fans_medal_level": 26, //对应房间勋章信息  (新增)
        "fans_medal_name": "aw4ifC", //对应房间勋章名字  (新增)
        "fans_medal_wearing_status": true //该房间粉丝勋章佩戴情况   (新增)
    }
}
```

## 付费留言下线

- 当该直播间有付费留言被下线时触发

**CMD**：LIVE_OPEN_PLATFORM_SUPER_CHAT_DEL

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 直播间id |
| message_ids | []int64 | 留言id |
| msg_id | string | 消息唯一id |

> 参考json

```json
{
    "cmd":"LIVE_OPEN_PLATFORM_SUPER_CHAT_DEL",
    "data":{
        "room_id":1,//直播间id
        "message_ids":[1,2],// 留言id
        "msg_id":""//消息唯一id
    }
}
```

## 付费大航海

- 当该直播间有人上舰时触发

**CMD**：LIVE_OPEN_PLATFORM_GUARD

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| user_info | user_info结构体 | 用户信息 |
| guard_level | int64 | 大航海等级 |
| guard_num | int64 | 大航海数量 |
| guard_unit | string | 大航海单位(正常单位为“月”，如为其他内容，无视`guard_num`以本字段内容为准，例如`*3天`) |
| price | int64 | 大航海金瓜子 |
| fans_medal_level | int64 | 粉丝勋章等级 |
| fans_medal_name | string | 粉丝勋章名 |
| fans_medal_wearing_status | bool | 该房间粉丝勋章佩戴情况 |
| room_id | int64 | 房间号 |
| msg_id | string | 消息唯一id |
| timestamp | int64 | 上舰时间秒级时间戳 |

| user_info结构体 | 类型 | 描述 |
| --- | --- | --- |
| uid | int64 | 用户UID（已废弃，固定为0） |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uname | string | 用户昵称 |
| uface | string | 用户头像 |

> 参考json

```json
{
    "cmd": "LIVE_OPEN_PLATFORM_GUARD",
    "data": {
        "user_info": {
            "uid":0,//用户UID(已废弃，固定为0)
            "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//用户唯一标识
            "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
            "uname":"",//用户昵称
            "uface": "http://i0.hdslb.com/bfs/face/4add3acfc930fcd07d06ea5e10a3a377314141c2.jpg" //用户头像
        },
        "guard_level": 3, //对应的大航海等级 1总督 2提督 3舰长
        "guard_num": 1,
        "guard_unit": "月", // (正常单位为“月”，如为其他内容，无视`guard_num`以本字段内容为准，例如`*3天`)
        "price": 198000,
        "fans_medal_level": 24, //粉丝勋章等级
        "fans_medal_name": "aw4ifC", //粉丝勋章名
        "fans_medal_wearing_status": false, //该房间粉丝勋章佩戴情况
        "timestamp": 1653555128,
        "room_id": 460695,
        "msg_id":""//消息唯一id
    }
}
```

## 点赞信息

- 该直播间有用户在移动端双击直播画面点赞时触发

**请注意：**

- 只有房间处于开播中，才会触发点赞事件。
- 对单一用户最近2秒聚合发送一次点赞次数。

**CMD**：LIVE_OPEN_PLATFORM_LIKE

**返回字段**：

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| uname | string | 用户昵称 |
| uid | int64 | 用户UID（已废弃，固定为0） |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| uface | string | 用户头像 |
| timestamp | int64 | 时间秒级时间戳 |
| room_id | int64 | 发生的直播间 |
| like_text | string | 点赞文案( “xxx点赞了”) |
| like_count | int64 | 对单个用户最近2秒的点赞次数聚合 |
| fans_medal_wearing_status | bool | 该房间粉丝勋章佩戴情况 |
| fans_medal_name | string | 粉丝勋章名 |
| fans_medal_level | int64 | 对应房间勋章信息 |

> 参考json

```json
{
    "data": {
        "uname": "哔哩哔哩直播",
        "uid":0,
        "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "uface": "https://i0.hdslb.com/bfs/face/8f6a614a48a3813d90da7a11894ae56a59396fcd.jpg",
        "timestamp": 1685946262,
        "like_text": "为主播点赞了",
        "like_count":114,
        "fans_medal_wearing_status": false,
        "fans_medal_name": "",
        "fans_medal_level": 0,
        "msg_id": "57a7c676-ff00-4967-bb09-03e800ab0f4d",
        "room_id": 1
    },
    "cmd": "LIVE_OPEN_PLATFORM_LIKE"
}
```

## 进入房间

- 该直播间有观众进入直播间时触发

**请注意：如果单个直播间每秒进入人数超过R个人，那么会被限制每秒推送数量，其中R由当前服务的负载动态决定(最低不会低于`10 QPS`)**

**CMD**：LIVE_OPEN_PLATFORM_LIVE_ROOM_ENTER

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 发生的直播间 |
| uface | string | 用户头像 |
| uname | string | 用户昵称 |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| timestamp | int64 | 发生的时间戳 |

```json
{
    "cmd":"LIVE_OPEN_PLATFORM_LIVE_ROOM_ENTER",
    "data":{
        "room_id":1,
        "uface":"", // 头像
        "uname":"",// 昵称
        "open_id":"39b8fedb-60a5-4e29-ac75-b16955f7e632",//用户唯一标识
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "timestamp":0, //时间戳
    }
}
```

## 关于退出房间

由于没有明确的退出事件触发和标志，在**包括但不限于**以下情况都下可能会触发“退出房间”事件：

- 标签页后台挂起，被浏览器冻结或终止。
- 直播间检测到无响应（确认弹窗超时）后主动断开连接。
- 网络不良导致的断续重连。
- 关闭直播间后服务器推送消息超时。
- 其他未列出的情况

以上这些情况通常在服务器异步调用情况下被发现是连接不可达，系统会猜测用户已退出房间。因此，在数据不可信且存在大量重复无意义数据的情况下，未提供明确的退出房间事件。

## 开始直播

- 该直播间开始直播时触发

**CMD**：LIVE_OPEN_PLATFORM_LIVE_START

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 发生的直播间 |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| timestamp | int64 | 发生的时间戳 |
| area_name | string | 开播二级分区名称 |
| title | string | 开播时刻，直播间的标题 |

```json
{
    "cmd": "LIVE_OPEN_PLATFORM_LIVE_START",
    "data": {
        "area_name": "户外",
        "open_id": "useropenidabcdefghigklmnopqrstuvwxyz",
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "room_id": 22018648,
        "timestamp": 1732175513,
        "title": "标题"
    }
}
```

## 结束直播

- 该直播间停止直播时触发

**CMD**： LIVE_OPEN_PLATFORM_LIVE_END

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| room_id | int64 | 发生的直播间 |
| open_id | string | 用户唯一标识 |
| union_id | string | 用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通) |
| timestamp | int64 | 发生的时间戳 |
| area_name | string | 开播二级分区名称 |
| title | string | 开播时刻，直播间的标题 |

```json
{
    "cmd": "LIVE_OPEN_PLATFORM_LIVE_END",
    "data": {
        "area_name": "户外",
        "open_id": "useropenidabcdefghigklmnopqrstuvwxyz",
        "union_id":"U_05ad57b6655a44528cb95a892c491232",//用户在同一个开发者下的唯一标识(默认为空，根据业务需求单独申请开通)
        "room_id": 22018648,
        "timestamp": 1732175516,
        "title": "标题"
    }
}
```

## 消息推送结束通知

- 当目前长连接停止推送时触发

**当收到该CMD消息后则代表对应的game_id已失效，不会再收到消息推送。失效的原因一般是由于心跳过期或者主动调用END等错误，具体错误请开发者监听API和心跳返回信息。**

**CMD**：LIVE_OPEN_PLATFORM_INTERACTION_END

| 字段名 | 类型 | 描述 |
| --- | --- | --- |
| game_id | string | 结束消息推送的game_id |
| timestamp | int64 | 发生的时间戳 |

```json
{
    "cmd": "LIVE_OPEN_PLATFORM_INTERACTION_END",
    "data": {
        "game_id": "f0423922-60e9-4f8c-b5f3-6ad2cb888a22",
        "timestamp": 1714113037
    }
}
```
