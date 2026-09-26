# 哔哩哔哩直播开放平台

直播桥对开放平台保持 client 模式：服务端保存 Access Key，用主播身份码调用应用 API，并维护一条官方 WebSocket。这里收录实现这条链路所需要的鉴权、应用 API 与长连接协议。

原文集合：[开放平台-直播&互玩接入文档](https://open-live.bilibili.com/document/bdb1a8e5-a675-5bfe-41a9-7a7163f75dbf)。

| 文档 | 说明 |
| --- | --- |
| [鉴权](auth.md) | 统一鉴权与公共错误码。直播桥对开放平台的全部 HTTP 请求都走这套签名。 原文：[鉴权](https://open-live.bilibili.com/document/74eec767-e594-7ddd-6aba-257e8317c05d)。 |
| [应用 API](app-api.md) | 项目开启、关闭、心跳与批量心跳。直播桥用身份码换取一场 `game_id` 和官方长连接信息。 原文：[应用 API](https://open-live.bilibili.com/document/eba8e2e1-847d-e908-2e5c-7a1ec7d9266f)。 |
| [长链数据协议](websocket-protocol.md) | 官方 WebSocket 上的二进制包格式、鉴权包与心跳包。 原文：[长链数据协议](https://open-live.bilibili.com/document/657d8e34-f926-a133-16c0-300c1afc6e6b)。 |
| [长链命令](websocket-commands.md) | 长连接推送的直播间事件命令与字段。 原文：[长链命令](https://open-live.bilibili.com/document/f9ce25be-312e-1f4a-85fd-fef21f1637f8)。 |

## 调用顺序

1. 按 [鉴权](auth.md) 为每次 HTTP 请求签名。域名是 `https://live-open.biliapi.com`，方法一律为 POST，正文为 JSON。
2. 调用 [应用 API](app-api.md) 的 `/v2/app/start`，用身份码换取 `game_id`、`auth_body` 和 `wss_link`。
3. 按 [长链数据协议](websocket-protocol.md) 连接 `wss_link`，发送鉴权包，并同时维持 WebSocket 心跳与项目心跳。
4. 按 [长链命令](websocket-commands.md) 解析 `cmd`，把事件分发给使用同一身份码的下游客户端。
5. 场次结束时调用 `/v2/app/end`。
