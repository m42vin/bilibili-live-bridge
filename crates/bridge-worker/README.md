# bridge-worker

Cloudflare Workers / Durable Objects 最小运行时适配，单向依赖 `bridge-core`。
入口、Admission DO 和 Room DO 均用 Rust 编写，通过 `workers-rs 0.8.7` 编译为 WASM。

## 本阶段能力

- `POST /admit`：共享订阅校验、同身份码路由、短期一次性连接凭证。
- `GET /ws/:room_id?ticket=...`：Room DO 校验凭证，升级后无需重复 subscribe。
- 同码已有场次复用；异码映射同房时关闭重复场次，共享原上游连接。
- 平台请求签名、start / heartbeat / end；WSS 连接、二进制鉴权、协议解码和事件转发。
- 每场项目心跳、上游协议心跳、活动超时与应用层 ping/pong。
- 实时模式，以及 ACK 窗口和当前对象内的有界事件续传；原 native 客户端可直接使用。
- 场次所有权移交及清理责任持久化；客户端未使用凭证、最后一个连接离开后清理。

HTTP 接入成功只代表取得连接资格；上游鉴权成功之后才发送 ready。

## 开发与运行

在本目录执行（需要 Rust stable、WASM target、Node.js 22+）：

```sh
rustup target add wasm32-unknown-unknown
cargo install worker-build --version 0.8.7 --locked
npm ci
cp .dev.vars.example .dev.vars
# 编辑 .dev.vars，填写平台凭据和 BILIBILI_APP_ID
npm run dev
```

本地服务启动后，在仓库根目录使用已有客户端：

```sh
BRIDGE_URL=http://127.0.0.1:8787/admit AUTH_CODE='主播身份码' \
  cargo run --locked -p bridge-native --bin downstream_client
```

`GET /health` 仅检查入口可响应，不检测平台凭据或上游连通性。
`.dev.vars` 是本地秘密，不入库；正式环境通过 Wrangler secret 配置三个同名值。
配置平台凭据后，真实接入会调用 Bilibili start/end。

接入等待或失败时查看 Worker 终端：`platform request started/completed` 标记 start 请求，
`platform request failed` 包含路径、失败阶段、是否超时、HTTP 状态、平台业务错误码和耗时，
不输出平台 message、请求正文或凭据。`fetch` 表示仍在等待响应头，`response_body` 表示读取响应正文，
`platform_response` 表示解析或平台业务错误；平台请求最多等待 10 秒，客户端 HTTP 接入最多等待 30 秒。

```sh
npm run check     # 构建、绑定及部署包检查，不发布
# 配置好正式 secrets 后，才执行 npx wrangler deploy
```

生产入口使用 HTTPS。当前授权与 native 相同：持有身份码即可申请接入，没有额外用户认证。
若开放公网，需要在入口另行配置访问策略和全局限流。

## 架构与边界

入口只允许 /admit、/ws/:room_id 和 /health，不暴露 DO 内部控制接口。
Admission 按 `app_id + SHA256(code)` 命名；Room 按 `app_id + room_id` 命名。
只在内部绑定请求中传递上游鉴权数据，HTTP 成功正文仅返回带随机 UUID 凭证的 WebSocket 地址。

首次 start 的结果先登记为 Admission 的 pending 清理责任，Room 持久化场次后确认接管。
响应丢失时由 alarm 查询 Room 所有权；未接管的场次由 Admission end。
同码并发初始化中的请求返回 server_busy，客户端退避后复用，不重复 start。
未知码调用 start 后按房间归并。内部控制暂用 DO stub.fetch：workers-rs RPC 客户端仍需额外 JS bindings，
此阶段使用带固定路径的内部 JSON 协议，保持纯 Rust 构建。

Room 当前对象内持有上游 socket、订阅和日志。持久存储只保存清理责任，不承诺跨重启续传。
对象重建先尝试 end 已知旧场次并关闭旧下游，随后才能创建新会话；旧游标返回 stream_unavailable。
没有 shutdown hook，清理由持久 alarm 驱动。远端 start 成功但响应丢失时无法得知 game_id；
对此保留每码 60 秒开场冷却，最终依赖平台心跳失效回收，不能保证远端操作恰好一次。

采用 SQLite-backed DO，通过其 Storage API 保存少量元数据，不需要 D1/KV。
活动出站 WebSocket 不休眠，成本随房间活跃时长累积。

## 固定资源与时间设置

| 项目 | 当前值 |
| --- | --- |
| HTTP 请求正文 / 单条下游消息 | 8 KiB |
| 平台 HTTP 响应 / 上游二进制帧 | 1 MiB |
| 每房连接 / 未使用凭证 | 各 32 |
| 凭证有效期 / 无客户端宽限期 | 30 秒 |
| ACK 窗口上限 | 64 |
| 每房事件日志 | 256 条、1 MiB、最长 300 秒，任一先到先淘汰 |
| 同码 start 冷却 | 60 秒（成功或失败均消耗） |
| HTTP / 单个 WS 建连 / 鉴权等待 | 各 10 秒 |
| 建连候选数量 | 最多 3 个，连接失败尝试下一个；鉴权阶段失败终止 |
| 项目 / 上游心跳 | 20 秒，由 10 秒 alarm 检查到期任务 |
| 上游无有效事件或心跳回复 | 60 秒终止 |
| 应用层 ping / pong 期限 | alarm 周期探测 / 10 秒 |

内存日志只保证桥已收到的事件。游标超前、过期或流不存在明确失败，绝不自动新开场次。
结束事件在 ACK 模式下等待已发送尾部确认再发 closed；对象清理后结束流不再保留。
当前只保留一条房间流，无订阅关闭后的保留时间受 30 秒闲置清理限制，300 秒是活动日志的最长保留时间。
WebSocket send 是运行时排队操作，不具备 native 的 flush/发送超时保证；ACK 模式提供业务窗口约束。

尚未实现：自动上游重连、项目心跳退避恢复、跨对象批量心跳、项目级总配额、跨重启日志恢复、
历史流索引、下游发送字节预算。上游断线、鉴权失败、项目心跳失败会终止并尝试 end，不静默换场。

## 验证

仓库根目录：

```sh
cargo test --locked --workspace
cargo check --locked -p bridge-worker --target wasm32-unknown-unknown
cargo clippy --locked -p bridge-worker --target wasm32-unknown-unknown -- -D warnings
```

本目录 `npm test` 使用真实 workerd/Miniflare 执行生成的 WASM，在本机临时端口启动模拟 Bilibili。
它验证请求签名、接入、一次性凭证、上游鉴权、事件序号、ACK 窗口、续传、异码同房、双心跳、结束清理、未使用凭证回收、DO 重建和鉴权拒绝。
不读取 `.env`/`.dev.vars`，不使用真实凭据，不调用真实平台。
测试单独构建到 `build/test`，用编译时 `BRIDGE_WORKER_TEST_API_ORIGIN` 指向桩；正式构建不要设置该变量。

Wrangler 配置固定 `no_nodejs_global_timers`（worker::Delay 要求数字定时器句柄）和
`no_websocket_standard_binary_type`（SDK 读取 ArrayBuffer），升级 SDK/兼容日期时一起验证。
测试 runtime 与 Wrangler 配置统一使用兼容日期 `2026-10-06`，不超过锁定的 workerd 支持范围。
升级兼容日期时应同时升级相关运行时依赖，并验证 `npm test` 与 `npm run dev` 均可启动；
日期超过本地 workerd 上限会导致启动失败，`npm run check` 的 dry-run 不能替代运行时启动验证。
自动 invocation 日志关闭以避免记录含 ticket 的 URL；应用日志不输出身份码、平台鉴权或凭证。
