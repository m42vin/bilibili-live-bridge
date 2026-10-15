# Bilibili 直播桥

一个用 Rust 实现的轻量级 Bilibili 直播事件桥。通过官方直播开放平台，用主播身份码开启互动场次、接收官方 WebSocket 推送，并将解析后的直播间事件分发给多个订阅者。

提供开放平台客户端、进程内会话管理和面向本机客户端的 WebSocket 服务；下游协议见 [客户端协议](docs/downstream-protocol.md)。

## 已实现功能

- **开放平台 API**：HTTP 请求签名、项目开启与关闭、单场心跳和批量心跳。
- **官方 WebSocket**：连接鉴权、心跳与 Ping/Pong、二进制包解析、zlib 解压和同帧多事件处理；建连时按顺序尝试平台返回的候选地址。
- **直播间事件**：弹幕、跨房弹幕、礼物、付费留言及下线、上舰、点赞、进房、开播、下播和互动结束。未识别的命令保留 `cmd` 与原始 `data`。
- **会话复用**：同一身份码的并发接入只开启一次场次；不同身份码关联同一房间时复用已有会话，共享一条官方长连接。
- **共享心跳**：一个会话管理器统一维护项目心跳，每批最多包含 200 个 `game_id`，建连与鉴权期间也持续维护；WebSocket 心跳由各自的官方连接维护。
- **心跳有限恢复**：暂时性业务、传输和解码错误按成功时间限定恢复预算并退避重试；访问受限时降低频率，明确的鉴权、权限和参数错误立即关闭对应批次。
- **上游健康与恢复**：运行期间发送最多等待 5 秒，60 秒没有有效事件或官方心跳回复时判失活；默认在 120 秒内恢复原场次，保留订阅及项目心跳，并通知下游状态。
- **会话清理**：最后一个订阅者离开、互动结束、上游恢复终止或项目心跳达到关闭条件时，尝试关闭官方场次；支持等待全部会话关闭。
- **内存续传与 ACK**：可选确认模式使用共享有界事件日志、流标识和桥侧序号，支持慢消费追赶、断线补发和结束流尾部回放；超出保留范围明确报错。
- **下游 WebSocket**：每条 `/ws` 连接订阅一个房间，按序推送 JSON 事件；报告缓冲溢出，支持 Ping/Pong、读写限制和优雅退出。服务在最后一个客户端离开后保留上游 30 秒供重连。
- **结构化日志**：基于 tracing 记录 API 请求、连接与会话生命周期、心跳及清理失败；异步任务的日志携带房间和场次上下文。

## Workspace 结构

- `bridge-core`：请求签名、API 数据模型与响应解析、官方二进制协议和事件、下游 JSON 协议、交付模型，以及健康/心跳/重连的纯策略、失败分类、退避计算、恢复状态及接入/关闭生命周期。不依赖 Tokio、HTTP 客户端或系统网络。
- `bridge-native`：原生 HTTP / WebSocket、会话管理、心跳与恢复、内存日志、配置、日志初始化和所有可执行入口，单向依赖 `bridge-core`。

`bridge-native` 保留库名 `bilibili_live_bridge`，原公共类型路径通过重导出保持可用。Cargo 包名已从 `bilibili-live-bridge` 改为 `bridge-native`；外部项目的本地依赖改为：

```toml
[dependencies]
bilibili-live-bridge = { package = "bridge-native", path = "../bilibili-live-bridge/crates/bridge-native" }
```

在仓库根目录仍可执行 `cargo run --locked --bin server`。验证所有成员使用 `cargo test --locked --workspace`；仅验证纯核心使用 `cargo test --locked -p bridge-core`。
心跳、上游恢复和单会话接入/关闭决策已移入 core；native 保留索引、锁、通知、网络清理及调度。尚不包含 Workers 运行时或持久化日志。

## 快速开始

### 准备

- 安装 Rust stable 和 Cargo，项目使用 Rust 2024 edition。
- 准备直播开放平台的 Access Key Id、Access Key Secret 和项目 ID（`app_id`）。
- 准备主播身份码，示例程序通过 `AUTH_CODE` 环境变量读取。身份码用于开启或加入会话，房间号不能替代身份码。获取方式见[应用 API 文档](docs/open-live/app-api.md)。

在仓库根目录构建：

```sh
cargo build --locked
```

### 配置

复制配置模板，并填写三个必填项：

```sh
cp .env.example .env
```

```dotenv
BILIBILI_ACCESS_KEY_ID='你的 Access Key Id'
BILIBILI_ACCESS_KEY_SECRET='你的 Access Key Secret'
BILIBILI_APP_ID=1234567890123
```

`Config::from_env()` 只读取进程环境变量，当前不会自动加载 `.env`。在 Bash 或 Zsh 中，可以手动导出配置后再运行程序：

```sh
set -a
source .env
set +a
export AUTH_CODE='主播身份码'
cargo run --locked --bin multi_client
```

也可以直接通过 `export` 或进程启动环境设置这些变量。Access Key 用于桥接进程对开放平台签名；`.env` 已被 Git 忽略。

| 环境变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `BILIBILI_ACCESS_KEY_ID` | 是 | 无 | 开放平台 Access Key Id |
| `BILIBILI_ACCESS_KEY_SECRET` | 是 | 无 | 开放平台 Access Key Secret |
| `BILIBILI_APP_ID` | 是 | 无 | 项目 ID，必须是大于 0 的 `i64` 整数 |
| `BRIDGE_LISTEN` | 否 | `127.0.0.1:8080` | 下游 WebSocket 监听地址，格式为 IP 和端口 |
| `BRIDGE_MAX_CONNECTIONS` | 否 | `512` | 全部下游连接，含握手和等待订阅 |
| `BRIDGE_MAX_HANDSHAKES` | 否 | `32` | 正在执行下游 HTTP→WS 升级的连接 |
| `BRIDGE_MAX_PENDING_ATTACHES` | 否 | `64` | 等待接入结果的请求，含进程内和同码等待者 |
| `BRIDGE_MAX_STARTING_SESSIONS` | 否 | `4` | 平台开启、上游连接与鉴权任务，含失败清理 |
| `BRIDGE_MAX_SESSIONS` | 否 | `64` | 建立、活动及清理中的会话；结束日志不计入 |
| `BRIDGE_START_RATE_PER_SEC` | 否 | `1` | 实际 start 请求每秒补充配额，失败不退还 |
| `BRIDGE_START_BURST` | 否 | `2` | start 令牌桶突发容量 |
| `BRIDGE_MAX_SUBSCRIBERS_PER_ROOM` | 否 | `32` | 每房间活动订阅数，含进程内订阅 |
| `BRIDGE_CLIENT_MESSAGE_RATE_PER_SEC` | 否 | `1000` | 每连接入站业务消息补充速率，含 ACK |
| `BRIDGE_CLIENT_MESSAGE_BURST` | 否 | `2000` | 每连接业务消息突发容量；控制帧不消耗 |
| `BRIDGE_EVENT_RETENTION_SECS` | 否 | `300` | 内存事件与结束流保留时间，正整数秒 |
| `BRIDGE_EVENT_LOG_MAX_BYTES` | 否 | `33554432` | 全部流共享的事件日志字节预算，正整数；不代表进程 RSS 上限 |
| `BRIDGE_EVENT_LOG_MAX_STREAMS` | 否 | `128` | 活动与结束流数量上限，正整数 |
| `BRIDGE_ACK_WINDOW` | 否 | `64` | 每条确认模式连接最多未确认事件数，正整数 |
| `BRIDGE_WEBSOCKET_HEARTBEAT_SECS` | 否 | `20` | 官方 WebSocket 心跳间隔，整数秒，范围 `1..=29` |
| `BRIDGE_WEBSOCKET_WRITE_TIMEOUT_SECS` | 否 | `5` | 上游单次发送期限，包含准备和 flush |
| `BRIDGE_WEBSOCKET_ACTIVITY_TIMEOUT_SECS` | 否 | `60` | 最后有效事件或官方心跳回复后的存活期限；必须大于长连接心跳间隔加发送期限 |
| `BRIDGE_UPSTREAM_RECONNECT_TIMEOUT_SECS` | 否 | `120` | 原场次恢复总预算，包含旧连接释放、建连与退避；`0` 禁用重连 |
| `BRIDGE_UPSTREAM_RECONNECT_RETRY_INITIAL_MS` | 否 | `2000` | 上游恢复初始退避，毫秒，必须非零且不超过上限 |
| `BRIDGE_UPSTREAM_RECONNECT_RETRY_MAX_MS` | 否 | `10000` | 上游恢复最终退避上限，毫秒，含抖动 |
| `BRIDGE_APP_HEARTBEAT_SECS` | 否 | `20` | 项目心跳间隔，整数秒，范围 `1..=59` |
| `BRIDGE_APP_HEARTBEAT_FAILURE_TIMEOUT_SECS` | 否 | `45` | 从最后确认成功起算的恢复期限，正整数秒；首次失败后启用 |
| `BRIDGE_APP_HEARTBEAT_RETRY_INITIAL_MS` | 否 | `2000` | 普通重试初始退避，正整数毫秒 |
| `BRIDGE_APP_HEARTBEAT_RETRY_MAX_MS` | 否 | `10000` | 普通退避上限，正整数毫秒，含抖动 |
| `RUST_LOG` | 否 | `info` | 日志级别和模块过滤，由日志初始化读取 |
| `AUTH_CODE` | 示例程序需要 | 无 | 主播身份码，由示例程序单独读取，不属于 `Config` |

恢复期限默认 45 秒，依据收录的平台文档中最短 60 秒失效窗口，为响应延迟和安全余量预留 15 秒。
重试从 2 秒起指数增长，加入 ±20% 抖动，普通重试最长等待 10 秒；
`4009` 或 HTTP 429 的延迟至少为正常项目心跳间隔，`4009` 也可能涉及权限。
成功后回到原共享心跳周期；连续失败次数用于退避，关闭条件由时间预算替代原来的三次失败阈值。

管理器构造时，恢复期限超过 45 秒、心跳间隔加 10 秒请求超时不小于恢复期限、
或初始退避大于上限，会记录警告，保留原值并继续启动；退避仍按上限封顶。
自定义不安全组合无法保证在平台失效前恢复，首次失败时预算可能已经耗尽。
零值、格式错误或超出时钟范围仍返回配置错误。
这些恢复设置用于 `Manager`，低层 `Client` 的单次请求不自动重试。

### 启动 WebSocket 服务

导出三个平台配置变量后运行：

```sh
cargo run --locked --bin server
```

本机客户端连接 `ws://127.0.0.1:8080/ws`，在握手后发送：

```json
{"type":"subscribe","code":"主播身份码"}
```

接入成功后先收到 `ready`，再收到带 `cmd` 和 `data` 的 `event`。多条同码连接共享一场上游互动；
最后一个客户端离开后保留上游 30 秒，期间重连复用原场次；上述原实时模式不补发，确认模式可按游标续传。
Ctrl-C 或 Unix SIGTERM 会停止接入并等待清理。完整消息、超时和错误语义见 [客户端协议](docs/downstream-protocol.md)。

### 第一阶段资源保护

上述资源默认值用于工程试运行，不代表平台官方配额。全部值须在 `1..=u32::MAX` 内，
握手上限不能超过连接上限，开场上限不能超过会话上限。遇到资源不足立即拒绝，不排无界队列。
server 创建一份 `ResourceBudget` 注入 Manager；连接、Manager 克隆和进程内接入共享它。
库构造器也默认启用同一组限制；可在克隆或接入前通过 `with_resource_budget` 注入自定义预算。

同码复用已有会话不消耗 start 令牌或开场并发；未知身份码先通过准入，取得房间号后才合并。
失败的 start 也消耗频率额度；取消等待立即退还待接入名额，共享后台任务继续维护已开启场次。
会话名额在 end 等必要清理完成后归还；日志保留不延长该名额占用。
活动流续传仍需房间订阅名额，结束流回放只占连接及接入请求额度。

暂时错误为 `server_busy`、`rate_limited` 和 `subscription_limit`，可附带 `retry_after_ms`。
升级前尽力返回 HTTP 503 和 Retry-After；资源不足以回应时直接断开，不另起拒绝任务。
消息超额在 JSON 解析前结束连接；重复 ACK 可以接受，但仍计入消息频率。
`ResourceBudget::snapshot()` 提供当前五类名额占用、活动订阅、start 调用及分类拒绝累计数；
结构化日志以固定资源名记录取得、释放和拒绝，不包含身份码。

公网入口的代理、TLS、用户认证和按 IP 配额均不在此阶段；客户端只需主播身份码。
发送字节预算、历史回放限速和失败身份码冷却留待后续实现。

### 下游客户端示例

先在一个终端配置平台凭据并启动 `server`，再在另一个终端运行：

```sh
export AUTH_CODE='主播身份码'
cargo run --locked --bin downstream_client
```

客户端只需要身份码，无需平台 Access Key 或 `BILIBILI_APP_ID`，也不会自动读取 `.env`。
可通过 `BRIDGE_URL` 修改地址，默认是 `ws://127.0.0.1:8080/ws`；例如：

```sh
BRIDGE_URL='ws://127.0.0.1:8080/ws' RUST_LOG=info,downstream_client=debug cargo run --locked --bin downstream_client
```

[`downstream_client`](crates/bridge-native/src/bin/downstream_client.rs) 默认以 `ack:true` 订阅，处理 ready、事件、
上游 reconnecting / connected 状态和 resume_unavailable。网络接收和业务处理分开，
处理成功后更新进程内游标并发送累计 ACK，正文在 debug 日志中输出。

下游网络失败、关闭帧或 90 秒无入站活动时，按 2、4、8、10 秒基础退避加 0–20% 抖动重连，
带回最后连续处理完成的 `resume_after`；订阅保持 60 秒后，下次故障从 2 秒重新退避。
保留范围内补发桥已记录的事件，重复事件按流标识与序号去重，序号出现缺口时报错退出。
上游恢复时保持下游连接等待；上游没有到达桥的事件无法补发。

收到 closed 正常退出；server_busy、rate_limited、subscription_limit 和 HTTP 429/503 采用退避重试，
等待至少为基础退避与 retry_after_ms 建议的较大者，再增加抖动（远端建议最多采纳一天）。
缺少建议值也可重试；重试保留进程内已处理游标。attach_failed、invalid_request、续传不可用、
无效消息或其他 HTTP 4xx 握手拒绝时报错退出，不自动放弃游标重新开启场次。桥重启或日志过期后无法续传。
示例只在进程内保留游标，需要跨客户端重启恢复时应将业务结果和游标一起持久化。
Ctrl-C 可中断建连、事件等待和退避，关闭清理最多等待 2 秒。
握手和单次发送最多等待 10 秒。完整 ACK、范围和错误语义见 [客户端协议](docs/downstream-protocol.md)。

### 日志

所有二进制入口先调用 `logging::init()`，日志输出到 stderr，包含时间、级别、模块以及 span 上下文。默认使用 `info`，记录场次开启和关闭、会话接入与结束；连接候选地址失败、心跳异常和清理失败使用 `warn`，导致示例程序退出的错误使用 `error`。重定向到文件时自动关闭 ANSI 颜色：

```sh
cargo run --locked --bin multi_client 2>bridge.log
```

`RUST_LOG` 支持[EnvFilter 的级别与模块过滤语法](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)。例如，只查看警告及错误，或为桥和示例程序开启调试日志：

```sh
RUST_LOG=warn cargo run --locked --bin multi_client
RUST_LOG=info,bilibili_live_bridge=debug,multi_client=debug cargo run --locked --bin multi_client
```

`debug` 记录请求过程、事件转发，以及示例程序接收的事件详情；`trace` 还记录 WebSocket 心跳。库自身不记录完整事件正文。日志字段不包含 Access Key、身份码、签名、HTTP 请求正文或 WebSocket 鉴权正文；示例程序的事件详情可能包含弹幕等用户内容，按需开启。`RUST_LOG` 未设置或为空时使用 `info`，格式无效时启动返回错误。

作为库接入时，日志事件由调用方的 subscriber 收集。可以在程序入口调用 `bilibili_live_bridge::logging::init()?` 使用同一套配置，也可以安装自己的 subscriber。全局初始化每个进程只调用一次。

### 示例程序

以下上游示例需要先设置上述环境变量，并会实际调用开放平台开启、维持和关闭场次。
下游示例 `downstream_client` 的用法见上文：

| 命令 | 用途 |
| --- | --- |
| `cargo run --locked --bin multi_client` | 用同一身份码同时创建两个进程内订阅，演示会话复用、事件分发和共享项目心跳 |
| `cargo run --locked --bin ws_listen` | 开启一场互动，连接官方 WebSocket，打印事件并发送单场项目心跳 |
| `cargo run --locked --bin http_auth` | 开启场次，等待一个项目心跳间隔，发送一次心跳后关闭场次，用于验证 HTTP 鉴权链路 |

`multi_client` 和 `ws_listen` 可用 Ctrl-C 退出，程序随后尝试关闭场次。运行时需要显式指定 `--bin`，仓库包含多个二进制入口。
`http_auth` 在心跳失败时也尝试关闭场次；清理失败会记录日志，并保留原始心跳错误。

## 在代码中接入

库的入口为 `bilibili_live_bridge`，通常通过会话管理器使用：

1. 用 `config::Config::from_env()` 加载配置，再用 `session::Manager::from_config(&config)` 创建管理器。
2. 调用 `manager.attach(&auth_code).await` 获取 `Subscription`。克隆 `Manager` 会共享同一组会话。
3. 调用 `subscription.recv().await` 接收 `Arc<LiveEvent>`，通过事件变体处理数据，或用 `event.cmd()` 获取平台命令名。
4. 丢弃订阅表示离开；默认最后一个订阅者离开后立即关闭场次。通过 `Manager::with_options` 和 `ManagerOptions::idle_grace` 可以设置闲置宽限期。进程退出前调用 `manager.shutdown().await`，等待全部场次完成关闭调用。

`Manager::new`、`with_options` 和 `with_heartbeat_policy` 默认启用上游健康检查与原场次有限恢复；
`from_config` 和 `server` 使用环境配置。通过
`Manager::with_policies(api, websocket_heartbeat, app_heartbeat, options, heartbeat_policy, upstream_policy)`
定制两套策略，`UpstreamPolicy::reconnect = None` 禁用上游重连。
库的 `Manager::attach_delivery(code, resume_after)` 返回日志订阅，配合 `DeliveryPolicy` 和
`Manager::with_policies_and_delivery` 可定制内存保留范围；ACK 流量控制由下游服务实现。
`Subscription::recv()` 仍保持原实时广播语义。
`subscription.watch_status()` 返回 `UpstreamStatus` 的 watch 接收器，与直播事件独立，慢接收者可合并中间状态。

订阅落后时，`recv()` 返回 `RecvError::Lagged { skipped }`；再次读取会从保留的事件继续，不会阻塞其他订阅者。会话结束且缓冲读完后返回 `RecvError::Closed`。

上游断线或健康超时会有限恢复原 `game_id`，仍使用原鉴权信息和候选地址；恢复顺序从上次地址的下一个开始环形遍历，并先去掉重复地址（平台实测 5 项里浙江出现两次）。原鉴权复用已用 `auth_reauth` 连真实平台验证通过（见[开发文档](docs/development.md)），明确拒绝时结束订阅。
重连成功后连续健康一个存活周期（默认 60 秒）才清除旧恢复预算，短暂成功不能无限延长恢复。
恢复空窗不补发事件，也不跨连接去重；原场次失效、协议/业务解码错误、鉴权拒绝或预算耗尽后结束订阅，需要重新 `attach`。
下游新增 `status` 消息，状态为 `reconnecting` 或 `connected`，客户端需要接受该消息类型。
如果直接使用 `open_live::api::Client` 和 `open_live::ws::Connection`，调用方需要自行维护项目心跳、持续调用 `Connection::recv()` 驱动心跳和健康检查，自己处理重连，并在结束时调用 `Client::end()`。完整示例见 [`ws_listen`](crates/bridge-native/src/bin/ws_listen.rs) 和 [`multi_client`](crates/bridge-native/src/bin/multi_client.rs)。

[库入口的 rustdoc](crates/bridge-native/src/lib.rs) 提供包含退出清理与订阅落后处理的接入示例，该示例由 doctest 检查编译。

## 开发与文档

```sh
cargo test --locked
cargo fmt --check
cargo doc --no-deps --open
```

测试使用本地 HTTP / WebSocket 桩和模拟平台，覆盖配置校验、请求签名、包与事件解析、并发接入、会话复用、心跳和关闭流程，无需真实平台凭据。

| 路径 | 内容 |
| --- | --- |
| [架构说明](docs/architecture.md) | 模块边界、会话复用、心跳、事件分发与关闭流程 |
| [开发指南](docs/development.md) | 开发环境、测试与调试、常见扩展方式和注释维护 |
| [下游客户端协议](docs/downstream-protocol.md) | WebSocket 接入、消息封装、心跳、重连和关闭语义 |
| [`crates/bridge-native/src/config.rs`](crates/bridge-native/src/config.rs) | 环境变量配置与校验 |
| [`crates/bridge-native/src/logging.rs`](crates/bridge-native/src/logging.rs) | tracing 初始化与 `RUST_LOG` 过滤 |
| [`crates/bridge-native/src/open_live/`](crates/bridge-native/src/open_live/) | 开放平台 API、鉴权、错误码与官方 WebSocket 客户端 |
| [`crates/bridge-native/src/session/`](crates/bridge-native/src/session/) | 会话复用、订阅分发、批量心跳与生命周期管理 |
| [`crates/bridge-native/src/downstream.rs`](crates/bridge-native/src/downstream.rs) | 下游 WebSocket 服务和连接生命周期 |
| [`crates/bridge-native/src/bin/`](crates/bridge-native/src/bin/) | 示例程序与 WebSocket 服务入口 |
| [`docs/open-live/`](docs/open-live/README.md) | 收录的开放平台鉴权、应用 API、长链协议与事件命令文档 |

## 许可证

本项目以双协议授权，你可以任选其一使用：

- Apache License 2.0（[LICENCE-APACHE](LICENCE-APACHE)）
- MIT License（[LICENCE-MIT](LICENCE-MIT)）
