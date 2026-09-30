# Bilibili 直播桥

一个用 Rust 实现的轻量级 Bilibili 直播事件桥。通过官方直播开放平台，用主播身份码开启互动场次、接收官方 WebSocket 推送，并将解析后的直播间事件分发给多个订阅者。

提供开放平台客户端、进程内会话管理和面向本机客户端的 WebSocket 服务；下游协议见 [客户端协议](docs/downstream-protocol.md)。

## 已实现功能

- **开放平台 API**：HTTP 请求签名、项目开启与关闭、单场心跳和批量心跳。
- **官方 WebSocket**：连接鉴权、心跳与 Ping/Pong、二进制包解析、zlib 解压和同帧多事件处理；建连时按顺序尝试平台返回的候选地址。
- **直播间事件**：弹幕、跨房弹幕、礼物、付费留言及下线、上舰、点赞、进房、开播、下播和互动结束。未识别的命令保留 `cmd` 与原始 `data`。
- **会话复用**：同一身份码的并发接入只开启一次场次；不同身份码关联同一房间时复用已有会话，共享一条官方长连接。
- **共享心跳**：一个会话管理器统一维护项目心跳，每批最多包含 200 个 `game_id`；WebSocket 心跳由各自的官方连接维护。
- **会话清理**：最后一个订阅者离开、互动结束、上游断开或心跳失败达到关闭条件时，尝试关闭官方场次；支持等待全部会话关闭。
- **下游 WebSocket**：每条 `/ws` 连接订阅一个房间，按序推送 JSON 事件；报告缓冲溢出，支持 Ping/Pong、读写限制和优雅退出。服务在最后一个客户端离开后保留上游 30 秒供重连。
- **结构化日志**：基于 tracing 记录 API 请求、连接与会话生命周期、心跳及清理失败；异步任务的日志携带房间和场次上下文。

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
| `BRIDGE_WEBSOCKET_HEARTBEAT_SECS` | 否 | `20` | 官方 WebSocket 心跳间隔，整数秒，范围 `1..=29` |
| `BRIDGE_APP_HEARTBEAT_SECS` | 否 | `20` | 项目心跳间隔，整数秒，范围 `1..=59` |
| `RUST_LOG` | 否 | `info` | 日志级别和模块过滤，由日志初始化读取 |
| `AUTH_CODE` | 示例程序需要 | 无 | 主播身份码，由示例程序单独读取，不属于 `Config` |

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
最后一个客户端离开后保留上游 30 秒，期间重连复用原场次，事件不补发。
Ctrl-C 或 Unix SIGTERM 会停止接入并等待清理。完整消息、超时和错误语义见 [客户端协议](docs/downstream-protocol.md)。

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

以下程序需要先设置上述环境变量，并会实际调用开放平台开启、维持和关闭场次：

| 命令 | 用途 |
| --- | --- |
| `cargo run --locked --bin multi_client` | 用同一身份码同时创建两个进程内订阅，演示会话复用、事件分发和共享项目心跳 |
| `cargo run --locked --bin ws_listen` | 开启一场互动，连接官方 WebSocket，打印事件并发送单场项目心跳 |
| `cargo run --locked --bin http_auth` | 开启场次，等待一个项目心跳间隔，发送一次心跳后关闭场次，用于验证 HTTP 鉴权链路 |

`multi_client` 和 `ws_listen` 可用 Ctrl-C 退出，程序随后尝试关闭场次。运行时需要显式指定 `--bin`，仓库包含多个二进制入口。

## 在代码中接入

库的入口为 `bilibili_live_bridge`，通常通过会话管理器使用：

1. 用 `config::Config::from_env()` 加载配置，再用 `session::Manager::from_config(&config)` 创建管理器。
2. 调用 `manager.attach(&auth_code).await` 获取 `Subscription`。克隆 `Manager` 会共享同一组会话。
3. 调用 `subscription.recv().await` 接收 `Arc<LiveEvent>`，通过事件变体处理数据，或用 `event.cmd()` 获取平台命令名。
4. 丢弃订阅表示离开；默认最后一个订阅者离开后立即关闭场次。通过 `Manager::with_options` 和 `ManagerOptions::idle_grace` 可以设置闲置宽限期。进程退出前调用 `manager.shutdown().await`，等待全部场次完成关闭调用。

订阅落后时，`recv()` 返回 `RecvError::Lagged { skipped }`；再次读取会从保留的事件继续，不会阻塞其他订阅者。会话结束且缓冲读完后返回 `RecvError::Closed`。

当前没有断线自动重连：上游断开会结束会话，需要重新调用 `attach`。如果直接使用 `open_live::api::Client` 和 `open_live::ws::Connection`，调用方需要自行维护项目心跳、持续调用 `Connection::recv()` 驱动 WebSocket 心跳，并在结束时调用 `Client::end()`。完整示例见 [`ws_listen`](src/bin/ws_listen.rs) 和 [`multi_client`](src/bin/multi_client.rs)。

[库入口的 rustdoc](src/lib.rs) 提供包含退出清理与订阅落后处理的接入示例，该示例由 doctest 检查编译。

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
| [`src/config.rs`](src/config.rs) | 环境变量配置与校验 |
| [`src/logging.rs`](src/logging.rs) | tracing 初始化与 `RUST_LOG` 过滤 |
| [`src/open_live/`](src/open_live/) | 开放平台 API、鉴权、错误码与官方 WebSocket 客户端 |
| [`src/session/`](src/session/) | 会话复用、订阅分发、批量心跳与生命周期管理 |
| [`src/downstream.rs`](src/downstream.rs) | 下游 WebSocket 服务和连接生命周期 |
| [`src/bin/`](src/bin/) | 示例程序与 WebSocket 服务入口 |
| [`docs/open-live/`](docs/open-live/README.md) | 收录的开放平台鉴权、应用 API、长链协议与事件命令文档 |

## 许可证

本项目以双协议授权，你可以任选其一使用：

- Apache License 2.0（[LICENCE-APACHE](LICENCE-APACHE)）
- MIT License（[LICENCE-MIT](LICENCE-MIT)）
