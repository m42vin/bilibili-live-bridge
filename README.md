# Bilibili 直播桥

一个用 Rust 实现的轻量级 Bilibili 直播事件桥。通过官方直播开放平台，用主播身份码开启互动场次、接收官方 WebSocket 推送，并将解析后的直播间事件分发给多个订阅者。

> 当前已实现开放平台客户端和进程内会话管理，可通过示例程序接收事件。`server` 仍是占位入口，尚未提供面向外部客户端的 WebSocket 服务与接入协议。

## 已实现功能

- **开放平台 API**：HTTP 请求签名、项目开启与关闭、单场心跳和批量心跳。
- **官方 WebSocket**：连接鉴权、心跳与 Ping/Pong、二进制包解析、zlib 解压和同帧多事件处理；建连时按顺序尝试平台返回的候选地址。
- **直播间事件**：弹幕、跨房弹幕、礼物、付费留言及下线、上舰、点赞、进房、开播、下播和互动结束。未识别的命令保留 `cmd` 与原始 `data`。
- **会话复用**：同一身份码的并发接入只开启一次场次；不同身份码关联同一房间时复用已有会话，共享一条官方长连接。
- **共享心跳**：一个会话管理器统一维护项目心跳，每批最多包含 200 个 `game_id`；WebSocket 心跳由各自的官方连接维护。
- **会话清理**：最后一个订阅者离开、互动结束、上游断开或心跳失败达到关闭条件时，尝试关闭官方场次；支持等待全部会话关闭。

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
| `BRIDGE_LISTEN` | 否 | `127.0.0.1:8080` | 预留的下游监听地址，格式为 IP 和端口；当前没有启动监听服务 |
| `BRIDGE_WEBSOCKET_HEARTBEAT_SECS` | 否 | `20` | 官方 WebSocket 心跳间隔，整数秒，范围 `1..=29` |
| `BRIDGE_APP_HEARTBEAT_SECS` | 否 | `20` | 项目心跳间隔，整数秒，范围 `1..=59` |
| `AUTH_CODE` | 示例程序需要 | 无 | 主播身份码，由示例程序单独读取，不属于 `Config` |

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
4. 丢弃订阅表示离开；最后一个订阅者离开后触发场次关闭。进程退出前调用 `manager.shutdown().await`，等待全部场次完成关闭调用。

订阅落后时，`recv()` 返回 `RecvError::Lagged { skipped }`；再次读取会从保留的事件继续，不会阻塞其他订阅者。会话结束且缓冲读完后返回 `RecvError::Closed`。

当前没有断线自动重连：上游断开会结束会话，需要重新调用 `attach`。如果直接使用 `open_live::api::Client` 和 `open_live::ws::Connection`，调用方需要自行维护项目心跳、持续调用 `Connection::recv()` 驱动 WebSocket 心跳，并在结束时调用 `Client::end()`。完整示例见 [`ws_listen`](src/bin/ws_listen.rs) 和 [`multi_client`](src/bin/multi_client.rs)。

## 开发与文档

```sh
cargo test --locked
cargo fmt --check
cargo doc --no-deps --open
```

测试使用本地 HTTP / WebSocket 桩和模拟平台，覆盖配置校验、请求签名、包与事件解析、并发接入、会话复用、心跳和关闭流程，无需真实平台凭据。

| 路径 | 内容 |
| --- | --- |
| [`src/config.rs`](src/config.rs) | 环境变量配置与校验 |
| [`src/open_live/`](src/open_live/) | 开放平台 API、鉴权、错误码与官方 WebSocket 客户端 |
| [`src/session/`](src/session/) | 会话复用、订阅分发、批量心跳与生命周期管理 |
| [`src/bin/`](src/bin/) | 示例程序与待实现的服务入口 |
| [`docs/open-live/`](docs/open-live/README.md) | 收录的开放平台鉴权、应用 API、长链协议与事件命令文档 |

## 许可证

本项目以双协议授权，你可以任选其一使用：

- Apache License 2.0（[LICENCE-APACHE](LICENCE-APACHE)）
- MIT License（[LICENCE-MIT](LICENCE-MIT)）
