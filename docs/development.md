# 开发指南

本文面向修改和验证项目的开发者。运行配置和示例程序见 [README](../README.md)，模块边界与会话约束见 [架构说明](architecture.md)，接口细节以源码 rustdoc 为准。

## 环境与首次验证

使用 Rust stable 与 Cargo，项目采用 Rust 2024 edition。仓库当前未声明最低支持的 Rust 版本，也未固定工具链；首次开发先确认本机工具链能构建锁定的依赖。

以下命令均在仓库根目录执行：

```sh
rustc --version
cargo --version
cargo build --locked --workspace
cargo test --locked --workspace
cargo fmt --all --check
RUSTDOCFLAGS="-D warnings -W missing_docs" cargo doc --locked --workspace --no-deps
```

`--locked` 用于避免检查过程更新 `Cargo.lock`。首次构建可能需要下载依赖；缓存齐全时可追加 `--offline`，或使用 `--frozen` 同时要求锁文件不变且不访问网络。

单元测试使用假凭据、模拟平台和本地 HTTP / WebSocket 桩，不需要配置真实 Access Key 或身份码。网络桩会监听 `127.0.0.1` 的动态端口，运行测试的环境需要允许回环监听。

`cargo run --locked --bin server` 启动下游 WebSocket 服务，联调方式见 [客户端协议](downstream-protocol.md)；
手动验证上游也可以使用 README 中的三个示例程序。
下游客户端使用 `cargo run --locked --bin downstream_client`，只需设置 `AUTH_CODE`，
可选 `BRIDGE_URL`；客户端行为测试使用本地 WebSocket 桩，无需真实平台。

单独检查核心及 Wasm 编译（需预先安装 `wasm32-unknown-unknown` target）：

```sh
cargo test --locked -p bridge-core
cargo check --locked -p bridge-core --target wasm32-unknown-unknown
cargo tree --locked -p bridge-core
```

核心禁止引入 Tokio、原生网络、系统时间与随机数生成；运行时提供这些能力。共享模型通过 native 重导出，新增协议、纯策略和恢复状态应在 core 使用显式时间测试，时钟范围、错误摘要适配、连接和任务行为在 native 测试。

## 从哪里开始修改

| 修改内容 | 实现位置 | 验证位置 |
| --- | --- | --- |
| 配置读取与校验 | [config.rs](../crates/bridge-native/src/config.rs) | [config_test.rs](../crates/bridge-native/src/config_test.rs) |
| 日志初始化与过滤 | [logging.rs](../crates/bridge-native/src/logging.rs) | [logging_test.rs](../crates/bridge-native/src/logging_test.rs) |
| HTTP API、响应与请求签名 | [api.rs](../crates/bridge-native/src/open_live/api.rs)、[auth.rs](../crates/bridge-core/src/open_live/auth.rs) | [api_test.rs](../crates/bridge-native/src/open_live/api_test.rs)、[auth_test.rs](../crates/bridge-core/src/open_live/auth_test.rs) |
| API 响应模型与校验 | [model.rs](../crates/bridge-core/src/open_live/model.rs) | [model_test.rs](../crates/bridge-core/src/open_live/model_test.rs) |
| 平台业务错误码 | [error.rs](../crates/bridge-core/src/open_live/error.rs) | [error_test.rs](../crates/bridge-core/src/open_live/error_test.rs) |
| 官方连接、鉴权和心跳 | [conn.rs](../crates/bridge-native/src/open_live/ws/conn.rs) | [conn_test.rs](../crates/bridge-native/src/open_live/ws/conn_test.rs) |
| 二进制包与压缩 | [packet.rs](../crates/bridge-core/src/open_live/ws/packet.rs) | [packet_test.rs](../crates/bridge-core/src/open_live/ws/packet_test.rs) |
| 直播事件字段与命令 | [cmd.rs](../crates/bridge-core/src/open_live/ws/cmd.rs) | [cmd_test.rs](../crates/bridge-core/src/open_live/ws/cmd_test.rs) |
| 纯接入/关闭生命周期 | [lifecycle.rs](../crates/bridge-core/src/session/lifecycle.rs) | [lifecycle_test.rs](../crates/bridge-core/src/session/lifecycle_test.rs) |
| 原生会话复用、订阅与清理 | [manager.rs](../crates/bridge-native/src/session/manager.rs)、[platform.rs](../crates/bridge-native/src/session/platform.rs) | [manager_test.rs](../crates/bridge-native/src/session/manager_test.rs) |
| 纯策略、失败分类与退避 | [policy.rs](../crates/bridge-core/src/session/policy.rs)、[failure.rs](../crates/bridge-core/src/session/failure.rs) | [policy_test.rs](../crates/bridge-core/src/session/policy_test.rs)、[failure_test.rs](../crates/bridge-core/src/session/failure_test.rs) |
| 纯恢复状态与截止时间 | [recovery.rs](../crates/bridge-core/src/session/recovery.rs) | [recovery_test.rs](../crates/bridge-core/src/session/recovery_test.rs) |
| 原生心跳调度与恢复预算 | [heartbeat.rs](../crates/bridge-native/src/session/heartbeat.rs) | [heartbeat_test.rs](../crates/bridge-native/src/session/heartbeat_test.rs)、[heartbeat_recovery_test.rs](../crates/bridge-native/src/session/heartbeat_recovery_test.rs) |
| 下游 WebSocket 与协议 | [downstream.rs](../crates/bridge-native/src/downstream.rs)、[protocol.rs](../crates/bridge-core/src/downstream/protocol.rs) | [protocol_test.rs](../crates/bridge-core/src/downstream/protocol_test.rs)、[downstream_test.rs](../crates/bridge-native/src/downstream/downstream_test.rs) |
| 内存事件日志与游标 | [delivery.rs](../crates/bridge-native/src/session/delivery.rs) | [delivery_test.rs](../crates/bridge-native/src/session/delivery_test.rs) |
| 下游客户端示例与重连 | [downstream_client.rs](../crates/bridge-native/src/bin/downstream_client.rs) | [client_test.rs](../crates/bridge-native/src/downstream/client_test.rs) |
| 示例程序 | [src/bin](../crates/bridge-native/src/bin/) | 构建检查；需要真实平台时手动运行 |

## 测试组织与调试

测试放在实现旁边的 `*_test.rs`，通过 `#[cfg(test)]` 和 `#[path = "..."]` 加载。内部测试可以访问私有实现，避免仅为测试扩大公共 API。

按模块筛选测试：

```sh
cargo test --locked session::manager::manager_test
cargo test --locked open_live::ws::packet::packet_test
cargo test --locked logging::logging_test
cargo test --locked --doc
```

`cargo test --locked` 同时运行默认目标测试与库的文档示例。`--all-targets` 不包含 doctest；如果使用它，还要单独运行 `--doc`。rustdoc 中的 `no_run` 示例会编译但不执行，适合需要真实凭据和网络的接入代码。

三类桩分别验证不同边界：

- HTTP 桩接收真实请求，检查路径、请求体和签名，再返回预设响应。
- WebSocket 桩完成本地握手，检查鉴权包、心跳、Ping/Pong、同帧事件和候选地址切换。
- 会话测试的 `Mock` 实现私有 `Platform` / `LiveSocket`，控制开启、连接、推送、心跳失败和结束时序。
- 下游测试同时启动本地 HTTP、官方 WebSocket 和下游监听器，验证真实握手、事件分发、协议错误和任务清理，以及 ACK 窗口、续传、缺口和结束流回放。
- 日志测试使用暂停时钟验证保留时间、全局容量、超大事件缺口、流淘汰和身份码归属；客户端桩验证处理后 ACK、重连游标、重复事件和缺口退出。

会话并发测试优先通过桩的通知或控制入口构造时序，再使用超时限制测试等待；避免用固定 sleep 猜测任务是否完成。项目心跳策略可以用测试专用的 `beat_once` 推进，不必等待真实周期。

下游 `delivery_interleaving` 回归测试使用仅测试编译的 ACK 快照钩子，固定推进确认和淘汰的交错，
验证旧快照不误报缺口、结束流全部确认后正常关闭，以及持续全局通知不饿死事件读取。
测试同时断言交错实际发生和输出事件序号，避免只检查未报错而漏掉交付失败。

配置测试通过 `from_reader` 注入变量，不修改进程的全局环境。日志测试使用 `logging_test::capture` 创建局部 dispatcher；异步测试需要把 dispatcher 绑定到被轮询的 future，避免跨 `await` 持有线程局部日志守卫。不要在并行测试中反复调用全局 `logging::init`。

真实平台调试按 README 设置配置和 `AUTH_CODE`，再运行：

```sh
RUST_LOG=info,bilibili_live_bridge=debug,multi_client=debug cargo run --locked --bin multi_client
RUST_LOG=info,bilibili_live_bridge=trace,ws_listen=debug cargo run --locked --bin ws_listen
RUST_LOG=info cargo run --locked --bin auth_reauth -- --wait-secs 60
```

`auth_reauth` 单独验证原场次的 `auth_body` 能否重新鉴权，即上游恢复的前提；它开启一场真实互动，
按 `wss_link` 顺序握手、释放、再用原鉴权重复握手，并在恢复后调用项目心跳确认场次存续。
`.env` 不会被自动加载，需要先 `set -a && . ./.env && set +a`。测试套件只覆盖它的参数与凭据校验
（[auth_reauth_cli.rs](../crates/bridge-native/tests/auth_reauth_cli.rs)），真实鉴权结果需要人工运行并记录。

使用房间号、场次 ID、请求路径和命令名关联日志。debug 可以看到示例程序的事件详情，trace 可以看到 WebSocket 心跳；真实运行会开启和关闭平台场次。

## 常见扩展方式

### 新增或调整直播事件

1. 对照 [长链命令资料](open-live/websocket-commands.md) 与实际脱敏样例，明确命令名、字段类型、单位及缺失语义。
2. 在 `cmd.rs` 增加或调整结构，补字段注释；按语义选择 `#[serde(default)]` 和 `Option`。默认值只处理字段缺失，普通数值或字符串字段的显式 JSON `null` 并不会因此成为默认值。
3. 同步修改命令常量、`LiveEvent` 变体、`cmd()`、`parse_command` 分派，以及 `ws.rs` 的公开导出。
4. 只有明确结束互动推送的事件才调整 `ends_push()`；下播事件和互动结束事件需要区分。
5. 在 `cmd_test.rs` 验证字段解析、缺失或无效输入及未知命令兼容。影响会话结束行为时，补相应会话测试。

已知结构当前会忽略未声明字段，未知命令保留原始 `data`。改变这项兼容策略时，同步更新架构说明和公共注释。

### 新增 API 或错误码

请求方法放在 `api.rs`，复用统一请求路径。请求体必须先序列化，再对同一份字符串签名并发送，避免签名内容与实际正文不同。

补请求和响应结构、参数校验及对应测试；HTTP 状态错误与平台业务错误需要分别处理。新增 `ErrorCode` 时同时更新 `from_raw`、`raw` 和往返测试，保留未知码的 `Other(i64)`。

批量心跳客户端会拒绝重复 ID，而不会自动去重；成功响应也可能包含失败场次。调整这些规则时需要同步检查会话心跳策略。

### 修改会话生命周期

先阅读架构说明中的 [状态与并发约束](architecture.md#状态与并发约束) 和 [关闭与失败处理](architecture.md#关闭与失败处理)。索引更新、订阅计数、停止信号与网络清理的顺序属于行为约束。纯状态修改放在 core 的 `session::lifecycle`，使用显式时间测试；native 负责在生命周期锁内应用状态并发布 watch 投影，在锁外执行网络清理。不要将 `begin_close` 与 `complete_cleanup` 合并，前者拒绝新接入，后者才允许同码重新开启及 shutdown 返回。

根据改动选择相应的行为验证：

- 同码并发只开启一次；跨码同房复用时清理重复场次。
- 最后一个订阅离开后按闲置宽限期关闭；重连取消旧截止时间，到期只清理一次；慢订阅者不阻塞其他订阅者。
- 互动结束先转发再终止；可恢复断线保留同一场次，终止错误和恢复预算耗尽清理一次。
- 项目心跳部分失败只影响对应场次；暂时错误按时间预算恢复，保持与上游恢复策略独立。
- 接入期间关闭管理器、建连失败、结束失败，以及同码再次接入与上一场清理的竞态。

使用现有模拟平台扩展必要的时序控制。不要让会话单元测试依赖真实平台；新增平台操作时同时更新生产适配器与测试实现。

### 修改配置或增加日志

配置项需要同步更新常量、解析与默认值、访问方法、`.env.example`、README 配置表和配置测试。`RUST_LOG` 属于日志初始化，示例用的 `AUTH_CODE` 不属于 `Config`。

新增日志时优先记录显式的项目、房间、场次、命令和错误字段。`#[tracing::instrument]` 使用 `skip_all`，再列出允许记录的字段，避免自动记录身份码、密钥、请求体或鉴权正文。

派生后台任务时按现有方式附加 span 和当前 subscriber，保持异步上下文；涉及敏感输入的改动，用捕获日志的测试验证不会输出这些输入。

## 注释与文档维护

- 模块注释说明职责与调用方责任，公共接口注释说明参数约束、错误和生命周期。
- 内部注释解释并发顺序、清理守卫、通知及兼容处理的理由，避免逐行重复代码。
- 接口细节放 rustdoc，使用步骤放 README，设计与行为边界放架构说明，开发方法放本文。
- 配置、公共 API 或行为变化与文档更新放在同一修改中。平台资料保留来源，核对上游内容时记录核对范围与日期。
- 需要外部资源的示例标为 `no_run`；能在本地运行的示例保持可执行，避免用 `ignore` 跳过编译检查。

生成可浏览的公共 API 文档：

```sh
cargo doc --locked --workspace --no-deps --open
```

查看内部实现的 rustdoc 时追加 `--document-private-items`。README 和本文中的文件链接均相对仓库定位，新增或移动文件时需检查链接。

提交前运行与改动相关的测试，并完成格式、默认测试和严格文档构建。对纯文档改动，重点确认链接与描述匹配当前实现；对新增 Rust 示例，还要确认 doctest 编译通过。

## 上游健康与恢复验证

健康期限测试使用暂停时钟及可控 Sink，覆盖发送准备/flush 阻塞、取消恢复和缓存事件不保活。
`upstream_recovery_test.rs` 使用平台桩验证原场次恢复、稳定窗口、预算、闲置、项目心跳及 shutdown。
本地 WebSocket 测试验证心跳回复、候选集群切换及下游 ready/status/event/closed 顺序；断线立即结束的旧测试显式禁用恢复。
真实平台验证用 `auth_reauth`，拒绝时结束订阅，不重新 start。

2026-10-01 对真实平台的运行结论（`--wait-secs 60`，单次运行）：

- 释放首条连接后，用原 `auth_body` 重新鉴权成功；平台没有把重新鉴权视为冲突。
- 重新鉴权后的连接在 60 秒观察期内保持打开且未收到事件，说明平台接受后不静默关闭；
  该观察长度等于默认 `activity_timeout`，更长的静默存活需要更大的观察窗口。
- 两次项目心跳（释放旧连接后、重新鉴权后）都成功，原场次在整个过程中保持存续。

`wss_link` 的实际结构（另一次请求记录，5 项、4 个不同地址）：

```text
0 wss://zj-cn-live-comet.chat.bilibili.com:443/sub   浙江
1 wss://zj-cn-live-comet.chat.bilibili.com:443/sub   重复
2 wss://bd-bj-live-comet-11.chat.bilibili.com:443/sub 北京
3 wss://bd-sz-live-comet-06.chat.bilibili.com:443/sub 深圳
4 wss://broadcastlv.chat.bilibili.com:443/sub        主入口
```

平台会重复同一个集群，且候选确实包含不同地域。恢复时的候选顺序因此在去重后计算偏移；
否则上次停在浙江（下标 0）时，偏移会落到下标 1 的重复项，让同一地址被连续尝试两次，
在 10 秒握手期限下白耗一份恢复预算。真实地址集下的完整跨集群故障转移仍需断网联调。

剩余的未验证部分：真实断线触发的完整恢复路径、平台真实不可用时跨集群故障转移的全过程、
以及静默存活超过一个 `activity_timeout` 的行为。

```sh
cargo test --locked health_test
cargo test --locked upstream_recovery
cargo clippy --locked --all-targets -- -D warnings
```
