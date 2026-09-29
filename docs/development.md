# 开发指南

本文面向修改和验证项目的开发者。运行配置和示例程序见 [README](../README.md)，模块边界与会话约束见 [架构说明](architecture.md)，接口细节以源码 rustdoc 为准。

## 环境与首次验证

使用 Rust stable 与 Cargo，项目采用 Rust 2024 edition。仓库当前未声明最低支持的 Rust 版本，也未固定工具链；首次开发先确认本机工具链能构建锁定的依赖。

以下命令均在仓库根目录执行：

```sh
rustc --version
cargo --version
cargo build --locked
cargo test --locked
cargo fmt --check
RUSTDOCFLAGS="-D warnings -W missing_docs" cargo doc --locked --no-deps
```

`--locked` 用于避免检查过程更新 `Cargo.lock`。首次构建可能需要下载依赖；缓存齐全时可追加 `--offline`，或使用 `--frozen` 同时要求锁文件不变且不访问网络。

单元测试使用假凭据、模拟平台和本地 HTTP / WebSocket 桩，不需要配置真实 Access Key 或身份码。网络桩会监听 `127.0.0.1` 的动态端口，运行测试的环境需要允许回环监听。

当前没有外部客户端服务可供联调。`cargo run --locked --bin server` 只初始化日志并提示入口未实现；手动验证上游使用 README 中的三个示例程序。

## 从哪里开始修改

| 修改内容 | 实现位置 | 验证位置 |
| --- | --- | --- |
| 配置读取与校验 | [config.rs](../src/config.rs) | [config_test.rs](../src/config_test.rs) |
| 日志初始化与过滤 | [logging.rs](../src/logging.rs) | [logging_test.rs](../src/logging_test.rs) |
| HTTP API、响应与请求签名 | [api.rs](../src/open_live/api.rs)、[auth.rs](../src/open_live/auth.rs) | [api_test.rs](../src/open_live/api_test.rs)、[auth_test.rs](../src/open_live/auth_test.rs) |
| 平台业务错误码 | [error.rs](../src/open_live/error.rs) | [error_test.rs](../src/open_live/error_test.rs) |
| 官方连接、鉴权和心跳 | [conn.rs](../src/open_live/ws/conn.rs) | [conn_test.rs](../src/open_live/ws/conn_test.rs) |
| 二进制包与压缩 | [packet.rs](../src/open_live/ws/packet.rs) | [packet_test.rs](../src/open_live/ws/packet_test.rs) |
| 直播事件字段与命令 | [cmd.rs](../src/open_live/ws/cmd.rs) | [cmd_test.rs](../src/open_live/ws/cmd_test.rs) |
| 会话复用、订阅与清理 | [manager.rs](../src/session/manager.rs)、[platform.rs](../src/session/platform.rs) | [manager_test.rs](../src/session/manager_test.rs) |
| 示例程序 | [src/bin](../src/bin/) | 构建检查；需要真实平台时手动运行 |

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

会话并发测试优先通过桩的通知或控制入口构造时序，再使用超时限制测试等待；避免用固定 sleep 猜测任务是否完成。项目心跳策略可以用测试专用的 `beat_once` 推进，不必等待真实周期。

配置测试通过 `from_reader` 注入变量，不修改进程的全局环境。日志测试使用 `logging_test::capture` 创建局部 dispatcher；异步测试需要把 dispatcher 绑定到被轮询的 future，避免跨 `await` 持有线程局部日志守卫。不要在并行测试中反复调用全局 `logging::init`。

真实平台调试按 README 设置配置和 `AUTH_CODE`，再运行：

```sh
RUST_LOG=info,bilibili_live_bridge=debug,multi_client=debug cargo run --locked --bin multi_client
RUST_LOG=info,bilibili_live_bridge=trace,ws_listen=debug cargo run --locked --bin ws_listen
```

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

先阅读架构说明中的 [状态与并发约束](architecture.md#状态与并发约束) 和 [关闭与失败处理](architecture.md#关闭与失败处理)。索引更新、订阅计数、停止信号与网络清理的顺序属于行为约束。

根据改动选择相应的行为验证：

- 同码并发只开启一次；跨码同房复用时清理重复场次。
- 最后一个订阅离开才开始关闭；慢订阅者不阻塞其他订阅者。
- 互动结束事件先转发，再结束会话；上游关闭和读取失败都清理场次。
- 心跳部分失败只影响对应场次；非业务错误按连续失败次数处理。
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
cargo doc --locked --no-deps --open
```

查看内部实现的 rustdoc 时追加 `--document-private-items`。README 和本文中的文件链接均相对仓库定位，新增或移动文件时需检查链接。

提交前运行与改动相关的测试，并完成格式、默认测试和严格文档构建。对纯文档改动，重点确认链接与描述匹配当前实现；对新增 Rust 示例，还要确认 doctest 编译通过。
