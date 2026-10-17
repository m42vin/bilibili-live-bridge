# 运维观测

server 为单实例运行提供独立管理入口，默认监听 `127.0.0.1:9090`，通过 `BRIDGE_ADMIN_LISTEN` 配置。
业务 WebSocket 仍监听 `BRIDGE_LISTEN` 的 `/ws`，管理入口无需身份码，也不会消耗业务连接名额。
配置、反向代理与维护退出说明见 [README](../README.md)，协议见 [下游客户端协议](downstream-protocol.md)。

## 探针语义

| 请求 | 成功含义 | 维护退出期间 |
| --- | --- | --- |
| `GET /livez` | 管理入口能处理请求，返回 200 和 `ok` | 清理期间仍返回 200 |
| `GET /readyz` | 服务已初始化，业务监听已绑定，Manager 尚未禁止新接入 | 返回 503 和 `draining` |
| `GET /metrics` | 返回 Prometheus 0.0.4 文本，Content-Type 包含 `version=0.0.4` | 继续提供当前计数和清理中的占用 |

server 在两个监听器都成功绑定后才启动服务，管理地址绑定失败会启动失败。
退出时先停止业务接入，就绪立即失败；管理入口保留到业务收尾完成或退出预算耗尽。
进程已经结束后，探针连接失败也是失败结果，不能要求此时仍收到 HTTP 503。
管理监听失败或请求任务 panic 会触发业务维护退出；普通 HTTP 读写失败不使服务退出。

探针不主动调用 Bilibili，不验证平台凭据、特定房间或候选集群的可用性。
上游暂时断线、项目心跳恢复、业务名额暂满都不会使 `/readyz` 失败，避免把房间故障扩散为整个实例不可接入。
这些状态应通过下面的指标告警；liveness 不宜因为 Bilibili 不可用而自动重启进程。

管理入口只支持 GET，一次连接处理一个请求后关闭，不提供 keep-alive 或 HTTP 接入订阅。
最多 32 条并发连接、8 KiB 请求头、32 个头字段；读取和写入各最多两秒。并发满时直接关闭新连接，
不创建排队任务。未知路径返回 404，其他方法返回 405，无效或过大的头返回 400；超时可以直接断开。
业务反向代理只需转发 `/ws`；管理地址默认回环，可按部署环境改为运维网络可达地址。

## 指标与口径

指标均以 `bridge_` 开头，counter 在 Manager 重建或进程重启后归零，不持久化。
同一 Manager 的克隆共享故障统计；复用同一 ResourceBudget 的多个 Manager 会看到相同资源快照，不能重复求和。
直接使用 Client / Connection、绕过 Manager 的调用不包含在平台操作指标中。
各组件分别加锁采样，不保证整份响应是同一时刻的原子快照。

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `bridge_ready` | gauge | 与 ready 探针相同的接入状态，1 为可接入，0 为关闭中 |
| `bridge_resource_occupied{resource}` | gauge | 连接、握手、待接入、开场及会话名额的当前占用 |
| `bridge_resource_limit{resource}` | gauge | 上述五类名额的配置上限 |
| `bridge_resource_rejections_total{resource}` | counter | 十类资源拒绝累计次数 |
| `bridge_sending_bytes` | gauge | 序列化及待 flush JSON 字节预留，取消后保留到写端销毁 |
| `bridge_send_limit_bytes` / `bridge_connection_send_limit_bytes` | gauge | 共享发送及单连接单消息上限 |
| `bridge_replay_bytes_per_second` / `bridge_replay_burst_bytes` | gauge | 每连接历史回放字节速率与突发配置 |
| `bridge_failed_code_entries` / `bridge_failed_code_limit` | gauge | 失败摘要缓存实际条目数与总上限，条目包含在途预留和惰性清理前的过期记录 |
| `bridge_failed_code_cooldown_seconds` | gauge | start 失败后的同码冷却配置 |
| `bridge_subscribers` | gauge | 当前持有活动会话守卫的订阅数，含进程内调用方 |
| `bridge_start_requests_total` | counter | 已消耗额度的实际 start 调用数，包含失败请求 |
| `bridge_upstream_sessions{state}` | gauge | 已就绪且未开始关闭的上游，分别为 connected、reconnecting |
| `bridge_heartbeat_recovering_sessions` | gauge | 当前心跳登记中失败次数大于零的场次数 |
| `bridge_event_log_bytes` | gauge | 实际存储的事件 JSON、命令和记录开销计费字节 |
| `bridge_event_log_streams` / `bridge_event_log_events` | gauge | 当前保留的流数、事件数 |
| `bridge_event_log_limit_bytes` / `bridge_event_log_limit_streams` | gauge | 日志字节预算与流数上限 |
| `bridge_event_log_dropped_events_total` | counter | 因过期、容量、流移除或超大事件而移除或未保留的事件数 |
| `bridge_platform_operations_total{operation,outcome}` | counter | 平台操作完成或取消的结果分类次数 |
| `bridge_platform_operation_duration_seconds{operation}` | summary | 平台操作已等待的秒数，导出 `_count`、`_sum`，不导出分位数 |
| `bridge_failures_total{kind}` | counter | 关键运行时故障事件，固定类别见下表 |

`resource` 标签固定为 connection、handshake、pending_attach、starting_session、session、start_rate、subscriber、client_message、sending_bytes、failed_code。
前五项提供占用与并发上限，后几项的速率或每房额度仍通过 README 配置表管理。
日志字节不包含传输缓冲、广播、临时序列化和全部元数据，不代表进程 RSS 上限。
日志按原有访问路径惰性清理过期内容，指标采集不会触发淘汰，因此可能仍包含尚未清理的过期记录。
正常过期或已结束流回收也会增加 dropped counter，不能据此认定下游漏处理。

平台操作 `operation` 固定为 start、batch_heartbeat、end、connect、reconnect、socket_close。
结果 `outcome` 固定为 success、invalid、transport、timeout、decode、http、platform、protocol、cancelled。
connect/reconnect 包含整个候选连接与鉴权过程；batch_heartbeat 按一次批量请求计数，不按场次数计数。
批量响应成功但部分场次失效时，操作仍是 success，另增加 heartbeat_failed_game 故障计数。
取消包括停机、闲置、请求已无有效场次和外层恢复截止时间，不等同于故障；不能把所有非 success 都当作网络错误。
成功响应正文超限的 ResponseTooLarge 归入 decode，保留本地响应大小限制与平台业务错误的区别。
HTTP 客户端报告的超时记为 timeout；外层调度器或关闭期限取消的操作记为 cancelled，并在对应超时故障中呈现。

| `bridge_failures_total` 的 kind | 计数边界 |
| --- | --- |
| upstream_eof | 上游读取返回 EOF，不包含已解析的互动结束事件 |
| upstream_invalid / upstream_transport / upstream_decode / upstream_protocol | 上游读取返回对应错误类别 |
| socket_close_timeout | 上游关闭操作自身的五秒期限耗尽；更早的外层取消只记为 cancelled |
| heartbeat_timeout | 项目心跳调度器观察到请求超时 |
| heartbeat_failed_game | 成功批量响应中，对仍有效的场次应用失效决定 |
| downstream_io | 非维护取消的下游发送、Pong 超时或编码失败 |
| downstream_invalid | 下游无效订阅、业务消息或 ACK 导致连接结束 |
| downstream_handshake | 下游 WebSocket 握手失败或超时 |
| resume_unavailable | 下游因不能完整续传而结束，按连接终止计数，不统计重复内部校验 |
| shutdown_timeout | 服务退出总预算耗尽 |
| heartbeat_recovery_exhausted / upstream_recovery_exhausted | 对应恢复预算耗尽，终止场次；不包含主动维护取消 |
| downstream_oversized | 单条 JSON 超过发送上限，明确返回 message_too_large，不跳过事件或自动清除游标 |

故障计数与操作结果可以描述同一次异常的不同边界，不能直接相加作为“总失败数”。
end 操作的非成功结果用于观察清理失败；退出预算内完成仍不保证平台确认 end 成功。
指标不包含身份码、房间号、场次 ID、流 ID、候选地址、事件正文、原始错误或动态平台错误码。

## 采集与告警示例

Prometheus 与 bridge 同机运行时，可配置：

```yaml
scrape_configs:
  - job_name: bilibili-live-bridge
    metrics_path: /metrics
    static_configs:
      - targets: ['127.0.0.1:9090']
```

分开部署时将地址改为采集器可访问的管理监听地址；不要把采集器自己的回环地址当作 bridge 地址。
文本格式遵循 [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)。
可以从以下查询建立告警，并按实际容量与恢复预算选择持续时间和阈值：

```promql
# 采集失败与主动关闭分别呈现。
up{job="bilibili-live-bridge"} == 0
bridge_ready == 0

# 上游和项目心跳恢复，不作为实例存活失败。
bridge_upstream_sessions{state="reconnecting"} > 0
bridge_heartbeat_recovering_sessions > 0

# 资源拒绝、清理失败及退出预算耗尽。
sum by (resource) (rate(bridge_resource_rejections_total[5m])) > 0
sum(rate(bridge_platform_operations_total{operation="end",outcome!~"success|cancelled"}[5m])) > 0
increase(bridge_failures_total{kind="shutdown_timeout"}[1h]) > 0

# 所有已完成或取消 start 操作的平均等待秒数，非分位数。
rate(bridge_platform_operation_duration_seconds_sum{operation="start"}[5m])
  / rate(bridge_platform_operation_duration_seconds_count{operation="start"}[5m])
```

退出超时后进程会结束，最后一次 counter 增量可能来不及被拉取；退出状态与对应结构化日志仍需由进程托管收集。
这套指标不提供主机 CPU、RSS、文件描述符或跨进程聚合，主机资源观测由部署环境补充。
