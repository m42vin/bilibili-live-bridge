//! 按房间复用一场官方互动，并把直播间事件分发给下游。

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{Notify, broadcast, watch};
use tokio::time::{Instant, sleep_until};
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use super::delivery::{DeliveryError, DeliveryPolicy, DeliverySubscription, EventCursor, EventLog};
use super::error::SessionError;
use super::platform::{LivePlatform, LiveSocket, Platform};
use super::recovery::NativeMoment;
use crate::config::Config;
use crate::open_live::api::{self, Anchor};
use crate::open_live::ws::LiveEvent;
pub use bridge_core::session::lifecycle::ManagerOptions;
use bridge_core::session::lifecycle::{
    AdmissionOwner, ClaimDecision, IdleAction, SessionLifecycle,
};
use bridge_core::session::recovery::HeartbeatRecovery;

#[path = "heartbeat.rs"]
mod heartbeat;
pub use heartbeat::HeartbeatPolicy;
#[path = "upstream.rs"]
mod upstream;
use heartbeat::run_heartbeats;
pub use upstream::{ReconnectPolicy, UpstreamPolicy, UpstreamStatus};

/// 每个生产会话的广播缓冲容量；慢订阅者溢出后跳过旧事件，不拖住其他订阅者。
const DEFAULT_EVENT_CAPACITY: usize = 256;
/// 尽力发送关闭帧的上限，不能让写端背压阻塞场次结束和本地清理。
const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// 直播桥上的全部会话。
///
/// [`Manager::attach`] 用身份码加入一场互动。同一个身份码，或 `start` 返回的同一个房间，
/// 共用一条官方长连接。`Clone` 共享这些会话。
/// 分别构造的管理器不会共享会话。析构管理器不等待清理，退出前需要调用
/// [`Manager::shutdown`] 并保持 Tokio runtime 存活。
///
/// 生产环境用 [`Manager::from_config`] 创建。那里会限制 WebSocket 心跳小于 30 秒、项目心跳小于 60 秒。
#[derive(Clone)]
pub struct Manager {
    state: Arc<State>,
}

impl Manager {
    /// 用已经构造好的应用 API 客户端创建会话管理器。
    ///
    /// `websocket_heartbeat` 传给官方长连接，`app_heartbeat` 是 `/v2/app/batchHeartbeat` 的间隔。
    /// 两个间隔都必须大于 0 且在时钟范围内；本构造方法不检查平台心跳上限。
    /// 项目心跳使用默认的 45 秒有限恢复策略；不安全的时间组合只警告。
    ///
    /// # Errors
    ///
    /// 任一间隔为零或超出时钟范围时返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn new(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
    ) -> Result<Self, SessionError> {
        Self::with_options(
            api,
            websocket_heartbeat,
            app_heartbeat,
            ManagerOptions::default(),
        )
    }

    /// 用应用 API 客户端、心跳间隔和生命周期选项创建会话管理器。
    ///
    /// 心跳间隔的约束与 [`Self::new`] 相同。闲置宽限期为零时立即清理。
    ///
    /// # Errors
    ///
    /// 心跳间隔为零，或间隔、闲置宽限期超出时钟范围时返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn with_options(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
        options: ManagerOptions,
    ) -> Result<Self, SessionError> {
        Self::with_heartbeat_policy(
            api,
            websocket_heartbeat,
            app_heartbeat,
            options,
            HeartbeatPolicy::default(),
        )
    }

    /// 用生命周期选项和项目心跳恢复策略创建管理器。
    ///
    /// 暂时性错误按最后成功时间限定恢复期限；明确的参数、鉴权和权限错误立即关闭本批。
    /// 时间组合不安全时仅警告，不修改配置或阻止启动；见 [`HeartbeatPolicy`]。
    ///
    /// ```no_run
    /// use bilibili_live_bridge::{
    ///     config::Config,
    ///     open_live::api::Client,
    ///     session::{HeartbeatPolicy, Manager, ManagerOptions},
    /// };
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = Config::from_env()?;
    /// let manager = Manager::with_heartbeat_policy(
    ///     Client::from_config(&config)?,
    ///     config.websocket_heartbeat(),
    ///     config.app_heartbeat(),
    ///     ManagerOptions::default(),
    ///     HeartbeatPolicy::default(),
    /// )?;
    /// # let _ = manager;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// 心跳间隔或策略时长为零，或间隔、策略时长、闲置宽限期超出时钟范围时，
    /// 返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn with_heartbeat_policy(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
        options: ManagerOptions,
        heartbeat_policy: HeartbeatPolicy,
    ) -> Result<Self, SessionError> {
        Self::with_policies(
            api,
            websocket_heartbeat,
            app_heartbeat,
            options,
            heartbeat_policy,
            UpstreamPolicy::default(),
        )
    }

    /// 用项目心跳和上游健康、恢复策略创建管理器。
    ///
    /// 默认启用原场次的有限重连；将 upstream_policy.reconnect 设为 None 可以禁用。
    ///
    /// # Errors
    ///
    /// 任一策略时长、期限组合或闲置宽限期无效时返回 SessionError::Invalid。
    pub fn with_policies(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
        options: ManagerOptions,
        heartbeat_policy: HeartbeatPolicy,
        upstream_policy: UpstreamPolicy,
    ) -> Result<Self, SessionError> {
        if websocket_heartbeat.is_zero() || app_heartbeat.is_zero() {
            return Err(SessionError::invalid("心跳间隔必须大于 0"));
        }
        if Instant::now().checked_add(options.idle_grace).is_none() {
            return Err(SessionError::invalid("闲置宽限期超出时钟范围"));
        }
        for every in [websocket_heartbeat, app_heartbeat] {
            if Instant::now().checked_add(every).is_none() {
                return Err(SessionError::invalid("心跳间隔超出时钟范围"));
            }
        }
        crate::policy::validate_heartbeat(heartbeat_policy).map_err(SessionError::invalid)?;
        crate::policy::warn_heartbeat(heartbeat_policy, app_heartbeat);
        crate::policy::validate_upstream(upstream_policy, websocket_heartbeat)
            .map_err(SessionError::invalid)?;
        let platform: Arc<dyn Platform> = Arc::new(LivePlatform::new(
            api,
            websocket_heartbeat,
            upstream_policy.health,
        ));
        Ok(Self::from_parts(
            platform,
            app_heartbeat,
            DEFAULT_EVENT_CAPACITY,
            options,
            heartbeat_policy,
            upstream_policy,
        ))
    }

    /// 使用显式日志与 ACK 策略创建管理器；日志在全部会话间共享容量。
    ///
    /// # Errors
    /// 心跳、恢复、闲置或日志策略无效时返回参数错误。
    pub fn with_policies_and_delivery(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
        options: ManagerOptions,
        heartbeat_policy: HeartbeatPolicy,
        upstream_policy: UpstreamPolicy,
        delivery_policy: DeliveryPolicy,
    ) -> Result<Self, SessionError> {
        delivery_policy.validate().map_err(SessionError::invalid)?;
        let mut manager = Self::with_policies(
            api,
            websocket_heartbeat,
            app_heartbeat,
            options,
            heartbeat_policy,
            upstream_policy,
        )?;
        Arc::get_mut(&mut manager.state)
            .expect("新管理器独占状态")
            .log = EventLog::new(delivery_policy);
        Ok(manager)
    }

    /// 开启 ACK 模式订阅，或按身份码与游标读取已有流（不会重新 start）。
    ///
    /// 首次接入从加入位置开始；续传要求身份码曾关联该流，且游标仍有效。
    /// 结束流保留期限内可补完尾部；进程重启后游标失效。
    ///
    /// # Errors
    /// 开启失败返回 Attach；日志、归属或位置不可用返回 Resume。
    pub async fn attach_delivery(
        &self,
        code: &str,
        after: Option<&EventCursor>,
    ) -> Result<DeliverySubscription, DeliveryError> {
        let code = normalize_code(code).map_err(DeliveryError::Attach)?;
        if self.is_shutting_down() {
            return Err(DeliveryError::Attach(SessionError::ShuttingDown));
        }
        if let Some(cursor) = after {
            let session = lock(&self.state.tables).by_code.get(code).cloned();
            let guard = session
                .filter(|s| s.stream_id == cursor.stream_id)
                .and_then(|s| s.try_subscribe())
                .map(|mut subscription| {
                    subscription.events = broadcast::channel(1).1;
                    subscription
                });
            if guard.is_some() {
                self.state.log.add_code(&cursor.stream_id, code);
            }
            self.state
                .log
                .reader(&cursor.stream_id, code, Some(cursor.seq), guard)
                .map_err(DeliveryError::Resume)
        } else {
            let mut subscription = self.attach(code).await.map_err(DeliveryError::Attach)?;
            // ACK 订阅只需生命周期计数，不保留未使用的旧广播接收端。
            subscription.events = broadcast::channel(1).1;
            let id = subscription.session.stream_id.clone();
            let base = subscription.delivery_base;
            self.state.log.add_code(&id, code);
            self.state
                .log
                .reader(&id, code, Some(base), Some(subscription))
                .map_err(DeliveryError::Resume)
        }
    }

    /// 用进程配置创建会话管理器。
    ///
    /// # Errors
    ///
    /// HTTP 客户端初始化失败或心跳间隔、恢复策略无效时返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn from_config(config: &Config) -> Result<Self, SessionError> {
        let api = api::Client::from_config(config)
            .map_err(|error| SessionError::invalid(error.to_string()))?;
        Self::with_policies_and_delivery(
            api,
            config.websocket_heartbeat(),
            config.app_heartbeat(),
            ManagerOptions::default(),
            config.heartbeat_policy(),
            config.upstream_policy(),
            config.delivery_policy(),
        )
    }

    /// 用身份码接入一场会话。
    ///
    /// 身份码前后的空白会去掉。同一个码的并发接入只会调用一次 `/v2/app/start`。
    /// 另一个码如果成功开启并返回同一个房间，会尝试关闭后一次场次，调用方改挂到已有的那场。
    /// 平台拒绝另一个码的开启时直接返回错误，不会根据房间索引绕过该错误。
    /// 取得场次后即维护项目心跳，包括建连与多地址鉴权期间；心跳失效会中止建连并清理场次。
    ///
    /// 返回的 [`Subscription`] 被丢弃时记一次离开。最后一个下游离开后，
    /// 等待 [`ManagerOptions::idle_grace`] 到期再触发 `/v2/app/end`；默认立即关闭。
    /// 同码上一场正在清理时等待其完成；接入返回 `Closed` 且未关闭管理器时额外尝试一次。
    /// 已运行会话断开后按上游策略恢复原场次；终止后需要调用方重新接入。
    ///
    /// # Errors
    ///
    /// 空身份码、管理器正在关闭、开启或建连失败，以及接入完成前会话结束时，
    /// 返回对应的 [`SessionError`]。
    #[must_use = "接入失败时需要处理 SessionError"]
    #[tracing::instrument(skip_all, err(Display, level = "warn"))]
    pub async fn attach(&self, code: &str) -> Result<Subscription, SessionError> {
        let code = normalize_code(code)?;
        let subscription = match self.state.attach_once(code).await {
            Err(SessionError::Closed) if !self.is_shutting_down() => {
                self.state.attach_once(code).await
            }
            other => other,
        }?;
        tracing::info!(
            room_id = subscription.room_id(),
            game_id = subscription.game_id(),
            "订阅已接入会话"
        );
        Ok(subscription)
    }

    /// 停止批量心跳，关闭所管理的会话，并等待本地清理流程完成。
    ///
    /// 返回之后，新的 [`Manager::attach`] 得到 [`SessionError::ShuttingDown`]。
    /// 可以再次调用；已经结束的会话不会再关闭一次。
    /// 正在接入的会话和跨身份码合并时的重复场次清理也纳入等待。
    /// 官方连接关闭最多等待 5 秒，随后释放连接并继续调用 `end`。
    /// 官方连接或 `end` 清理失败会记录日志，不从本方法返回，也不自动重试；
    /// 返回不保证平台已确认场次结束成功。调用方应等待此 future 完成再退出 runtime。
    /// 被取消的接入任务派生的尽力清理不纳入此处等待，应先协调并发接入任务结束。
    #[must_use = "丢弃后不会等待场次关闭"]
    pub async fn shutdown(&self) {
        self.state.shutdown().await;
    }

    /// 是否已经开始关闭，不再接受新的接入。
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.state.shutdown.load(Ordering::SeqCst)
    }

    fn from_parts(
        platform: Arc<dyn Platform>,
        app_heartbeat: Duration,
        event_capacity: usize,
        options: ManagerOptions,
        heartbeat_policy: HeartbeatPolicy,
        upstream_policy: UpstreamPolicy,
    ) -> Self {
        Self {
            state: Arc::new(State {
                platform,
                log: EventLog::new(DeliveryPolicy::default()),
                app_heartbeat,
                event_capacity,
                options,
                heartbeat_policy,
                upstream_policy,
                tables: Mutex::new(Tables {
                    by_code: HashMap::new(),
                    by_room: HashMap::new(),
                    sessions: Vec::new(),
                }),
                shutdown: AtomicBool::new(false),
                beats: Mutex::new(HashMap::new()),
                heartbeat_wake: Arc::new(Notify::new()),
                heartbeat_started: Mutex::new(false),
                heartbeat_stop: Stop::new(),
                heartbeat_finished: AtomicBool::new(false),
                heartbeat_done: Notify::new(),
            }),
        }
    }

    #[cfg(test)]
    pub(super) fn from_platform(
        platform: Arc<dyn Platform>,
        app_heartbeat: Duration,
        event_capacity: usize,
    ) -> Self {
        assert!(event_capacity > 0, "事件缓冲至少为 1");
        Self::from_parts(
            platform,
            app_heartbeat,
            event_capacity,
            ManagerOptions::default(),
            HeartbeatPolicy::default(),
            UpstreamPolicy {
                reconnect: None,
                ..UpstreamPolicy::default()
            },
        )
    }

    #[cfg(test)]
    pub(super) async fn beat_once(&self) {
        heartbeat::pump(&self.state).await;
    }
}

impl fmt::Debug for Manager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = lock(&self.state.tables);
        formatter
            .debug_struct("Manager")
            .field("codes", &tables.by_code.len())
            .field("rooms", &tables.by_room.len())
            .field("shutting_down", &self.is_shutting_down())
            .finish()
    }
}

/// 一场会话的进程内事件订阅。
///
/// 每个订阅独立读取，生产会话使用容量为 256 的广播缓冲。新订阅不回放加入前的事件。
/// 丢弃这个值表示该订阅者离开，但不会等待网络清理。不要用 [`std::mem::forget`] 跳过析构，
/// 否则订阅计数无法释放，场次可能持续存活，需要通过 [`Manager::shutdown`] 清理。
#[must_use = "丢弃订阅会在没有其他接入方时关闭这场会话"]
pub struct Subscription {
    room_id: i64,
    game_id: String,
    anchor: Anchor,
    events: broadcast::Receiver<Arc<LiveEvent>>,
    delivery_base: u64,
    session: Arc<Session>,
    active: bool,
}

impl Subscription {
    /// 这场互动的房间号。
    #[must_use]
    pub const fn room_id(&self) -> i64 {
        self.room_id
    }

    /// 这场互动的 `game_id`。
    #[must_use]
    pub fn game_id(&self) -> &str {
        &self.game_id
    }

    /// 授权这场互动的主播。
    #[must_use]
    pub const fn anchor(&self) -> &Anchor {
        &self.anchor
    }

    /// 订阅上游状态的最新快照；慢接收者可合并中间变化。
    #[must_use]
    pub fn watch_status(&self) -> watch::Receiver<UpstreamStatus> {
        self.session.upstream.subscribe()
    }

    /// 读取下一条直播间事件。
    ///
    /// 落后的订阅者收到 [`RecvError::Lagged`]，官方连接和其他下游不会被拖住。
    /// 再调用一次即可从保留下来的事件继续读。会话结束且缓冲读完后返回 [`RecvError::Closed`]。
    /// 被覆盖的事件不会补发；返回的 `Arc` 与其他订阅者共享同一份事件。
    ///
    /// # Errors
    ///
    /// 缓冲覆盖未读事件时返回 [`RecvError::Lagged`]；
    /// 所有发送端释放且缓冲读完时返回 [`RecvError::Closed`]。
    #[must_use = "读取失败时需要处理 RecvError"]
    pub async fn recv(&mut self) -> Result<Arc<LiveEvent>, RecvError> {
        match self.events.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(RecvError::Lagged { skipped }),
            Err(broadcast::error::RecvError::Closed) => Err(RecvError::Closed),
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.session.release();
        }
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Subscription")
            .field("room_id", &self.room_id)
            .field("game_id", &self.game_id)
            .finish()
    }
}

/// [`Subscription::recv`] 没有得到一条直播间事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    /// 订阅者落后于广播缓冲，中间跳过了 `skipped` 条。
    Lagged {
        /// 被挤掉的事件数。
        skipped: u64,
    },
    /// 这场会话的事件通道已关闭。
    Closed,
}

impl fmt::Display for RecvError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lagged { skipped } => {
                write!(formatter, "订阅落后，跳过了 {skipped} 条直播间事件")
            }
            Self::Closed => formatter.write_str("这场会话的事件通道已关闭"),
        }
    }
}

impl std::error::Error for RecvError {}

/// 同一个管理器及其克隆共享的索引、心跳登记和关闭信号。
struct State {
    log: Arc<EventLog>,
    platform: Arc<dyn Platform>,
    app_heartbeat: Duration,
    event_capacity: usize,
    options: ManagerOptions,
    heartbeat_policy: HeartbeatPolicy,
    upstream_policy: UpstreamPolicy,
    tables: Mutex<Tables>,
    shutdown: AtomicBool,
    beats: Mutex<HashMap<String, BeatSlot>>,
    heartbeat_wake: Arc<Notify>,
    heartbeat_started: Mutex<bool>,
    heartbeat_stop: Stop,
    heartbeat_finished: AtomicBool,
    heartbeat_done: Notify,
}

struct BeatSlot {
    session: Weak<Session>,
    recovery: HeartbeatRecovery<NativeMoment>,
    normal_due: Option<Instant>,
    in_flight: bool,
}

/// 两个索引可能指向同一会话；迁移与移除时需保持关联，并确认会话身份。
struct Tables {
    by_code: HashMap<String, Arc<Session>>,
    by_room: HashMap<i64, Arc<Session>>,
    /// 独立于复用索引的生命周期登记，保留正在清理的重复场次。
    sessions: Vec<Arc<Session>>,
}

impl State {
    async fn attach_once(self: &Arc<Self>, code: &str) -> Result<Subscription, SessionError> {
        let (session, leader) = self.claim(code).await?;
        if leader {
            self.lead(session, code).await
        } else {
            tracing::debug!("复用已有或正在建立的会话");
            follow(session).await
        }
    }

    async fn claim(self: &Arc<Self>, code: &str) -> Result<(Arc<Session>, bool), SessionError> {
        loop {
            let pending = {
                let mut tables = lock(&self.tables);
                if self.shutdown.load(Ordering::SeqCst) {
                    return Err(SessionError::ShuttingDown);
                }
                if let Some(existing) = tables.by_code.get(code).cloned() {
                    match existing.claim() {
                        ClaimDecision::Reuse => return Ok((existing, false)),
                        ClaimDecision::Replace => {
                            remove_session(&mut tables, code, &existing);
                            continue;
                        }
                        ClaimDecision::WaitForCleanup => Some(existing),
                    }
                } else {
                    let session = Session::starting(Arc::downgrade(self), self.options.idle_grace);
                    session.add_code(code.to_owned());
                    tables.by_code.insert(code.to_owned(), Arc::clone(&session));
                    tables.sessions.push(Arc::clone(&session));
                    return Ok((session, true));
                }
            };
            if let Some(existing) = pending {
                // 上一场还在 `/v2/app/end`。等它结束再 start，避免房间返回 7002。
                existing.until_finished().await;
            }
        }
    }

    async fn lead(
        self: &Arc<Self>,
        session: Arc<Session>,
        code: &str,
    ) -> Result<Subscription, SessionError> {
        let mut leader = Leader {
            session: Arc::clone(&session),
            platform: Arc::clone(&self.platform),
            owner: AdmissionOwner::default(),
        };
        let started = match self.platform.start(code).await {
            Ok(started) => started,
            Err(error) => {
                let error = SessionError::from_start(error);
                leader.owner.commit();
                session.fail(error.clone());
                return Err(error);
            }
        };
        leader.owner.acquired(started.game_id().to_owned());
        session.set_game_id(started.game_id().to_owned());
        tracing::debug!(
            room_id = started.anchor().room_id,
            game_id = %started.game_id(),
            "官方场次已开启，准备建立会话"
        );

        if self.stopping(&session) {
            return abort(&mut leader, &session, self.stop_error()).await;
        }

        let decision = self.decide_room(&session, code, started.anchor().room_id);
        match decision {
            RoomDecision::Abort => {
                return abort(&mut leader, &session, self.stop_error()).await;
            }
            RoomDecision::Join(existing) => {
                let game_id = leader
                    .owner
                    .game_id()
                    .map(str::to_owned)
                    .unwrap_or_else(|| started.game_id().to_owned());
                tracing::info!(
                    room_id = started.anchor().room_id,
                    %game_id,
                    "房间已有会话，关闭重复场次并复用"
                );
                if let Err(error) = self.platform.end(&game_id).await {
                    tracing::warn!(%game_id, %error, "关闭重复场次失败");
                }
                leader.owner.release_game();
                leader.owner.commit();
                if self.stopping(&session) {
                    let error = self.stop_error();
                    session.fail(error.clone());
                    return Err(error);
                }
                session.redirect(&existing);
                return follow(existing).await;
            }
            RoomDecision::Own => {}
        }

        if !self.register_beat(started.game_id(), &session) {
            return abort(&mut leader, &session, self.stop_error()).await;
        }
        // 建连和多地址鉴权期间也维持项目心跳；失效或 shutdown 时取消建连并清理已知场次。
        let connected = tokio::select! {
            biased;
            _ = session.stop.cancelled() => None,
            result = self.platform.connect(&started) => Some(result),
        };
        let socket = match connected {
            Some(Ok(socket)) => socket,
            Some(Err(error)) => {
                session.begin_close();
                let end = self.platform.end(started.game_id()).await;
                leader.owner.release_game();
                leader.owner.commit();
                let error = SessionError::from_connect(error, end);
                session.fail(error.clone());
                return Err(error);
            }
            None => return abort(&mut leader, &session, self.stop_error()).await,
        };

        if self.stopping(&session) {
            close_socket(socket, started.game_id(), started.anchor().room_id).await;
            return abort(&mut leader, &session, self.stop_error()).await;
        }

        let codes = lock(&session.codes).clone();
        if let Err(message) = self.log.register(
            &session.stream_id,
            started.anchor().room_id,
            started.game_id(),
            codes,
            session.upstream.subscribe(),
        ) {
            close_socket(socket, started.game_id(), started.anchor().room_id).await;
            return abort(&mut leader, &session, SessionError::invalid(message)).await;
        }

        let (events, _) = broadcast::channel(self.event_capacity);
        session.publish_ready(
            started.anchor().clone(),
            started.game_id().to_owned(),
            events.clone(),
        );
        let Some(subscription) = session.try_subscribe() else {
            close_socket(socket, started.game_id(), started.anchor().room_id).await;
            return abort(&mut leader, &session, self.stop_error()).await;
        };

        leader.owner.commit();
        leader.owner.release_game();
        spawn_upstream(
            Arc::clone(self),
            Arc::clone(&session),
            socket,
            events,
            started,
        );
        Ok(subscription)
    }

    fn decide_room(&self, session: &Arc<Session>, code: &str, room_id: i64) -> RoomDecision {
        let mut tables = lock(&self.tables);
        if self.shutdown.load(Ordering::SeqCst) || session.is_closing() {
            return RoomDecision::Abort;
        }
        if let Some(existing) = tables.by_room.get(&room_id).cloned()
            && !Arc::ptr_eq(&existing, session)
            && existing.is_joinable()
        {
            tables
                .by_code
                .insert(code.to_owned(), Arc::clone(&existing));
            existing.add_code(code.to_owned());
            self.log.add_code(&existing.stream_id, code);
            session.remove_code(code);
            return RoomDecision::Join(existing);
        }
        session.set_room(room_id);
        tables.by_room.insert(room_id, Arc::clone(session));
        RoomDecision::Own
    }

    fn stopping(&self, session: &Session) -> bool {
        self.shutdown.load(Ordering::SeqCst) || session.is_closing()
    }

    fn stop_error(&self) -> SessionError {
        if self.shutdown.load(Ordering::SeqCst) {
            SessionError::ShuttingDown
        } else {
            SessionError::Closed
        }
    }

    async fn shutdown(&self) {
        let sessions = {
            let tables = lock(&self.tables);
            self.shutdown.store(true, Ordering::SeqCst);
            snapshot(&tables)
        };
        tracing::info!(session_count = sessions.len(), "开始关闭会话管理器");
        // 先等待统一心跳任务停止，再发起本次主动关闭。
        self.stop_heartbeat().await;
        for session in &sessions {
            session.begin_close();
        }
        for session in &sessions {
            session.until_finished().await;
        }
        tracing::info!("会话管理器已关闭");
    }

    fn unlink(&self, session: &Arc<Session>) {
        let mut tables = lock(&self.tables);
        tables
            .sessions
            .retain(|current| !Arc::ptr_eq(current, session));
        if let Some(room_id) = session.room_id()
            && tables
                .by_room
                .get(&room_id)
                .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            tables.by_room.remove(&room_id);
        }
        let codes = lock(&session.codes).clone();
        for code in &codes {
            if tables
                .by_code
                .get(code)
                .is_some_and(|current| Arc::ptr_eq(current, session))
            {
                tables.by_code.remove(code);
            }
        }
    }

    fn register_beat(self: &Arc<Self>, game_id: &str, session: &Arc<Session>) -> bool {
        let lifecycle = lock(&session.lifecycle);
        if lifecycle.is_closing() || self.shutdown.load(Ordering::SeqCst) {
            return false;
        }
        lock(&self.beats).insert(
            game_id.to_owned(),
            BeatSlot {
                session: Arc::downgrade(session),
                recovery: HeartbeatRecovery::new(
                    self.heartbeat_policy,
                    self.app_heartbeat,
                    NativeMoment(Instant::now()),
                ),
                normal_due: None,
                in_flight: false,
            },
        );
        // 与 begin_close 的关闭标记串行，避免它移除心跳后又重新登记。
        drop(lifecycle);
        self.ensure_heartbeat();
        self.heartbeat_wake.notify_one();
        true
    }

    fn unregister_beat(&self, game_id: &str, session: &Session) {
        let mut beats = lock(&self.beats);
        if beats
            .get(game_id)
            .is_some_and(|slot| std::ptr::eq(slot.session.as_ptr(), session))
        {
            beats.remove(game_id);
        }
        drop(beats);
        self.heartbeat_wake.notify_one();
    }

    fn ensure_heartbeat(self: &Arc<Self>) {
        let mut started = lock(&self.heartbeat_started);
        if self.shutdown.load(Ordering::SeqCst) || *started {
            return;
        }
        *started = true;
        let weak = Arc::downgrade(self);
        let stop = self.heartbeat_stop.clone();
        let every = self.app_heartbeat;
        let span = tracing::info_span!(parent: None, "app_heartbeats", interval_secs = every.as_secs_f64());
        tokio::spawn(
            async move {
                tracing::debug!("批量项目心跳任务已启动");
                run_heartbeats(weak, stop, every).await;
                tracing::debug!("批量项目心跳任务已停止");
            }
            .instrument(span)
            .with_current_subscriber(),
        );
    }

    async fn stop_heartbeat(&self) {
        let started = {
            let started = lock(&self.heartbeat_started);
            self.heartbeat_stop.cancel();
            if *started {
                true
            } else {
                self.mark_heartbeat_finished();
                false
            }
        };
        if started {
            self.until_heartbeat_finished().await;
        }
    }

    fn mark_heartbeat_finished(&self) {
        self.heartbeat_finished.store(true, Ordering::SeqCst);
        self.heartbeat_done.notify_waiters();
    }

    async fn until_heartbeat_finished(&self) {
        let notified = self.heartbeat_done.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.heartbeat_finished.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

enum RoomDecision {
    Own,
    Join(Arc<Session>),
    Abort,
}

/// 首个接入的清理守卫。取消时只为已经取得 ID 的场次派生尽力清理任务。
/// 派生任务不加入会话完成等待，不能替代调用方协调接入任务和显式 shutdown。
struct Leader {
    session: Arc<Session>,
    platform: Arc<dyn Platform>,
    owner: AdmissionOwner,
}

impl Drop for Leader {
    fn drop(&mut self) {
        let Some(cancelled) = self.owner.cancel() else {
            return;
        };
        let game_id = cancelled.game_id;
        self.session.fail(SessionError::Closed);
        if let Some(game_id) = game_id {
            let platform = Arc::clone(&self.platform);
            let span = tracing::info_span!("session_cleanup", %game_id);
            tokio::spawn(
                async move {
                    tracing::info!("接入任务取消，关闭已开启场次");
                    if let Err(error) = platform.end(&game_id).await {
                        tracing::warn!(%game_id, %error, "接入取消后关闭场次失败");
                    }
                }
                .instrument(span)
                .with_current_subscriber(),
            );
        }
    }
}

async fn abort(
    leader: &mut Leader,
    session: &Arc<Session>,
    error: SessionError,
) -> Result<Subscription, SessionError> {
    session.begin_close();
    if let Some(game_id) = leader.owner.game_id().map(str::to_owned) {
        if let Err(error) = leader.platform.end(&game_id).await {
            tracing::warn!(%game_id, %error, "接入中止后关闭场次失败");
        }
        leader.owner.release_game();
    }
    leader.owner.commit();
    session.fail(error.clone());
    Err(error)
}

async fn follow(session: Arc<Session>) -> Result<Subscription, SessionError> {
    let mut session = session;
    for _ in 0..4 {
        let mut phase = session.phase.subscribe();
        loop {
            let action = {
                match &*phase.borrow() {
                    Phase::Ready { .. } => Action::Subscribe,
                    Phase::Failed(error) => Action::Fail(error.clone()),
                    Phase::Closed => Action::Fail(SessionError::Closed),
                    Phase::Attach(target) => match target.upgrade() {
                        Some(target) if !Arc::ptr_eq(&target, &session) => Action::Redirect(target),
                        _ => Action::Fail(SessionError::Closed),
                    },
                    Phase::Starting => Action::Wait,
                }
            };
            match action {
                Action::Subscribe => {
                    return session.try_subscribe().ok_or(SessionError::Closed);
                }
                Action::Fail(error) => return Err(error),
                Action::Redirect(target) => {
                    session = target;
                    break;
                }
                Action::Wait => {
                    if phase.changed().await.is_err() {
                        return Err(SessionError::Closed);
                    }
                }
            }
        }
    }
    Err(SessionError::Closed)
}

enum Action {
    Subscribe,
    Wait,
    Redirect(Arc<Session>),
    Fail(SessionError),
}

/// 取得所有权，确保超时取消关闭 future 后，在 end 请求前释放底层连接。
async fn close_socket(mut socket: Box<dyn LiveSocket>, game_id: &str, room_id: i64) {
    match tokio::time::timeout(SOCKET_CLOSE_TIMEOUT, socket.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(room_id, %game_id, %error, "关闭官方长连接失败");
        }
        Err(_) => {
            tracing::warn!(room_id, %game_id, "关闭官方长连接超时，释放连接并继续清理");
        }
    }
}

fn spawn_upstream(
    state: Arc<State>,
    session: Arc<Session>,
    socket: Box<dyn LiveSocket>,
    events: broadcast::Sender<Arc<LiveEvent>>,
    started: api::StartResult,
) {
    let span = tracing::info_span!(
        "session",
        room_id = started.anchor().room_id,
        game_id = %started.game_id()
    );
    tokio::spawn(
        upstream::run(state, session, socket, events, started)
            .instrument(span)
            .with_current_subscriber(),
    );
}

/// 被身份码和房间索引共享的会话。弱引用管理器，避免形成状态与会话的引用环。
struct Session {
    stream_id: String,
    state: Weak<State>,
    phase: watch::Sender<Phase>,
    upstream: watch::Sender<UpstreamStatus>,
    stop: Stop,
    lifecycle: Mutex<SessionLifecycle<NativeMoment>>,
    idle: watch::Sender<Option<Instant>>,
    codes: Mutex<Vec<String>>,
    room_id: Mutex<Option<i64>>,
    game_id: Mutex<Option<String>>,
    done: Notify,
}

/// 生命周期的通知投影，包含运行时资源；纯阶段及关闭决策以 lifecycle 为准。
#[derive(Clone)]
enum Phase {
    Starting,
    Ready {
        room_id: i64,
        game_id: String,
        anchor: Anchor,
        events: broadcast::Sender<Arc<LiveEvent>>,
    },
    Attach(Weak<Session>),
    Failed(SessionError),
    Closed,
}

impl Session {
    fn starting(state: Weak<State>, idle_grace: Duration) -> Arc<Self> {
        let (phase, _) = watch::channel(Phase::Starting);
        let (idle, _) = watch::channel(None);
        Arc::new(Self {
            state,
            phase,
            stream_id: uuid::Uuid::new_v4().to_string(),
            upstream: watch::channel(UpstreamStatus::Connected).0,
            stop: Stop::new(),
            lifecycle: Mutex::new(SessionLifecycle::starting(ManagerOptions { idle_grace })),
            idle,
            codes: Mutex::new(Vec::new()),
            room_id: Mutex::new(None),
            game_id: Mutex::new(None),
            done: Notify::new(),
        })
    }

    fn add_code(&self, code: String) {
        let mut codes = lock(&self.codes);
        if !codes.iter().any(|item| item == &code) {
            codes.push(code);
        }
    }

    fn remove_code(&self, code: &str) {
        lock(&self.codes).retain(|item| item != code);
    }

    fn set_room(&self, room_id: i64) {
        *lock(&self.room_id) = Some(room_id);
    }

    fn room_id(&self) -> Option<i64> {
        *lock(&self.room_id)
    }

    fn set_game_id(&self, game_id: String) {
        *lock(&self.game_id) = Some(game_id);
    }

    fn is_closing(&self) -> bool {
        let mut lifecycle = lock(&self.lifecycle);
        self.expire_idle_locked(&mut lifecycle);
        lifecycle.is_closing()
    }

    fn is_finished(&self) -> bool {
        lock(&self.lifecycle).is_finished()
    }

    fn claim(&self) -> ClaimDecision {
        let mut lifecycle = lock(&self.lifecycle);
        self.expire_idle_locked(&mut lifecycle);
        lifecycle.claim()
    }

    fn is_joinable(&self) -> bool {
        let mut lifecycle = lock(&self.lifecycle);
        self.expire_idle_locked(&mut lifecycle);
        lifecycle.is_joinable()
    }

    fn publish_ready(
        &self,
        anchor: Anchor,
        game_id: String,
        events: broadcast::Sender<Arc<LiveEvent>>,
    ) {
        let room_id = self.room_id().unwrap_or(anchor.room_id);
        let mut lifecycle = lock(&self.lifecycle);
        if !lifecycle.ready() {
            return;
        }
        self.phase.send_replace(Phase::Ready {
            room_id,
            game_id,
            anchor,
            events,
        });
    }

    fn try_subscribe(self: &Arc<Self>) -> Option<Subscription> {
        let (room_id, game_id, anchor, events) = {
            // 生命周期与通知投影在同一锁内发布；不能在关闭开始后增加订阅。
            let mut lifecycle = lock(&self.lifecycle);
            self.expire_idle_locked(&mut lifecycle);
            let ready = match &*self.phase.borrow() {
                Phase::Ready {
                    room_id,
                    game_id,
                    anchor,
                    events,
                } => Some((*room_id, game_id.clone(), anchor.clone(), events.clone())),
                _ => None,
            }?;
            if !lifecycle.subscribe(NativeMoment(Instant::now())) {
                if lifecycle.is_closing() {
                    self.idle.send_replace(None);
                    self.stop.cancel();
                }
                return None;
            }
            self.idle.send_replace(None);
            ready
        };
        let delivery_base = self
            .state
            .upgrade()
            .map_or(0, |state| state.log.tail(&self.stream_id));
        Some(Subscription {
            delivery_base,
            room_id,
            game_id,
            anchor,
            events: events.subscribe(),
            session: Arc::clone(self),
            active: true,
        })
    }

    fn release(&self) {
        let mut lifecycle = lock(&self.lifecycle);
        match lifecycle.release(NativeMoment(Instant::now())) {
            IdleAction::Unchanged => {}
            IdleAction::Schedule(deadline) => {
                self.idle.send_replace(Some(deadline.0));
            }
            IdleAction::Close => {
                self.idle.send_replace(None);
                self.stop.cancel();
            }
        }
    }

    fn expire_idle(&self) -> bool {
        self.expire_idle_locked(&mut lock(&self.lifecycle))
    }

    fn expire_idle_locked(&self, lifecycle: &mut SessionLifecycle<NativeMoment>) -> bool {
        if lifecycle.expire_idle(NativeMoment(Instant::now())) {
            self.idle.send_replace(None);
            self.stop.cancel();
            true
        } else {
            false
        }
    }

    fn begin_close(&self) {
        lock(&self.lifecycle).begin_close();
        self.stop.cancel();
        self.leave_heartbeat();
    }

    fn leave_heartbeat(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let Some(game_id) = self.game_id() else {
            return;
        };
        state.unregister_beat(&game_id, self);
    }

    fn game_id(&self) -> Option<String> {
        lock(&self.game_id).clone()
    }

    fn fail(self: &Arc<Self>, error: SessionError) {
        self.begin_close();
        if let Some(state) = self.state.upgrade() {
            state.log.finish(&self.stream_id);
            state.unlink(self);
        }
        {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.fail() {
                self.phase.send_replace(Phase::Failed(error));
            }
        }
        self.mark_finished();
    }

    fn redirect(self: &Arc<Self>, target: &Arc<Session>) {
        if let Some(state) = self.state.upgrade() {
            state.unlink(self);
        }
        {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.redirect() {
                self.phase
                    .send_replace(Phase::Attach(Arc::downgrade(target)));
            }
        }
        self.mark_finished();
    }

    fn publish_closed(&self) {
        let mut lifecycle = lock(&self.lifecycle);
        if lifecycle.close() {
            self.phase.send_replace(Phase::Closed);
        }
    }

    fn mark_finished(&self) {
        if *self.upstream.borrow() != UpstreamStatus::Closed {
            self.upstream.send_replace(UpstreamStatus::Closed);
        }
        if lock(&self.lifecycle).complete_cleanup() {
            self.done.notify_waiters();
        }
    }

    async fn until_finished(&self) {
        let notified = self.done.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.is_finished() {
            return;
        }
        notified.await;
    }
}

async fn until_idle(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[derive(Clone)]
struct Stop(Arc<StopInner>);

struct StopInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl Stop {
    fn new() -> Self {
        Self(Arc::new(StopInner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        self.0.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    async fn cancelled(&self) {
        let notified = self.0.notify.notified();
        let mut notified = std::pin::pin!(notified);
        // 先登记等待，再检查状态，避免 cancel 发生在检查与实际等待之间。
        notified.as_mut().enable();
        if self.0.cancelled.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

fn snapshot(tables: &Tables) -> Vec<Arc<Session>> {
    tables.sessions.clone()
}

fn remove_session(tables: &mut Tables, code: &str, session: &Arc<Session>) {
    tables.by_code.remove(code);
    if let Some(room_id) = session.room_id()
        && tables
            .by_room
            .get(&room_id)
            .is_some_and(|current| Arc::ptr_eq(current, session))
    {
        tables.by_room.remove(&room_id);
    }
}

fn normalize_code(code: &str) -> Result<&str, SessionError> {
    let code = code.trim();
    if code.is_empty() {
        return Err(SessionError::invalid("身份码不能为空"));
    }
    Ok(code)
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
#[path = "manager_test.rs"]
mod manager_test;
