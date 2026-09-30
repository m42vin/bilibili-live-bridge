//! 按房间复用一场官方互动，并把直播间事件分发给下游。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{Notify, broadcast, watch};
use tokio::time::{Instant, MissedTickBehavior, interval, sleep_until};
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use super::error::SessionError;
use super::platform::{LivePlatform, LiveSocket, Platform};
use crate::config::Config;
use crate::open_live::api::{self, Anchor};
use crate::open_live::ws::LiveEvent;

/// 每个生产会话的广播缓冲容量；慢订阅者溢出后跳过旧事件，不拖住其他订阅者。
const DEFAULT_EVENT_CAPACITY: usize = 256;
/// 尽力发送关闭帧的上限，不能让写端背压阻塞场次结束和本地清理。
const SOCKET_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
/// 项目心跳连续非业务错误达到这个次数后关闭会话，包括传输、HTTP 状态和解码失败。
const HEARTBEAT_TRANSPORT_LIMIT: u8 = 3;

/// 会话生命周期选项。默认在最后一个订阅者离开后立即关闭。
#[derive(Debug, Clone, Copy, Default)]
pub struct ManagerOptions {
    /// 最后一个订阅者离开后保留上游的时间。
    ///
    /// 宽限期内继续接收事件和发送两类心跳；新订阅复用原场次，但不回放期间的事件。
    /// 上游结束、心跳失效和主动 shutdown 不等待这个时间。
    pub idle_grace: Duration,
}

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
    /// 两个间隔都必须大于 0；本构造方法不检查平台心跳上限。
    ///
    /// # Errors
    ///
    /// 任一间隔为零时返回 [`SessionError::Invalid`]。
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
    /// 心跳间隔为零或闲置宽限期超过时钟可表示的范围时返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn with_options(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
        options: ManagerOptions,
    ) -> Result<Self, SessionError> {
        if websocket_heartbeat.is_zero() || app_heartbeat.is_zero() {
            return Err(SessionError::invalid("心跳间隔必须大于 0"));
        }
        if Instant::now().checked_add(options.idle_grace).is_none() {
            return Err(SessionError::invalid("闲置宽限期超出时钟范围"));
        }
        let platform: Arc<dyn Platform> = Arc::new(LivePlatform::new(api, websocket_heartbeat));
        Ok(Self::from_parts(
            platform,
            app_heartbeat,
            DEFAULT_EVENT_CAPACITY,
            options,
        ))
    }

    /// 用进程配置创建会话管理器。
    ///
    /// # Errors
    ///
    /// HTTP 客户端初始化失败或心跳间隔无效时返回 [`SessionError::Invalid`]。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn from_config(config: &Config) -> Result<Self, SessionError> {
        let api = api::Client::from_config(config)
            .map_err(|error| SessionError::invalid(error.to_string()))?;
        Self::new(api, config.websocket_heartbeat(), config.app_heartbeat())
    }

    /// 用身份码接入一场会话。
    ///
    /// 身份码前后的空白会去掉。同一个码的并发接入只会调用一次 `/v2/app/start`。
    /// 另一个码如果成功开启并返回同一个房间，会尝试关闭后一次场次，调用方改挂到已有的那场。
    /// 平台拒绝另一个码的开启时直接返回错误，不会根据房间索引绕过该错误。
    ///
    /// 返回的 [`Subscription`] 被丢弃时记一次离开。最后一个下游离开后，
    /// 等待 [`ManagerOptions::idle_grace`] 到期再触发 `/v2/app/end`；默认立即关闭。
    /// 同码上一场正在清理时等待其完成；接入返回 `Closed` 且未关闭管理器时额外尝试一次。
    /// 已运行会话断开后不自动重连，需要调用方重新接入。
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
    ) -> Self {
        Self {
            state: Arc::new(State {
                platform,
                app_heartbeat,
                event_capacity,
                options,
                tables: Mutex::new(Tables {
                    by_code: HashMap::new(),
                    by_room: HashMap::new(),
                }),
                shutdown: AtomicBool::new(false),
                beats: Mutex::new(HashMap::new()),
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
        )
    }

    #[cfg(test)]
    pub(super) async fn beat_once(&self) {
        pump(&self.state).await;
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
    platform: Arc<dyn Platform>,
    app_heartbeat: Duration,
    event_capacity: usize,
    options: ManagerOptions,
    tables: Mutex<Tables>,
    shutdown: AtomicBool,
    beats: Mutex<HashMap<String, BeatSlot>>,
    heartbeat_started: Mutex<bool>,
    heartbeat_stop: Stop,
    heartbeat_finished: AtomicBool,
    heartbeat_done: Notify,
}

struct BeatSlot {
    session: Weak<Session>,
    transport_failures: u8,
}

/// 两个索引可能指向同一会话；迁移与移除时需保持关联，并确认会话身份。
struct Tables {
    by_code: HashMap<String, Arc<Session>>,
    by_room: HashMap<i64, Arc<Session>>,
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
                    if existing.is_joinable() {
                        return Ok((existing, false));
                    }
                    if existing.is_finished() {
                        remove_session(&mut tables, code, &existing);
                        continue;
                    }
                    Some(existing)
                } else {
                    let session = Session::starting(Arc::downgrade(self), self.options.idle_grace);
                    session.add_code(code.to_owned());
                    tables.by_code.insert(code.to_owned(), Arc::clone(&session));
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
            game_id: None,
            committed: false,
        };
        let started = match self.platform.start(code).await {
            Ok(started) => started,
            Err(error) => {
                let error = SessionError::from_start(error);
                leader.committed = true;
                session.fail(error.clone());
                return Err(error);
            }
        };
        leader.game_id = Some(started.game_id().to_owned());
        tracing::debug!(
            room_id = started.anchor().room_id,
            game_id = started.game_id(),
            "官方场次已开启，准备建立会话"
        );

        if self.stopping(&session) {
            return abort(&mut leader, &session, SessionError::ShuttingDown).await;
        }

        let decision = self.decide_room(&session, code, started.anchor().room_id);
        match decision {
            RoomDecision::Abort => {
                return abort(&mut leader, &session, SessionError::ShuttingDown).await;
            }
            RoomDecision::Join(existing) => {
                let game_id = leader
                    .game_id
                    .clone()
                    .unwrap_or_else(|| started.game_id().to_owned());
                tracing::info!(
                    room_id = started.anchor().room_id,
                    %game_id,
                    "房间已有会话，关闭重复场次并复用"
                );
                if let Err(error) = self.platform.end(&game_id).await {
                    tracing::warn!(%game_id, %error, "关闭重复场次失败");
                }
                leader.game_id = None;
                leader.committed = true;
                session.redirect(&existing);
                return follow(existing).await;
            }
            RoomDecision::Own => {}
        }

        let socket = match self.platform.connect(&started).await {
            Ok(socket) => socket,
            Err(error) => {
                let end = self.platform.end(started.game_id()).await;
                leader.game_id = None;
                leader.committed = true;
                let error = SessionError::from_connect(error, end);
                session.fail(error.clone());
                return Err(error);
            }
        };

        if self.stopping(&session) {
            close_socket(socket, started.game_id(), started.anchor().room_id).await;
            return abort(&mut leader, &session, SessionError::ShuttingDown).await;
        }

        let (events, _) = broadcast::channel(self.event_capacity);
        session.publish_ready(
            started.anchor().clone(),
            started.game_id().to_owned(),
            events.clone(),
        );
        let Some(subscription) = session.try_subscribe() else {
            close_socket(socket, started.game_id(), started.anchor().room_id).await;
            return abort(&mut leader, &session, SessionError::ShuttingDown).await;
        };

        leader.committed = true;
        leader.game_id = None;
        spawn_upstream(
            Arc::clone(self),
            Arc::clone(&session),
            socket,
            events,
            started.game_id().to_owned(),
            started.anchor().room_id,
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

    fn register_beat(self: &Arc<Self>, game_id: &str, session: &Arc<Session>) {
        lock(&self.beats).insert(
            game_id.to_owned(),
            BeatSlot {
                session: Arc::downgrade(session),
                transport_failures: 0,
            },
        );
        self.ensure_heartbeat();
    }

    fn unregister_beat(&self, game_id: &str) {
        lock(&self.beats).remove(game_id);
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
    game_id: Option<String>,
    committed: bool,
}

impl Drop for Leader {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.committed = true;
        let game_id = self.game_id.take();
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
    if let Some(game_id) = leader.game_id.clone() {
        if let Err(error) = leader.platform.end(&game_id).await {
            tracing::warn!(%game_id, %error, "接入中止后关闭场次失败");
        }
        leader.game_id = None;
    }
    leader.committed = true;
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
    mut socket: Box<dyn LiveSocket>,
    events: broadcast::Sender<Arc<LiveEvent>>,
    game_id: String,
    room_id: i64,
) {
    state.register_beat(&game_id, &session);
    let span = tracing::info_span!("session", room_id, %game_id);
    tokio::spawn(
        async move {
            tracing::info!("直播间会话已建立");
            let mut idle = session.idle.subscribe();
            // 宽限期由同一个上游任务驱动，避免派生的旧计时器关闭已经恢复的订阅。
            loop {
                let deadline = *idle.borrow_and_update();
                tokio::select! {
                    biased;
                    _ = session.stop.cancelled() => {
                        tracing::debug!("会话收到关闭信号");
                        break;
                    }
                    _ = idle.changed() => {}
                    _ = until_idle(deadline) => {
                        if session.expire_idle() {
                            tracing::info!("闲置宽限期已到，关闭会话");
                            break;
                        }
                    }
                    incoming = socket.recv() => {
                        match incoming {
                            Ok(Some(event)) => {
                                let ends = event.ends_push();
                                tracing::debug!(cmd = event.cmd(), "转发直播间事件");
                                // 无接收者是宽限期的正常状态；立即清理由 release 的停止信号驱动。
                                let _ = events.send(Arc::new(event));
                                if ends {
                                    tracing::info!("平台已停止互动推送，关闭会话");
                                    break;
                                }
                            }
                            Ok(None) => {
                                tracing::info!("官方长连接已关闭，结束会话");
                                break;
                            }
                            Err(error) => {
                                tracing::warn!(room_id, %game_id, %error, "官方长连接读取失败，结束会话");
                                break;
                            }
                        }
                    }
                }
            }

            session.begin_close();
            session.stop.cancel();
            close_socket(socket, &game_id, room_id).await;
            if let Err(error) = state.platform.end(&game_id).await {
                tracing::warn!(room_id, %game_id, %error, "关闭官方场次失败");
            }
            state.unlink(&session);
            // `send` 在没有接收者时会丢掉新值。会话创建时没有人订阅 phase，必须用 replace。
            session.phase.send_replace(Phase::Closed);
            drop(events);
            tracing::info!("直播间会话已结束");
            session.mark_finished();
        }
        .instrument(span)
        .with_current_subscriber(),
    );
}

/// 被身份码和房间索引共享的会话。弱引用管理器，避免形成状态与会话的引用环。
struct Session {
    state: Weak<State>,
    phase: watch::Sender<Phase>,
    stop: Stop,
    subscribers: Mutex<Subscribers>,
    idle_grace: Duration,
    idle: watch::Sender<Option<Instant>>,
    codes: Mutex<Vec<String>>,
    room_id: Mutex<Option<i64>>,
    finished: AtomicBool,
    done: Notify,
}

struct Subscribers {
    count: usize,
    closing: bool,
    idle_deadline: Option<Instant>,
}

/// 接入状态通过 watch 发布；正在关闭另由订阅计数中的 closing 标记表达。
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
            stop: Stop::new(),
            subscribers: Mutex::new(Subscribers {
                count: 0,
                closing: false,
                idle_deadline: None,
            }),
            idle_grace,
            idle,
            codes: Mutex::new(Vec::new()),
            room_id: Mutex::new(None),
            finished: AtomicBool::new(false),
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

    fn is_closing(&self) -> bool {
        let mut subscribers = lock(&self.subscribers);
        self.expire_idle_locked(&mut subscribers);
        subscribers.closing
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    fn is_joinable(&self) -> bool {
        if self.is_finished() || self.is_closing() {
            return false;
        }
        matches!(&*self.phase.borrow(), Phase::Starting | Phase::Ready { .. })
    }

    fn publish_ready(
        &self,
        anchor: Anchor,
        game_id: String,
        events: broadcast::Sender<Arc<LiveEvent>>,
    ) {
        let room_id = self.room_id().unwrap_or(anchor.room_id);
        self.phase.send_replace(Phase::Ready {
            room_id,
            game_id,
            anchor,
            events,
        });
    }

    fn try_subscribe(self: &Arc<Self>) -> Option<Subscription> {
        let (room_id, game_id, anchor, events) = {
            let phase = self.phase.borrow();
            match &*phase {
                Phase::Ready {
                    room_id,
                    game_id,
                    anchor,
                    events,
                } => Some((*room_id, game_id.clone(), anchor.clone(), events.clone())),
                _ => None,
            }
        }?;
        {
            // Ready 与正在关闭可以同时存在，必须在计数锁内再次确认能否加入。
            let mut subscribers = lock(&self.subscribers);
            self.expire_idle_locked(&mut subscribers);
            if subscribers.closing {
                return None;
            }
            subscribers.count += 1;
            subscribers.idle_deadline = None;
            self.idle.send_replace(None);
        }
        Some(Subscription {
            room_id,
            game_id,
            anchor,
            events: events.subscribe(),
            session: Arc::clone(self),
            active: true,
        })
    }

    fn release(&self) {
        let mut subscribers = lock(&self.subscribers);
        subscribers.count = subscribers.count.saturating_sub(1);
        if subscribers.count == 0 && !subscribers.closing {
            if self.idle_grace.is_zero() {
                subscribers.closing = true;
                self.stop.cancel();
            } else {
                let deadline = Instant::now() + self.idle_grace;
                subscribers.idle_deadline = Some(deadline);
                self.idle.send_replace(Some(deadline));
            }
        }
    }

    fn expire_idle(&self) -> bool {
        self.expire_idle_locked(&mut lock(&self.subscribers))
    }

    fn expire_idle_locked(&self, subscribers: &mut Subscribers) -> bool {
        if !subscribers.closing
            && subscribers.count == 0
            && subscribers
                .idle_deadline
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            subscribers.closing = true;
            subscribers.idle_deadline = None;
            self.idle.send_replace(None);
            self.stop.cancel();
            return true;
        }
        false
    }

    fn begin_close(&self) {
        lock(&self.subscribers).closing = true;
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
        state.unregister_beat(&game_id);
    }

    fn game_id(&self) -> Option<String> {
        match &*self.phase.borrow() {
            Phase::Ready { game_id, .. } => Some(game_id.clone()),
            _ => None,
        }
    }

    fn fail(self: &Arc<Self>, error: SessionError) {
        self.begin_close();
        if let Some(state) = self.state.upgrade() {
            state.unlink(self);
        }
        self.phase.send_replace(Phase::Failed(error));
        self.mark_finished();
    }

    fn redirect(&self, target: &Arc<Session>) {
        self.phase
            .send_replace(Phase::Attach(Arc::downgrade(target)));
        self.mark_finished();
    }

    fn mark_finished(&self) {
        self.finished.store(true, Ordering::SeqCst);
        self.done.notify_waiters();
    }

    async fn until_finished(&self) {
        let notified = self.done.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.finished.load(Ordering::SeqCst) {
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

#[derive(Clone)]
struct PendingBeat {
    game_id: String,
    session: Weak<Session>,
}

async fn run_heartbeats(state: Weak<State>, stop: Stop, every: Duration) {
    let mut ticker = interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut armed = false;
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => {
                finish_heartbeats(&state);
                return;
            }
            _ = ticker.tick() => {}
        }
        let Some(current) = state.upgrade() else {
            return;
        };
        if current.heartbeat_stop.is_cancelled() {
            current.mark_heartbeat_finished();
            return;
        }
        // 丢弃调度器第一次立即就绪的 tick；后来注册的场次参与下一轮统一心跳。
        if !armed {
            armed = true;
            continue;
        }
        pump(&current).await;
        if current.heartbeat_stop.is_cancelled() {
            current.mark_heartbeat_finished();
            return;
        }
    }
}

fn finish_heartbeats(state: &Weak<State>) {
    if let Some(state) = state.upgrade() {
        state.mark_heartbeat_finished();
    }
}

async fn pump(state: &State) {
    let pending: Vec<PendingBeat> = {
        let beats = lock(&state.beats);
        beats
            .iter()
            .map(|(game_id, slot)| PendingBeat {
                game_id: game_id.clone(),
                session: slot.session.clone(),
            })
            .collect()
    };
    if pending.is_empty() {
        return;
    }
    for chunk in pending.chunks(api::MAX_BATCH_HEARTBEAT) {
        let current: Vec<PendingBeat> = {
            let beats = lock(&state.beats);
            // 快照之后可能关闭或替换会话，旧批次不能作用于新的登记项。
            chunk
                .iter()
                .filter(|beat| {
                    beats
                        .get(&beat.game_id)
                        .is_some_and(|slot| Weak::ptr_eq(&slot.session, &beat.session))
                })
                .cloned()
                .collect()
        };
        if current.is_empty() {
            continue;
        }
        dispatch_chunk(state, &current).await;
    }
}

async fn dispatch_chunk(state: &State, chunk: &[PendingBeat]) {
    let game_ids: Vec<String> = chunk.iter().map(|beat| beat.game_id.clone()).collect();
    // Some 表示有确定的业务结果；None 表示非业务错误，按场次累积失败次数。
    let failed = match state.platform.batch_heartbeat(&game_ids).await {
        Ok(result) => {
            tracing::debug!(
                batch_size = game_ids.len(),
                failed_count = result.failed_game_ids().len(),
                "会话批量心跳完成"
            );
            Some(
                result
                    .failed_game_ids()
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>(),
            )
        }
        Err(error) => {
            tracing::warn!(batch_size = game_ids.len(), %error, "会话批量心跳失败");
            if error.platform_code().is_some() {
                Some(game_ids.iter().cloned().collect())
            } else {
                None
            }
        }
    };
    let mut closing = Vec::new();
    {
        let mut beats = lock(&state.beats);
        for beat in chunk {
            let Some(slot) = beats.get_mut(&beat.game_id) else {
                continue;
            };
            if !Weak::ptr_eq(&slot.session, &beat.session) {
                continue;
            }
            let close = match &failed {
                Some(failed) => failed.contains(&beat.game_id),
                None => {
                    slot.transport_failures = slot.transport_failures.saturating_add(1);
                    slot.transport_failures >= HEARTBEAT_TRANSPORT_LIMIT
                }
            };
            if close {
                closing.push((beat.game_id.clone(), beat.session.clone()));
            } else if failed.is_some() {
                slot.transport_failures = 0;
            }
        }
        for (game_id, _) in &closing {
            beats.remove(game_id);
        }
    }
    for (game_id, session) in closing {
        if let Some(session) = session.upgrade() {
            let reason = if failed.is_some() {
                "platform_rejected"
            } else {
                "transport_failure_limit"
            };
            tracing::warn!(
                %game_id,
                room_id = ?session.room_id(),
                reason,
                "项目心跳失效，关闭会话"
            );
            session.begin_close();
        }
    }
}

fn snapshot(tables: &Tables) -> Vec<Arc<Session>> {
    let mut seen = HashSet::<*const Session>::new();
    let mut sessions = Vec::new();
    for session in tables.by_code.values().chain(tables.by_room.values()) {
        if seen.insert(Arc::as_ptr(session)) {
            sessions.push(Arc::clone(session));
        }
    }
    sessions
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

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
#[path = "manager_test.rs"]
mod manager_test;
