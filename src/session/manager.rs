//! 按房间复用一场官方互动，并把直播间事件分发给下游。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::{Notify, broadcast, watch};
use tokio::time::{MissedTickBehavior, interval};

use super::error::SessionError;
use super::platform::{LivePlatform, LiveSocket, Platform};
use crate::config::Config;
use crate::open_live::api::{self, Anchor};
use crate::open_live::ws::LiveEvent;

const DEFAULT_EVENT_CAPACITY: usize = 256;
/// 项目心跳连续传输失败达到这个次数后关闭会话。20 秒间隔下大约覆盖平台 60 秒的超时。
const HEARTBEAT_TRANSPORT_LIMIT: u8 = 3;

/// 直播桥上的全部会话。
///
/// [`Manager::attach`] 用身份码加入一场互动。同一个身份码，或 `start` 返回的同一个房间，
/// 共用一条官方长连接。`Clone` 共享这些会话。
///
/// 生产环境用 [`Manager::from_config`] 创建。那里会限制 WebSocket 心跳小于 30 秒、项目心跳小于 60 秒。
#[derive(Clone)]
pub struct Manager {
    state: Arc<State>,
}

impl Manager {
    /// 用已经构造好的应用 API 客户端创建会话管理器。
    ///
    /// `websocket_heartbeat` 传给官方长连接，`app_heartbeat` 是 `/v2/app/heartbeat` 的间隔。
    /// 两个间隔都必须大于 0。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn new(
        api: api::Client,
        websocket_heartbeat: Duration,
        app_heartbeat: Duration,
    ) -> Result<Self, SessionError> {
        if websocket_heartbeat.is_zero() || app_heartbeat.is_zero() {
            return Err(SessionError::invalid("心跳间隔必须大于 0"));
        }
        let platform: Arc<dyn Platform> = Arc::new(LivePlatform::new(api, websocket_heartbeat));
        Ok(Self::from_parts(
            platform,
            app_heartbeat,
            DEFAULT_EVENT_CAPACITY,
        ))
    }

    /// 用进程配置创建会话管理器。
    #[must_use = "创建失败时需要处理 SessionError"]
    pub fn from_config(config: &Config) -> Result<Self, SessionError> {
        let api = api::Client::from_config(config)
            .map_err(|error| SessionError::invalid(error.to_string()))?;
        Self::new(api, config.websocket_heartbeat(), config.app_heartbeat())
    }

    /// 用身份码接入一场会话。
    ///
    /// 身份码前后的空白会去掉。同一个码的并发接入只会调用一次 `/v2/app/start`。
    /// 另一个码如果开启了同一个房间，后一次开启会被关掉，调用方改挂到已有的那场。
    ///
    /// 返回的 [`Subscription`] 被丢弃时记一次离开。最后一个离开的下游会触发 `/v2/app/end`。
    #[must_use = "接入失败时需要处理 SessionError"]
    pub async fn attach(&self, code: &str) -> Result<Subscription, SessionError> {
        let code = normalize_code(code)?;
        match self.state.attach_once(code).await {
            Err(SessionError::Closed) if !self.is_shutting_down() => {
                self.state.attach_once(code).await
            }
            other => other,
        }
    }

    /// 关闭全部会话，并等待每一场都调用完 `/v2/app/end`。
    ///
    /// 返回之后，新的 [`Manager::attach`] 得到 [`SessionError::ShuttingDown`]。
    /// 可以再次调用；已经结束的会话不会再关闭一次。
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
    ) -> Self {
        Self {
            state: Arc::new(State {
                platform,
                app_heartbeat,
                event_capacity,
                tables: Mutex::new(Tables {
                    by_code: HashMap::new(),
                    by_room: HashMap::new(),
                }),
                shutdown: AtomicBool::new(false),
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
        Self::from_parts(platform, app_heartbeat, event_capacity)
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

/// 下游收到的一场会话。
///
/// 丢弃这个值表示该下游离开。不要用 [`std::mem::forget`] 丢掉它，否则这场会话会计数泄漏，
/// 官方侧会一直保持到心跳超时。
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

struct State {
    platform: Arc<dyn Platform>,
    app_heartbeat: Duration,
    event_capacity: usize,
    tables: Mutex<Tables>,
    shutdown: AtomicBool,
}

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
                    let session = Session::starting(Arc::downgrade(self));
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
                let _ = self.platform.end(&game_id).await;
                leader.game_id = None;
                leader.committed = true;
                session.redirect(&existing);
                return follow(existing).await;
            }
            RoomDecision::Own => {}
        }

        let mut socket = match self.platform.connect(&started).await {
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
            let _ = socket.close().await;
            return abort(&mut leader, &session, SessionError::ShuttingDown).await;
        }

        let (events, _) = broadcast::channel(self.event_capacity);
        session.publish_ready(
            started.anchor().clone(),
            started.game_id().to_owned(),
            events.clone(),
        );
        let Some(subscription) = session.try_subscribe() else {
            let _ = socket.close().await;
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
        for session in &sessions {
            session.begin_close();
        }
        for session in &sessions {
            session.until_finished().await;
        }
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
}

enum RoomDecision {
    Own,
    Join(Arc<Session>),
    Abort,
}

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
            tokio::spawn(async move {
                let _ = platform.end(&game_id).await;
            });
        }
    }
}

async fn abort(
    leader: &mut Leader,
    session: &Arc<Session>,
    error: SessionError,
) -> Result<Subscription, SessionError> {
    if let Some(game_id) = leader.game_id.clone() {
        let _ = leader.platform.end(&game_id).await;
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

fn spawn_upstream(
    state: Arc<State>,
    session: Arc<Session>,
    mut socket: Box<dyn LiveSocket>,
    events: broadcast::Sender<Arc<LiveEvent>>,
    game_id: String,
) {
    let app_heartbeat = state.app_heartbeat;
    let stop = session.stop.clone();
    let heartbeat_state = Arc::clone(&state);
    let heartbeat_game = game_id.clone();
    tokio::spawn(async move {
        // 项目心跳放在另一边，避免每 20 秒取消一次 `Connection::recv`。
        // `recv` 只在会话停止时被打断，此时连接马上就要关掉。
        let heartbeat = tokio::spawn(beat(heartbeat_state, stop, heartbeat_game, app_heartbeat));

        loop {
            tokio::select! {
                biased;
                _ = session.stop.cancelled() => break,
                incoming = socket.recv() => {
                    match incoming {
                        Ok(Some(event)) => {
                            let ends = event.ends_push();
                            if events.send(Arc::new(event)).is_err() || ends {
                                break;
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
        }

        session.begin_close();
        session.stop.cancel();
        let _ = heartbeat.await;
        let _ = socket.close().await;
        let _ = state.platform.end(&game_id).await;
        state.unlink(&session);
        // `send` 在没有接收者时会丢掉新值。会话创建时没有人订阅 phase，必须用 replace。
        session.phase.send_replace(Phase::Closed);
        drop(events);
        session.mark_finished();
    });
}

async fn beat(state: Arc<State>, stop: Stop, game_id: String, every: Duration) {
    let mut ticker = interval(every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // `start` 刚刚成功，第一次项目心跳等一个完整间隔。
    ticker.tick().await;
    let mut transport_failures = 0u8;
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = ticker.tick() => {
                match state.platform.heartbeat(&game_id).await {
                    Ok(()) => transport_failures = 0,
                    Err(error) if error.platform_code().is_some() => {
                        stop.cancel();
                        return;
                    }
                    Err(_) => {
                        transport_failures = transport_failures.saturating_add(1);
                        if transport_failures >= HEARTBEAT_TRANSPORT_LIMIT {
                            stop.cancel();
                            return;
                        }
                    }
                }
            }
        }
    }
}

struct Session {
    state: Weak<State>,
    phase: watch::Sender<Phase>,
    stop: Stop,
    subscribers: Mutex<Subscribers>,
    codes: Mutex<Vec<String>>,
    room_id: Mutex<Option<i64>>,
    finished: AtomicBool,
    done: Notify,
}

struct Subscribers {
    count: usize,
    closing: bool,
}

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
    fn starting(state: Weak<State>) -> Arc<Self> {
        let (phase, _) = watch::channel(Phase::Starting);
        Arc::new(Self {
            state,
            phase,
            stop: Stop::new(),
            subscribers: Mutex::new(Subscribers {
                count: 0,
                closing: false,
            }),
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
        lock(&self.subscribers).closing
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
            let mut subscribers = lock(&self.subscribers);
            if subscribers.closing {
                return None;
            }
            subscribers.count += 1;
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
            subscribers.closing = true;
            drop(subscribers);
            self.stop.cancel();
        }
    }

    fn begin_close(&self) {
        lock(&self.subscribers).closing = true;
        self.stop.cancel();
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

    async fn cancelled(&self) {
        let notified = self.0.notify.notified();
        let mut notified = std::pin::pin!(notified);
        notified.as_mut().enable();
        if self.0.cancelled.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
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
