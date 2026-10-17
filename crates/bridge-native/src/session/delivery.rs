//! 有界内存日志。通知仅用于唤醒，事件与读取位置保存在日志中。

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use bridge_core::delivery::{EventCursor, RecordedEvent, ResumeUnavailable};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::time::Instant;

use super::{SessionError, Subscription, UpstreamStatus};
use crate::open_live::ws::LiveEvent;

/// 一个管理器共享的日志和下游 ACK 窗口限制。
#[derive(Debug, Clone, Copy)]
pub struct DeliveryPolicy {
    /// 事件以及结束流的最长保留时间，默认 300 秒；清理在访问日志时执行。
    pub retention: Duration,
    /// 全部流的事件 JSON、命令和记录开销预算，默认 32 MiB，不包含传输缓冲。
    pub max_bytes: usize,
    /// 活动与已结束流的数量上限，默认 128；优先淘汰已结束流。
    pub max_streams: usize,
    /// 每条下游连接最多在途未确认事件数，默认 64。
    pub ack_window: u64,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(300),
            max_bytes: 32 << 20,
            max_streams: 128,
            ack_window: 64,
        }
    }
}

impl DeliveryPolicy {
    /// 校验日志容量、ACK 窗口及原生时钟可表示的保留期限。
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.retention.is_zero()
            || Instant::now().checked_add(self.retention).is_none()
            || self.max_bytes == 0
            || self.max_streams == 0
            || self.ack_window == 0
        {
            return Err("事件日志保留时间、容量、流数量和 ACK 窗口必须为有效正值");
        }
        Ok(())
    }
}

/// ACK 模式接入错误。
#[derive(Debug)]
pub enum DeliveryError {
    /// 开启或加入活动场次失败。
    Attach(SessionError),
    /// 日志、身份码归属或游标不可用；不会开启新场次。
    Resume(ResumeUnavailable),
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Attach(e) => e.fmt(f),
            Self::Resume(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for DeliveryError {}

struct Entry {
    event: Arc<RecordedEvent>,
    at: Instant,
    bytes: usize,
}

struct Stream {
    room_id: i64,
    game_id: String,
    codes: Vec<[u8; 32]>,
    status: watch::Receiver<UpstreamStatus>,
    events: VecDeque<Entry>,
    tail: u64,
    ended: Option<Instant>,
}

#[derive(Default)]
struct State {
    streams: HashMap<String, Stream>,
    bytes: usize,
    dropped_events: u64,
}

/// 管理全部事件流的有界日志，提供续传读取及变化通知。
pub(crate) struct EventLog {
    /// 所有事件流共享的保留、容量与 ACK 窗口策略。
    pub(crate) policy: DeliveryPolicy,
    state: Mutex<State>,
    changed: watch::Sender<u64>,
}

impl EventLog {
    /// 读取实际占用及累计移除数量，不触发过期清理或改变交付状态。
    pub(crate) fn metrics(&self) -> [u64; 4] {
        let state = super::manager::lock(&self.state);
        [
            state.bytes as u64,
            state.streams.len() as u64,
            state
                .streams
                .values()
                .map(|stream| stream.events.len() as u64)
                .sum(),
            state.dropped_events,
        ]
    }

    /// 创建共享事件日志；调用方应先校验策略。
    pub(crate) fn new(policy: DeliveryPolicy) -> Arc<Self> {
        Arc::new(Self {
            policy,
            state: Mutex::new(State::default()),
            changed: watch::channel(0).0,
        })
    }

    /// 注册活动流；容量已满时优先淘汰结束流，否则拒绝。
    pub(crate) fn register(
        &self,
        id: &str,
        room_id: i64,
        game_id: &str,
        codes: Vec<String>,
        status: watch::Receiver<UpstreamStatus>,
    ) -> Result<(), &'static str> {
        let mut state = super::manager::lock(&self.state);
        self.prune(&mut state);
        if state.streams.len() >= self.policy.max_streams {
            let oldest = state
                .streams
                .iter()
                .filter_map(|(id, s)| s.ended.map(|at| (at, id.clone())))
                .min();
            if let Some((_, id)) = oldest {
                remove_stream(&mut state, &id);
            } else {
                return Err("活动事件流数量达到日志上限");
            }
        }
        state.streams.insert(
            id.to_owned(),
            Stream {
                room_id,
                game_id: game_id.to_owned(),
                codes: codes
                    .iter()
                    .take(128)
                    .map(|code| fingerprint(code))
                    .collect(),
                status,
                events: VecDeque::new(),
                tail: 0,
                ended: None,
            },
        );
        Ok(())
    }

    /// 登记流的身份码归属，最多保留最近使用的 128 个摘要。
    pub(crate) fn add_code(&self, id: &str, code: &str) {
        if let Some(stream) = super::manager::lock(&self.state).streams.get_mut(id) {
            let code = fingerprint(code);
            if let Some(index) = stream.codes.iter().position(|old| old == &code) {
                stream.codes.remove(index);
            }
            if stream.codes.len() == 128 {
                stream.codes.remove(0);
            }
            stream.codes.push(code);
        }
    }

    /// 返回流的最大记录序号；流不存在时返回零。
    pub(crate) fn tail(&self, id: &str) -> u64 {
        super::manager::lock(&self.state)
            .streams
            .get(id)
            .map_or(0, |s| s.tail)
    }

    /// 分配序号并追加事件，超出共享预算时淘汰最早记录。
    pub(crate) fn append(&self, id: &str, event: &LiveEvent) -> Result<(), serde_json::Error> {
        let data = event.data_json()?;
        let cmd = event.cmd().to_owned();
        let bytes = data.get().len().saturating_add(cmd.len()).saturating_add(
            std::mem::size_of::<Entry>() + std::mem::size_of::<RecordedEvent>() + 32,
        );
        let mut state = super::manager::lock(&self.state);
        self.prune(&mut state);
        if let Some(stream) = state.streams.get_mut(id) {
            stream.tail = stream.tail.checked_add(1).expect("事件序号耗尽");
            if bytes > self.policy.max_bytes {
                // 无法保存单条大事件时建立明确缺口，并保持日志为连续后缀。
                let dropped = stream.events.len() as u64 + 1;
                let removed: usize = stream.events.drain(..).map(|e| e.bytes).sum();
                stream.events.shrink_to_fit();
                state.bytes -= removed;
                state.dropped_events = state.dropped_events.saturating_add(dropped);
            } else {
                stream.events.push_back(Entry {
                    event: Arc::new(RecordedEvent {
                        seq: stream.tail,
                        cmd,
                        data,
                    }),
                    at: Instant::now(),
                    bytes,
                });
                state.bytes = state.bytes.saturating_add(bytes);
            }
        }
        while state.bytes > self.policy.max_bytes {
            let oldest = state
                .streams
                .iter()
                .filter_map(|(id, s)| s.events.front().map(|e| (e.at, id.clone())))
                .min();
            let Some((_, id)) = oldest else {
                break;
            };
            let bytes = state
                .streams
                .get_mut(&id)
                .unwrap()
                .events
                .pop_front()
                .unwrap()
                .bytes;
            state.bytes -= bytes;
            state.dropped_events = state.dropped_events.saturating_add(1);
            compact(&mut state.streams.get_mut(&id).unwrap().events);
        }
        drop(state);
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(())
    }

    /// 标记流结束并唤醒读取器，保留日志供尾部回放。
    pub(crate) fn finish(&self, id: &str) {
        if let Some(stream) = super::manager::lock(&self.state).streams.get_mut(id) {
            stream.ended = Some(Instant::now());
        }
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn prune(&self, state: &mut State) {
        let now = Instant::now();
        let expired: Vec<_> = state
            .streams
            .iter()
            .filter(|(_, s)| {
                s.ended
                    .is_some_and(|at| now.duration_since(at) >= self.policy.retention)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            remove_stream(state, &id);
        }
        for stream in state.streams.values_mut() {
            while stream
                .events
                .front()
                .is_some_and(|e| now.duration_since(e.at) >= self.policy.retention)
            {
                state.bytes -= stream.events.pop_front().unwrap().bytes;
                state.dropped_events = state.dropped_events.saturating_add(1);
            }
            compact(&mut stream.events);
        }
    }

    /// 按身份码和游标创建读取器；活动流必须持有会话订阅。
    pub(crate) fn reader(
        self: &Arc<Self>,
        id: &str,
        code: &str,
        after: Option<u64>,
        guard: Option<Subscription>,
    ) -> Result<DeliverySubscription, ResumeUnavailable> {
        self.reader_checked(id, code, after, guard, true)
    }

    fn reader_checked(
        self: &Arc<Self>,
        id: &str,
        code: &str,
        after: Option<u64>,
        guard: Option<Subscription>,
        require_guard: bool,
    ) -> Result<DeliverySubscription, ResumeUnavailable> {
        // 先创建通知接收器，再读取快照，避免记录与等待之间丢失唤醒。
        let changed = self.changed.subscribe();
        let mut state = super::manager::lock(&self.state);
        self.prune(&mut state);
        let stream = state
            .streams
            .get(id)
            .filter(|s| s.codes.contains(&fingerprint(code)))
            .ok_or_else(unavailable)?;
        if require_guard && guard.is_none() && stream.ended.is_none() {
            return Err(unavailable());
        }
        let base = after.unwrap_or(stream.tail);
        validate(stream, base)?;
        Ok(DeliverySubscription {
            stream_id: id.to_owned(),
            room_id: stream.room_id,
            game_id: stream.game_id.clone(),
            base_seq: base,
            next: base + 1,
            log: Arc::clone(self),
            changed,
            status: stream.status.clone(),
            _guard: guard,
        })
    }
}

fn compact(events: &mut VecDeque<Entry>) {
    if events.capacity() > events.len().saturating_mul(2).saturating_add(64) {
        events.shrink_to_fit();
    }
}

fn fingerprint(code: &str) -> [u8; 32] {
    Sha256::digest(code.as_bytes()).into()
}

fn remove_stream(state: &mut State, id: &str) {
    if let Some(stream) = state.streams.remove(id) {
        state.bytes -= stream.events.iter().map(|e| e.bytes).sum::<usize>();
        state.dropped_events = state
            .dropped_events
            .saturating_add(stream.events.len() as u64);
    }
}

fn unavailable() -> ResumeUnavailable {
    ResumeUnavailable {
        reason: "stream_unavailable",
        earliest_after: None,
        latest_seq: None,
    }
}

fn validate(stream: &Stream, after: u64) -> Result<(), ResumeUnavailable> {
    let earliest = stream
        .events
        .front()
        .map_or(stream.tail, |e| e.event.seq - 1);
    let reason = if after < earliest {
        "cursor_expired"
    } else if after > stream.tail {
        "cursor_ahead"
    } else {
        return Ok(());
    };
    Err(ResumeUnavailable {
        reason,
        earliest_after: Some(earliest),
        latest_seq: Some(stream.tail),
    })
}

/// 从日志按序读取的订阅；持有活动会话时参与闲置计数，结束流仅回放日志。
pub struct DeliverySubscription {
    /// 事件流标识。
    pub stream_id: String,
    /// 房间号。
    pub room_id: i64,
    /// 原平台场次标识。
    pub game_id: String,
    /// 本次接入的已处理起点；ready 应将此值返回客户端。
    pub base_seq: u64,
    next: u64,
    log: Arc<EventLog>,
    changed: watch::Receiver<u64>,
    status: watch::Receiver<UpstreamStatus>,
    _guard: Option<Subscription>,
}

impl DeliverySubscription {
    /// 订阅全部流的日志变化通知，读取事实仍需检查日志。
    pub(crate) fn changes(&self) -> watch::Receiver<u64> {
        self.log.changed.subscribe()
    }

    /// 本管理器配置的 ACK 窗口大小。
    pub fn ack_window(&self) -> u64 {
        self.log.policy.ack_window
    }

    /// 独立于事件的上游状态快照。
    pub fn watch_status(&self) -> watch::Receiver<UpstreamStatus> {
        self.status.clone()
    }

    /// 验证最后确认位置仍可续传。可用于 ACK 窗口暂停时检查日志淘汰。
    pub fn validate_after(&self, seq: u64) -> Result<(), ResumeUnavailable> {
        let mut state = super::manager::lock(&self.log.state);
        self.log.prune(&mut state);
        validate(
            state.streams.get(&self.stream_id).ok_or_else(unavailable)?,
            seq,
        )
    }

    /// 读取下一条事件；结束且尾部读完返回 None，缺口返回错误而不跳过。
    ///
    /// # Errors
    /// 日志已淘汰、游标过期或无效时返回保留范围。
    pub async fn recv(&mut self) -> Result<Option<Arc<RecordedEvent>>, ResumeUnavailable> {
        loop {
            self.changed.borrow_and_update();
            {
                let mut state = super::manager::lock(&self.log.state);
                self.log.prune(&mut state);
                let stream = state.streams.get(&self.stream_id).ok_or_else(unavailable)?;
                validate(stream, self.next - 1)?;
                let offset = stream
                    .events
                    .front()
                    .map(|first| (self.next - first.event.seq) as usize);
                if let Some(event) = offset.and_then(|offset| stream.events.get(offset)) {
                    self.next += 1;
                    return Ok(Some(Arc::clone(&event.event)));
                }
                if stream.ended.is_some() {
                    return Ok(None);
                }
            }
            let _ = self.changed.changed().await;
        }
    }
}

#[cfg(test)]
#[path = "delivery_test.rs"]
mod tests;
