//! 单会话接入、订阅、闲置与关闭的纯状态。
//!
//! 调用方在同一串行边界内执行状态转换及发布通知，显式提供单调时间。
//! 房间索引、目标会话引用、错误正文、广播通道及网络清理由运行时持有。
use super::recovery::Moment;
use std::time::Duration;

/// 会话生命周期选项。
#[derive(Debug, Clone, Copy, Default)]
pub struct ManagerOptions {
    /// 最后一个订阅离开后的宽限期；零表示立即关闭。
    /// 宽限期内保留心跳及事件接收；平台结束和主动关闭不等待此期限。
    pub idle_grace: Duration,
}

/// 对接入者可见的阶段；关闭标记和清理完成状态独立保存。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPhase {
    /// 场次开启或上游连接鉴权中，后续接入等待首个接入者。
    Starting,
    /// 可尝试订阅，仍需检查关闭标记和闲置期限。
    Ready,
    /// 重复场次已处理，接入者转向另一会话。
    Redirected,
    /// 接入失败，错误由运行时保存。
    Failed,
    /// 已运行的会话已结束。
    Closed,
}

/// 同一身份码再次接入时的决定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimDecision {
    /// 复用当前会话，包括等待它完成接入。
    Reuse,
    /// 当前会话仍在清理，不能开始新场次。
    WaitForCleanup,
    /// 当前会话已完成，可移除旧索引并重新接入。
    Replace,
}

/// 订阅变化产生的运行时动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdleAction<T> {
    /// 不需要改变定时器或停止信号。
    Unchanged,
    /// 设置闲置截止时间。
    Schedule(T),
    /// 清除闲置计时器并停止会话任务；网络清理由任务处理。
    Close,
}

/// 单个会话的生命周期，不包含运行时资源或网络错误。
#[derive(Debug)]
pub struct SessionLifecycle<T: Moment = Duration> {
    phase: AdmissionPhase,
    subscribers: usize,
    idle_grace: Duration,
    idle_deadline: Option<T>,
    closing: bool,
    finished: bool,
}
impl<T: Moment> SessionLifecycle<T> {
    /// 创建尚未接入的会话；尚无订阅时不会自动启用闲置计时器。
    pub fn starting(options: ManagerOptions) -> Self {
        Self {
            phase: AdmissionPhase::Starting,
            subscribers: 0,
            idle_grace: options.idle_grace,
            idle_deadline: None,
            closing: false,
            finished: false,
        }
    }
    /// 当前接入阶段。
    pub fn phase(&self) -> AdmissionPhase {
        self.phase
    }
    /// 活动订阅数量。
    pub fn subscriber_count(&self) -> usize {
        self.subscribers
    }
    /// 当前闲置截止时间。
    pub fn idle_deadline(&self) -> Option<T> {
        self.idle_deadline
    }
    /// 是否已禁止新订阅；调用方应先处理到期的闲置计时器。
    pub fn is_closing(&self) -> bool {
        self.closing
    }
    /// 是否已完成本地清理；不代表平台确认 end 成功。
    pub fn is_finished(&self) -> bool {
        self.finished
    }
    /// 当前会话是否可作为复用目标；调用方先处理闲置到期。
    pub fn is_joinable(&self) -> bool {
        !self.finished
            && !self.closing
            && matches!(self.phase, AdmissionPhase::Starting | AdmissionPhase::Ready)
    }
    /// 同码接入的决定；调用方先处理闲置到期并在索引锁内使用结果。
    pub fn claim(&self) -> ClaimDecision {
        if self.is_joinable() {
            ClaimDecision::Reuse
        } else if self.finished {
            ClaimDecision::Replace
        } else {
            ClaimDecision::WaitForCleanup
        }
    }
    /// 发布就绪阶段。关闭可能已同时开始，此时仍不能增加订阅。
    pub fn ready(&mut self) -> bool {
        if self.phase != AdmissionPhase::Starting || self.finished {
            return false;
        }
        self.phase = AdmissionPhase::Ready;
        true
    }
    /// 增加一个订阅并取消闲置截止时间。等于截止时间时拒绝接入。
    /// 调用方在失败后检查关闭标记并发出停止信号。
    pub fn subscribe(&mut self, now: T) -> bool {
        self.expire_idle(now);
        if self.phase != AdmissionPhase::Ready || self.closing || self.finished {
            return false;
        }
        let Some(count) = self.subscribers.checked_add(1) else {
            return false;
        };
        self.subscribers = count;
        self.idle_deadline = None;
        true
    }
    /// 释放一个订阅；只有最后一次有效释放启用宽限期。
    pub fn release(&mut self, now: T) -> IdleAction<T> {
        if self.subscribers == 0 {
            return IdleAction::Unchanged;
        }
        self.subscribers -= 1;
        if self.subscribers != 0 || self.closing || self.finished {
            return IdleAction::Unchanged;
        }
        if self.idle_grace.is_zero() {
            self.closing = true;
            self.idle_deadline = None;
            return IdleAction::Close;
        }
        let Some(deadline) = now.checked_add(self.idle_grace) else {
            // 无法表示期限时及时关闭，不能无限保留资源。
            self.closing = true;
            self.idle_deadline = None;
            return IdleAction::Close;
        };
        self.idle_deadline = Some(deadline);
        IdleAction::Schedule(deadline)
    }
    /// 根据当前截止时间处理到期；旧计时器不会关闭已重新订阅的会话。
    pub fn expire_idle(&mut self, now: T) -> bool {
        if !self.closing
            && !self.finished
            && self.subscribers == 0
            && self.idle_deadline.is_some_and(|at| at <= now)
        {
            self.closing = true;
            self.idle_deadline = None;
            true
        } else {
            false
        }
    }
    /// 开始主动关闭，立即拒绝后续订阅，但不标记清理完成。
    /// 返回是否首次进入关闭中；运行时的取消和注销操作仍应可重复调用。
    pub fn begin_close(&mut self) -> bool {
        let first = !self.closing;
        self.closing = true;
        first
    }
    /// 发布接入失败；运行时随后清理通知及资源，再调用 complete_cleanup。
    pub fn fail(&mut self) -> bool {
        self.terminal(AdmissionPhase::Failed)
    }
    /// 发布重定向；运行时必须先处理重复场次，保存目标引用并完成清理。
    pub fn redirect(&mut self) -> bool {
        self.terminal(AdmissionPhase::Redirected)
    }
    /// 发布已运行会话结束；网络结束调用已尝试，不要求成功。
    pub fn close(&mut self) -> bool {
        self.terminal(AdmissionPhase::Closed)
    }
    fn terminal(&mut self, phase: AdmissionPhase) -> bool {
        if self.finished
            || matches!(
                self.phase,
                AdmissionPhase::Failed | AdmissionPhase::Redirected | AdmissionPhase::Closed
            )
        {
            return false;
        }
        self.phase = phase;
        self.closing = true;
        true
    }
    /// 本地收尾完成后标记结束；就绪或接入中的会话不能直接跳过终止阶段。
    /// 返回是否首次完成，用于避免重复通知。
    pub fn complete_cleanup(&mut self) -> bool {
        if self.finished || matches!(self.phase, AdmissionPhase::Starting | AdmissionPhase::Ready) {
            return false;
        }
        self.finished = true;
        true
    }
}

/// 被取消接入的清理需求。无场次 ID 时只能本地失败，不能调用 end。
#[derive(Debug, PartialEq, Eq)]
pub struct CancelledAdmission {
    /// 已取得且仍归接入者负责的场次 ID。
    pub game_id: Option<String>,
}

/// 首个接入者的清理所有权。不会执行析构任务，也不保证远端操作恰好一次。
#[derive(Debug, Default)]
pub struct AdmissionOwner {
    game_id: Option<String>,
    committed: bool,
}
impl AdmissionOwner {
    /// start 成功后登记场次；网络响应之前无法确定远端是否已开启。
    pub fn acquired(&mut self, game_id: String) {
        if !self.committed {
            self.game_id = Some(game_id);
        }
    }
    /// 仍需由接入者负责关闭的场次 ID。
    pub fn game_id(&self) -> Option<&str> {
        self.game_id.as_deref()
    }
    /// end 已返回后释放场次责任；接入本身可能尚未结束。
    pub fn release_game(&mut self) {
        self.game_id = None;
    }
    /// 接入成功转交运行任务，或失败/重定向已处理，撤销取消清理责任。
    pub fn commit(&mut self) {
        self.game_id = None;
        self.committed = true;
    }
    /// 取消未提交的接入，只返回一次清理需求。运行时决定如何发布失败并尝试 end。
    pub fn cancel(&mut self) -> Option<CancelledAdmission> {
        if self.committed {
            return None;
        }
        self.committed = true;
        Some(CancelledAdmission {
            game_id: self.game_id.take(),
        })
    }
}

#[cfg(test)]
#[path = "lifecycle_test.rs"]
mod lifecycle_test;
