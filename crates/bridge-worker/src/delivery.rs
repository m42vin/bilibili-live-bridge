//! Worker 的有界房间日志与连接 ACK 窗口，不依赖平台绑定。
//! 保留策略服务于当前对象内的续传，不提供跨对象重建的恢复。

use std::collections::VecDeque;

/// 单房间的有界内存事件日志，淘汰后仍保留尾部序号。
pub(crate) struct Log {
    /// 最后记录的事件序号；空日志从零开始。
    pub tail: u64,
    bytes: usize,
    entries: VecDeque<(u64, u64, String)>,
}

impl Log {
    /// 创建尚未记录事件的日志。
    pub fn new() -> Self {
        Self {
            tail: 0,
            bytes: 0,
            entries: VecDeque::new(),
        }
    }

    /// 按毫秒时间戳淘汰超过 300 秒、256 条或 1 MiB 正文预算的记录。
    pub fn trim(&mut self, now: u64) {
        while self
            .entries
            .front()
            .is_some_and(|(_, at, _)| now.saturating_sub(*at) >= 300_000)
            || self.entries.len() > 256
            || self.bytes > 1_048_576
        {
            if let Some((_, _, text)) = self.entries.pop_front() {
                self.bytes -= text.len();
            }
        }
    }

    /// 分配序号后写入事件；超大记录被淘汰时留下可检测的续传缺口。
    pub fn push(&mut self, now: u64, text: String) {
        self.tail += 1;
        self.bytes += text.len();
        self.entries.push_back((self.tail, now, text));
        self.trim(now);
    }

    /// 最早可接受的续传游标，即首条保留记录的前一个序号。
    pub fn earliest(&self) -> u64 {
        self.entries.front().map_or(self.tail, |e| e.0 - 1)
    }

    /// 校验游标未超前且其后的记录仍在保留范围内。
    pub fn check(&self, seq: u64) -> Result<(), &'static str> {
        if seq > self.tail {
            Err("cursor_ahead")
        } else if seq < self.earliest() {
            Err("cursor_expired")
        } else {
            Ok(())
        }
    }

    /// 游标之后紧邻的事件正文；缺口与已到尾部均由调用方结合范围判断。
    pub fn next(&self, seq: u64) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.0 == seq + 1)
            .map(|e| e.2.as_str())
    }
}

/// 单连接累计 ACK 状态与未确认事件上限。
#[derive(Clone)]
pub(crate) struct Window {
    /// 已确认序号，重复或过期 ACK 不使其回退。
    pub acked: u64,
    /// 已交给运行时发送队列的最高序号，不代表客户端已处理。
    pub sent: u64,
    /// 允许同时未确认的事件数量，由接入校验保证非零。
    pub limit: u64,
}

impl Window {
    /// 接受不超出已发送位置的累计确认，拒绝未来序号。
    pub fn acknowledge(&mut self, seq: u64) -> Result<(), &'static str> {
        if seq > self.sent {
            return Err("ACK exceeds sent sequence");
        }
        self.acked = self.acked.max(seq);
        Ok(())
    }

    /// 未确认事件数量是否仍允许发送下一条事件。
    pub fn available(&self) -> bool {
        self.sent - self.acked < self.limit
    }
}

#[cfg(test)]
#[path = "delivery_test.rs"]
mod delivery_test;
