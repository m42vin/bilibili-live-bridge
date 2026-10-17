//! 下游 JSON 的有界序列化及发送字节所有权；控制帧和终止通知使用独立的小消息路径。

use std::io::{self, Write};

use serde::Serialize;
use tokio_tungstenite::tungstenite::Message;

use super::Exit;
use crate::resource::{ResourceBudget, SendPermit};

/// 一个连接的写端；sink 先销毁，再释放可能仍被其缓冲引用的字节额度。
pub(super) struct Output<S> {
    /// 实际网络或测试写端。
    pub(super) sink: S,
    /// 与同服务全部连接共享的预算。
    pub(super) budget: ResourceBudget,
    sending: Option<SendPermit>,
}

/// 已编码且持有额度的业务消息，发送开始前才登记 ACK 上界。
pub(super) struct Prepared {
    /// 已编码的 JSON 帧。
    pub(super) message: Message,
    /// 序列化和待 flush 字节的所有权。
    pub(super) permit: SendPermit,
}

impl<S> Output<S> {
    /// 包装写端和经校验的共享预算。
    pub(super) fn new(sink: S, budget: ResourceBudget) -> Self {
        Self {
            sink,
            budget,
            sending: None,
        }
    }

    /// 序列化前取得额度；失败时不保留部分 JSON，也不推进 ACK 上界。
    pub(super) fn prepare(&self, message: &impl Serialize) -> Result<Prepared, Exit> {
        let mut permit = self.budget.reserve_send().map_err(Exit::Resource)?;
        let mut buffer = Bounded {
            bytes: Vec::new(),
            limit: self.budget.policy().max_connection_send_bytes,
            oversized: false,
        };
        if serde_json::to_writer(&mut buffer, message).is_err() {
            return Err(if buffer.oversized {
                Exit::Oversized
            } else {
                Exit::Failed
            });
        }
        permit.shrink(buffer.bytes.len());
        let text = String::from_utf8(buffer.bytes).map_err(|_| Exit::Failed)?;
        Ok(Prepared {
            message: Message::text(text),
            permit,
        })
    }

    /// 将额度交给写端，取消或超时后仍保留到写端销毁。
    pub(super) fn hold(&mut self, permit: SendPermit) {
        assert!(self.sending.is_none(), "已取消或失败的写端不能复用");
        self.sending = Some(permit);
    }

    /// 成功 flush 后释放在途字节。
    pub(super) fn flushed(&mut self) {
        self.sending.take();
    }
}

struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
    oversized: bool,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.oversized = true;
            return Err(io::Error::other("下游消息超过字节上限"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// 仅计数序列化长度，不持有输出副本或发送额度，用于历史回放令牌计费。
pub(super) fn encoded_len(message: &impl Serialize, limit: usize) -> Result<usize, Exit> {
    let mut writer = Counter { length: 0, limit };
    serde_json::to_writer(&mut writer, message).map_err(|_| Exit::Oversized)?;
    Ok(writer.length)
}

struct Counter {
    length: usize,
    limit: usize,
}

impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.length) {
            return Err(io::Error::other("下游消息超过字节上限"));
        }
        self.length += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "output_test.rs"]
mod output_test;
