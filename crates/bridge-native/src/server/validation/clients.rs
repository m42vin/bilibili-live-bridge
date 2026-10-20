//! 有序消费、累计 ACK 与显式游标重连的压测客户端，处理统计使用有限直方图。

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use super::*;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const BOUNDS: [u64; 12] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000, u64::MAX];

/// 保留最后处理位置的测试消费者；不会在续传失败时自行清除游标。
pub(super) struct Consumer {
    socket: Socket,
    ready: Value,
    last: u64,
    index: usize,
}

impl Consumer {
    /// 用显式游标订阅，返回首个业务响应供错误场景核对。
    pub(super) async fn connect(url: &str, cursor: Option<Value>, index: usize) -> Result<Self> {
        let (mut socket, _) = timeout(Duration::from_secs(5), connect_async(url)).await??;
        let request = json!({"type":"subscribe","code":"validation-room","ack":true,"ack_window":64,"resume_after":cursor});
        socket.send(Message::text(request.to_string())).await?;
        let ready = next(&mut socket).await?;
        let last = ready["seq"].as_u64().unwrap_or(0);
        Ok(Self {
            socket,
            ready,
            last,
            index,
        })
    }
    /// 首个业务响应。
    pub(super) fn ready(&self) -> &Value {
        &self.ready
    }
    /// 当前场次 ID，仅用于本地桩关联。
    pub(super) fn game(&self) -> &str {
        self.ready["game_id"].as_str().unwrap()
    }
    /// 最后连续处理完成的游标。
    pub(super) fn cursor(&self) -> Value {
        json!({"stream_id":self.ready["stream_id"],"seq":self.last})
    }
    /// 返回下一个非控制业务消息。
    pub(super) async fn next(&mut self) -> Result<Value> {
        next(&mut self.socket).await
    }
    /// 消费到指定源索引，检查桥序号及源序号，累计确认并返回有界延迟统计。
    pub(super) async fn receive_to(&mut self, target: usize, origin: Instant) -> Result<Delivery> {
        let mut delivery = Delivery::default();
        while self.index < target {
            let message = self.next().await?;
            match message["type"].as_str() {
                Some("event") => {
                    let seq = message["seq"].as_u64().ok_or("缺少桥序号")?;
                    let index = message["data"]["index"].as_u64().ok_or("缺少源索引")? as usize;
                    check(
                        seq == self.last + 1 && index == self.index + 1,
                        format!(
                            "交付缺口：seq {seq} after {}，source {index} after {}",
                            self.last, self.index
                        ),
                    )?;
                    check(
                        message["stream_id"] == self.ready["stream_id"],
                        "同连接流标识变化",
                    )?;
                    self.last = seq;
                    self.index = index;
                    let sent = message["data"]["sent_us"].as_u64().unwrap();
                    delivery.record((origin.elapsed().as_micros() as u64).saturating_sub(sent));
                    if seq % 16 == 0 || index == target {
                        self.socket
                            .send(Message::text(
                                json!({"type":"ack","stream_id":self.ready["stream_id"],"seq":seq})
                                    .to_string(),
                            ))
                            .await?;
                    }
                }
                Some("status") => {}
                other => return Err(format!("消费期间收到终止消息 {other:?}: {message}").into()),
            }
        }
        Ok(delivery)
    }
    /// 完成关闭握手；已收到终止消息的连接也允许释放。
    pub(super) async fn close(mut self) {
        let _ = timeout(Duration::from_secs(2), async {
            let _ = self.socket.close(None).await;
            while let Some(Ok(message)) = self.socket.next().await {
                if matches!(message, Message::Close(_)) {
                    let _ = self.socket.flush().await;
                    break;
                }
            }
        })
        .await;
    }
}

async fn next(socket: &mut Socket) -> Result<Value> {
    timeout(Duration::from_secs(15), async {
        loop {
            match socket.next().await.ok_or("下游 EOF")?? {
                Message::Text(text) => return Ok(serde_json::from_str(&text)?),
                Message::Ping(_) => socket.flush().await?,
                Message::Close(_) => return Err("业务消息前收到关闭帧".into()),
                _ => {}
            }
        }
    })
    .await?
}

/// 有限直方图，不按事件保存增长的延迟数组。
#[derive(Default)]
pub(super) struct Delivery {
    count: u64,
    buckets: [u64; 12],
    max_us: u64,
}

impl Delivery {
    fn record(&mut self, micros: u64) {
        self.count += 1;
        self.max_us = self.max_us.max(micros);
        let bucket = BOUNDS
            .iter()
            .position(|bound| *bound >= micros.div_ceil(1000))
            .unwrap();
        self.buckets[bucket] += 1;
    }
    /// 合并多个并行消费者的统计。
    pub(super) fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.max_us = self.max_us.max(other.max_us);
        for (target, source) in self.buckets.iter_mut().zip(other.buckets) {
            *target += source;
        }
    }
    /// 有限直方图的第 95 百分位上界，单位毫秒。
    pub(super) fn p95(&self) -> u64 {
        let mut count = 0;
        for (bound, amount) in BOUNDS.iter().zip(self.buckets) {
            count += amount;
            if count >= (self.count * 95).div_ceil(100) {
                return *bound;
            }
        }
        u64::MAX
    }
    /// 报告交付量与延迟口径。
    pub(super) fn report(&self) -> Value {
        json!({"deliveries":self.count,"p95_upper_ms":self.p95(),"max_latency_us":self.max_us})
    }
}
