//! 在平台适配边界观测操作结果与耗时，不改变重试、取消或资源所有权。

use std::{future::Future, sync::Arc};

use super::platform::{BoxFuture, LiveSocket, Platform};
use crate::observability::{Counters, Fault, Operation, Outcome};
use crate::open_live::api::{BatchHeartbeatResult, StartResult};
use crate::open_live::{
    error::ApiError,
    ws::{LiveEvent, WsError},
};

/// 包装管理器的平台实现，所有分支共用同一份有界计数器。
pub(super) struct ObservedPlatform {
    inner: Arc<dyn Platform>,
    counters: Arc<Counters>,
}

impl ObservedPlatform {
    /// 为真实适配器或测试桩添加相同的观测边界。
    pub(super) fn new(inner: Arc<dyn Platform>, counters: Arc<Counters>) -> Self {
        Self { inner, counters }
    }

    async fn api<T>(
        &self,
        operation: Operation,
        future: impl Future<Output = Result<T, ApiError>>,
    ) -> Result<T, ApiError> {
        let timer = self.counters.operation(operation);
        let result = future.await;
        timer.finish(result.as_ref().err().map_or(Outcome::Success, Outcome::api));
        result
    }

    async fn socket(
        &self,
        operation: Operation,
        future: impl Future<Output = Result<Box<dyn LiveSocket>, WsError>>,
    ) -> Result<Box<dyn LiveSocket>, WsError> {
        let timer = self.counters.operation(operation);
        let result = future.await;
        timer.finish(result.as_ref().err().map_or(Outcome::Success, Outcome::ws));
        result.map(|inner| {
            Box::new(ObservedSocket {
                inner,
                counters: self.counters.clone(),
            }) as Box<dyn LiveSocket>
        })
    }
}

impl Platform for ObservedPlatform {
    fn start<'a>(&'a self, code: &'a str) -> BoxFuture<'a, Result<StartResult, ApiError>> {
        Box::pin(self.api(Operation::Start, self.inner.start(code)))
    }

    fn batch_heartbeat<'a>(
        &'a self,
        ids: &'a [String],
    ) -> BoxFuture<'a, Result<BatchHeartbeatResult, ApiError>> {
        Box::pin(self.api(Operation::BatchHeartbeat, self.inner.batch_heartbeat(ids)))
    }

    fn end<'a>(&'a self, game: &'a str) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(self.api(Operation::End, self.inner.end(game)))
    }

    fn connect<'a>(
        &'a self,
        started: &'a StartResult,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        Box::pin(self.socket(Operation::Connect, self.inner.connect(started)))
    }

    fn reconnect<'a>(
        &'a self,
        started: &'a StartResult,
        endpoint: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        Box::pin(self.socket(
            Operation::Reconnect,
            self.inner.reconnect(started, endpoint),
        ))
    }
}

struct ObservedSocket {
    inner: Box<dyn LiveSocket>,
    counters: Arc<Counters>,
}

impl LiveSocket for ObservedSocket {
    fn endpoint(&self) -> Option<&str> {
        self.inner.endpoint()
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>> {
        Box::pin(async {
            let result = self.inner.recv().await;
            match &result {
                Err(error) => self.counters.fault(Fault::upstream(error)),
                Ok(None) => self.counters.fault(Fault::UpstreamEof),
                _ => {}
            }
            result
        })
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>> {
        Box::pin(async {
            let timer = self.counters.operation(Operation::SocketClose);
            let result = self.inner.close().await;
            timer.finish(result.as_ref().err().map_or(Outcome::Success, Outcome::ws));
            result
        })
    }
}
