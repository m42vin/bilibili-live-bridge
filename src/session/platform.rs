//! 会话层使用的开放平台操作。
//!
//! 生产实现走 [`crate::open_live::api::Client`] 和 [`crate::open_live::ws::Connection`]。
//! 测试用同一组方法替换成桩，避免会话测试依赖真实网络。

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::open_live::api::{BatchHeartbeatResult, Client, StartResult};
use crate::open_live::error::ApiError;
use crate::open_live::ws::{Connection, LiveEvent, WsError};

pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub(super) trait Platform: Send + Sync + 'static {
    fn start<'a>(&'a self, code: &'a str) -> BoxFuture<'a, Result<StartResult, ApiError>>;
    fn batch_heartbeat<'a>(
        &'a self,
        game_ids: &'a [String],
    ) -> BoxFuture<'a, Result<BatchHeartbeatResult, ApiError>>;
    fn end<'a>(&'a self, game_id: &'a str) -> BoxFuture<'a, Result<(), ApiError>>;
    fn connect<'a>(
        &'a self,
        started: &'a StartResult,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>>;
}

pub(super) trait LiveSocket: Send {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>>;
    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>>;
}

pub(super) struct LivePlatform {
    api: Client,
    websocket_heartbeat: Duration,
}

impl LivePlatform {
    pub(super) fn new(api: Client, websocket_heartbeat: Duration) -> Self {
        Self {
            api,
            websocket_heartbeat,
        }
    }
}

impl Platform for LivePlatform {
    fn start<'a>(&'a self, code: &'a str) -> BoxFuture<'a, Result<StartResult, ApiError>> {
        Box::pin(self.api.start(code))
    }

    fn batch_heartbeat<'a>(
        &'a self,
        game_ids: &'a [String],
    ) -> BoxFuture<'a, Result<BatchHeartbeatResult, ApiError>> {
        Box::pin(async move {
            self.api
                .batch_heartbeat(game_ids.iter().map(String::as_str))
                .await
        })
    }

    fn end<'a>(&'a self, game_id: &'a str) -> BoxFuture<'a, Result<(), ApiError>> {
        Box::pin(self.api.end(game_id))
    }

    fn connect<'a>(
        &'a self,
        started: &'a StartResult,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        let heartbeat = self.websocket_heartbeat;
        Box::pin(async move {
            let connection = Connection::from_start(started, heartbeat).await?;
            Ok(Box::new(WsSocket(connection)) as Box<dyn LiveSocket>)
        })
    }
}

struct WsSocket(Connection);

impl LiveSocket for WsSocket {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>> {
        Box::pin(self.0.recv())
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>> {
        Box::pin(self.0.close())
    }
}
