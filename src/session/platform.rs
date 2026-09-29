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

/// 让异步操作可通过 trait 对象调用，保留对平台和参数的借用寿命。
pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// 会话需要的最小平台操作；生命周期与重试策略由管理器负责。
pub(super) trait Platform: Send + Sync + 'static {
    /// 用身份码开启场次，返回房间号与官方连接信息。
    fn start<'a>(&'a self, code: &'a str) -> BoxFuture<'a, Result<StartResult, ApiError>>;
    /// 发送一批项目心跳；成功结果仍可能包含失败场次。
    fn batch_heartbeat<'a>(
        &'a self,
        game_ids: &'a [String],
    ) -> BoxFuture<'a, Result<BatchHeartbeatResult, ApiError>>;
    /// 结束已知场次，管理器决定如何记录清理错误。
    fn end<'a>(&'a self, game_id: &'a str) -> BoxFuture<'a, Result<(), ApiError>>;
    /// 建立并鉴权长连接；失败后由管理器清理已开启场次。
    fn connect<'a>(
        &'a self,
        started: &'a StartResult,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>>;
}

/// 一条会话独占的上游连接，供生产适配器和测试桩替换。
pub(super) trait LiveSocket: Send {
    /// 等待事件并驱动连接心跳；None 表示上游已关闭。
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>>;
    /// 关闭连接，不代替平台的场次结束操作。
    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>>;
}

/// 用真实 HTTP 客户端和官方 WebSocket 实现平台操作。
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
