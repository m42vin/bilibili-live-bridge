//! 会话层使用的开放平台操作。
//!
//! 生产实现走 [`crate::open_live::api::Client`] 和 [`crate::open_live::ws::Connection`]。
//! 测试用同一组方法替换成桩，避免会话测试依赖真实网络。

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use crate::open_live::api::{BatchHeartbeatResult, Client, StartResult};
use crate::open_live::error::ApiError;
use crate::open_live::ws::{Connection, HealthPolicy, LiveEvent, WsError};

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

    /// 用原鉴权信息恢复，生产实现优先尝试上次地址之后的集群。
    fn reconnect<'a>(
        &'a self,
        started: &'a StartResult,
        _endpoint: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        self.connect(started)
    }
}

/// 一条会话独占的上游连接，供生产适配器和测试桩替换。
pub(super) trait LiveSocket: Send {
    /// 当前候选地址；模拟连接可省略。
    fn endpoint(&self) -> Option<&str> {
        None
    }

    /// 等待事件并驱动连接心跳；None 表示上游已关闭。
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>>;

    /// 关闭连接，不代替平台的场次结束操作。
    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>>;
}

/// 用真实 HTTP 客户端和官方 WebSocket 实现平台操作。
pub(super) struct LivePlatform {
    api: Client,
    websocket_heartbeat: Duration,
    health: HealthPolicy,
}

impl LivePlatform {
    /// 用已校验的时长和健康策略包装原生平台客户端。
    pub(super) fn new(api: Client, websocket_heartbeat: Duration, health: HealthPolicy) -> Self {
        Self {
            api,
            websocket_heartbeat,
            health,
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
            let connection =
                Connection::from_start_with_policy(started, heartbeat, self.health).await?;
            Ok(Box::new(WsSocket(connection)) as Box<dyn LiveSocket>)
        })
    }

    fn reconnect<'a>(
        &'a self,
        started: &'a StartResult,
        endpoint: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Box<dyn LiveSocket>, WsError>> {
        Box::pin(async move {
            let ordered = rotate_links(started.wss_link(), endpoint);
            let connection = Connection::connect_with_policy(
                ordered,
                started.auth_body(),
                self.websocket_heartbeat,
                self.health,
            )
            .await?;
            Ok(Box::new(WsSocket(connection)) as Box<dyn LiveSocket>)
        })
    }
}

/// 恢复时的候选顺序：从上次地址的下一个开始环形遍历，并去掉重复地址。
///
/// 平台会返回重复的 `wss_link`（例如浙江集群连出两次）。去重既不浪费握手期限，
/// 也让恢复预算花在真正不同的集群上；同一个地址仍可能在最后一轮被重新尝试。
/// 上一个地址不在列表里或没有历史时从第一项开始。
fn rotate_links<'a>(links: &'a [String], endpoint: Option<&str>) -> Vec<&'a str> {
    let mut distinct: Vec<&str> = Vec::with_capacity(links.len());
    for link in links.iter().map(String::as_str) {
        if !link.trim().is_empty() && !distinct.contains(&link) {
            distinct.push(link);
        }
    }
    // 偏移必须在去重后的列表上计算，否则重复项会把起点挪回已经失败的集群。
    let offset = endpoint
        .and_then(|endpoint| distinct.iter().position(|link| *link == endpoint))
        .map_or(0, |index| index + 1);
    let mut ordered = Vec::with_capacity(distinct.len());
    for link in distinct.iter().cycle().skip(offset).take(distinct.len()) {
        ordered.push(*link);
    }
    ordered
}

struct WsSocket(Connection);

impl LiveSocket for WsSocket {
    fn endpoint(&self) -> Option<&str> {
        Some(self.0.endpoint())
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Option<LiveEvent>, WsError>> {
        Box::pin(self.0.recv())
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), WsError>> {
        Box::pin(self.0.close())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_live::api::Anchor;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[tokio::test]
    async fn recovery_uses_original_auth_and_next_cluster() {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_url = format!("ws://{}", first.local_addr().unwrap());
        let second_url = format!("ws://{}", second.local_addr().unwrap());
        let mut tasks = Vec::new();
        for listener in [first, second] {
            tasks.push(tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let Message::Binary(auth) = socket.next().await.unwrap().unwrap() else {
                    panic!("expected auth");
                };
                assert_eq!(&auth[16..], b"same-auth");
                let reply = [
                    0, 0, 0, 26, 0, 16, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, b'{', b'"', b'c', b'o', b'd',
                    b'e', b'"', b':', b'0', b'}',
                ];
                socket.send(Message::binary(reply.to_vec())).await.unwrap();
                let _ = socket.next().await;
            }));
        }
        let started = StartResult::from_parts(
            "same-game",
            "same-auth",
            vec![
                first_url.clone(),
                // 平台会重复同一个集群，例如浙江连出两次。
                first_url.clone(),
                second_url.clone(),
            ],
            Anchor {
                room_id: 7,
                uname: String::new(),
                uface: String::new(),
                uid: 0,
                open_id: String::new(),
                union_id: String::new(),
            },
        );
        let platform = LivePlatform::new(
            Client::new("fake-key", "fake-secret", 1).unwrap(),
            Duration::from_secs(20),
            HealthPolicy::default(),
        );
        let mut original = platform.connect(&started).await.unwrap();
        assert_eq!(original.endpoint(), Some(first_url.as_str()));
        original.close().await.unwrap();
        let mut restored = platform
            .reconnect(&started, Some(&first_url))
            .await
            .unwrap();
        assert_eq!(restored.endpoint(), Some(second_url.as_str()));
        restored.close().await.unwrap();
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[test]
    fn recovery_order_skips_duplicates_and_wraps() {
        let links = vec![
            "wss://zj".to_owned(),
            "wss://zj".to_owned(),
            "wss://bj".to_owned(),
            "wss://sz".to_owned(),
            "wss://main".to_owned(),
        ];
        // 上次停在浙江时，重复项不能让浙江被再试一次。
        assert_eq!(
            rotate_links(&links, Some("wss://zj")),
            ["wss://bj", "wss://sz", "wss://main", "wss://zj"]
        );
        // 没有历史或地址不在列表里时从第一项开始。
        assert_eq!(
            rotate_links(&links, None),
            ["wss://zj", "wss://bj", "wss://sz", "wss://main"]
        );
        assert_eq!(
            rotate_links(&links, Some("wss://gone")),
            ["wss://zj", "wss://bj", "wss://sz", "wss://main"]
        );
        // 停在最后一项时回绕到第一项。
        assert_eq!(
            rotate_links(&links, Some("wss://main")),
            ["wss://zj", "wss://bj", "wss://sz", "wss://main"]
        );
        // 单个地址、空串和重复项都保持可用顺序。
        assert_eq!(
            rotate_links(&["wss://only".to_owned()], None),
            ["wss://only"]
        );
        assert_eq!(
            rotate_links(&["wss://a".to_owned(), "  ".to_owned()], None),
            ["wss://a"]
        );
    }
}
