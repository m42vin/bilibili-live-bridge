//! 共同驱动器保留管理探针直到业务收尾完成。

use super::*;

#[tokio::test]
async fn management_listener_lives_through_business_drain_and_stops_with_service() {
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    let manager = Manager::new(
        Client::new("fake-key", "fake-secret", 1).unwrap(),
        Duration::from_secs(20),
        Duration::from_secs(20),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let admin_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_address = admin_listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(serve(
        listener,
        admin_listener,
        manager,
        async {
            let _ = stopped.await;
        },
        Duration::from_secs(2),
    ));
    let (mut client, _) = connect_async(format!("ws://{address}/ws")).await.unwrap();
    stop.send(()).unwrap();
    let Message::Text(text) = tokio::time::timeout(Duration::from_secs(3), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("应通知维护退出");
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap()["type"],
        "service_restarting"
    );
    // 客户端尚未应答关闭，业务清理仍在进行，管理入口必须继续响应。
    for (path, status) in [("/readyz", "503"), ("/livez", "200"), ("/metrics", "200")] {
        let response = tokio::time::timeout(Duration::from_secs(1), async {
            let mut socket = tokio::net::TcpStream::connect(admin_address).await.unwrap();
            socket
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: test\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
            String::from_utf8(bytes).unwrap()
        })
        .await
        .unwrap();
        assert!(response.starts_with(&format!("HTTP/1.1 {status}")));
    }
    assert!(matches!(
        client.next().await.unwrap().unwrap(),
        Message::Close(_)
    ));
    let _ = client.flush().await;
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(admin_address).await.is_err());
}
