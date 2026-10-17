//! 管理 HTTP 路由、资源隔离及有界 I/O 的行为验证。

use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::*;
use crate::open_live::api::Client;
use crate::resource::{Resource, ResourceBudget, ResourcePolicy};

fn manager() -> Manager {
    Manager::new(
        Client::new("fake-key", "fake-secret", 1).unwrap(),
        Duration::from_secs(20),
        Duration::from_secs(20),
    )
    .unwrap()
}

async fn request(address: std::net::SocketAddr, request: &[u8]) -> String {
    timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap()
}

async fn start(
    manager: Manager,
) -> (
    std::net::SocketAddr,
    oneshot::Sender<()>,
    JoinHandle<io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(serve(listener, manager, async {
        let _ = stopped.await;
    }));
    (address, stop, task)
}

#[tokio::test]
async fn probes_survive_business_saturation_and_reflect_draining() {
    let budget = ResourceBudget::new(ResourcePolicy {
        max_connections: 1,
        max_handshakes: 1,
        ..Default::default()
    })
    .unwrap();
    let manager = manager().with_resource_budget(budget.clone()).unwrap();
    let (address, stop, task) = start(manager.clone()).await;
    let _full = budget.acquire(Resource::Connection).unwrap();
    assert!(budget.acquire(Resource::Connection).is_err());
    assert!(
        request(address, b"GET /livez HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(
        request(address, b"GET /readyz HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200")
    );
    let response = request(address, b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n").await;
    assert!(response.contains("Content-Type: text/plain; version=0.0.4"));
    assert!(response.contains("bridge_resource_occupied{resource=\"connection\"} 1\n"));
    assert!(response.contains("bridge_resource_rejections_total{resource=\"connection\"} 1\n"));
    let _ = manager.shutdown().await;
    assert!(
        request(address, b"GET /readyz HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 503")
    );
    assert!(
        request(address, b"GET /livez HTTP/1.1\r\nHost: test\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200")
    );
    assert!(metrics(&manager).contains("bridge_ready 0\n"));
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn management_rejects_bad_methods_headers_and_routes_without_echoing_input() {
    let manager = manager();
    let (address, stop, task) = start(manager.clone()).await;
    let oversized = format!(
        "GET /livez HTTP/1.1\r\nX-Private: {}\r\n\r\n",
        "a".repeat(MAX_HEADER_BYTES)
    );
    let too_many = format!(
        "GET /livez HTTP/1.1\r\n{}\r\n",
        "X-Test: value\r\n".repeat(33)
    );
    for (raw, status) in [
        ("POST /readyz HTTP/1.1\r\nHost: test\r\n\r\n", "405"),
        ("GET /private-code HTTP/1.1\r\nHost: test\r\n\r\n", "404"),
        ("not http\r\n\r\n", "400"),
        (&oversized, "400"),
        (&too_many, "400"),
    ] {
        let response = request(address, raw.as_bytes()).await;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}")),
            "{response}"
        );
        assert!(!response.contains("private-code"));
        assert!(response.contains("Connection: close\r\n"));
    }
    assert!(!manager.is_shutting_down());
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn management_read_and_write_waits_are_bounded() {
    for block_write in [false, true] {
        let manager = manager();
        let (mut peer, socket) = tokio::io::duplex(64);
        if block_write {
            peer.write_all(b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n")
                .await
                .unwrap();
        }
        let before = Instant::now();
        let result = respond(socket, &manager).await;
        assert_eq!(Instant::now() - before, IO_TIMEOUT);
        if block_write {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        } else {
            result.unwrap();
        }
    }
}
