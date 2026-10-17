use serde_json::json;

use super::*;
use crate::resource::ResourcePolicy;

#[test]
fn serialization_limits_release_partial_reservations_and_apply_shared_capacity() {
    let budget = ResourceBudget::new(ResourcePolicy {
        max_connection_send_bytes: 64,
        max_send_bytes: 64,
        ..Default::default()
    })
    .unwrap();
    let output = Output::new((), budget.clone());
    assert!(matches!(
        output.prepare(&json!({"value":"x".repeat(64)})),
        Err(Exit::Oversized)
    ));
    assert_eq!(budget.snapshot().sending_bytes, 0);
    let prepared = output.prepare(&json!({"value":"ok"})).unwrap();
    assert_eq!(budget.snapshot().sending_bytes, 14);
    assert!(matches!(output.prepare(&json!(1)), Err(Exit::Resource(_))));
    drop(prepared);
    assert_eq!(budget.snapshot().sending_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_flush_holds_bytes_until_the_sink_is_destroyed() {
    use crate::downstream::protocol::ServerMessage;
    use crate::downstream::{IO_TIMEOUT, PeerState, send_json};
    use futures_util::Sink;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::sync::watch;
    struct Blocked {
        budget: ResourceBudget,
        message: Option<Message>,
    }
    impl Sink<Message> for Blocked {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, message: Message) -> io::Result<()> {
            self.message = Some(message);
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl Drop for Blocked {
        fn drop(&mut self) {
            assert!(self.message.is_some());
            assert!(
                self.budget.snapshot().sending_bytes > 0,
                "缓冲写端销毁前不能归还字节"
            );
        }
    }
    let budget = ResourceBudget::new(ResourcePolicy {
        max_connection_send_bytes: 128,
        max_send_bytes: 128,
        ..Default::default()
    })
    .unwrap();
    let mut output = Output::new(
        Blocked {
            budget: budget.clone(),
            message: None,
        },
        budget.clone(),
    );
    let (_peer_tx, mut peer) = watch::channel(PeerState::Connected);
    let (_stop_tx, mut stop) = watch::channel(false);
    let before = tokio::time::Instant::now();
    assert_eq!(
        send_json(
            &mut output,
            &ServerMessage::Status { state: "connected" },
            &mut peer,
            &mut stop
        )
        .await,
        Err(Exit::Failed)
    );
    assert_eq!(before.elapsed(), IO_TIMEOUT);
    assert!(budget.snapshot().sending_bytes > 0);
    drop(output);
    assert_eq!(budget.snapshot().sending_bytes, 0);
}
