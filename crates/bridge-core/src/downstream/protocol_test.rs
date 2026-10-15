use super::*;
use crate::open_live::ws::Danmaku;
use serde_json::{Value, json};
fn subscription_code(text: &str) -> Result<String, &'static str> {
    parse_subscribe(text).map(|request| request.code)
}

#[test]
fn protocol_preserves_known_and_unknown_data() {
    let event = LiveEvent::Danmaku(Danmaku {
        msg: "hello".to_owned(),
        room_id: 7,
        ..Danmaku::default()
    });
    let value = serde_json::to_value(ServerMessage::event(7, &event)).unwrap();
    assert_eq!(value["type"], "event");
    assert_eq!(value["cmd"], "LIVE_OPEN_PLATFORM_DM");
    assert_eq!(value["room_id"], 7);
    assert_eq!(value["data"]["msg"], "hello");
    assert_eq!(value["data"]["uid"], 0);
    for data in [json!({"future_field": [1, true, null]}), Value::Null] {
        let event = LiveEvent::Unknown {
            cmd: "FUTURE_EVENT".into(),
            data: data.clone(),
        };
        assert_eq!(
            serde_json::to_value(ServerMessage::event(7, &event)).unwrap(),
            json!({"type":"event", "room_id":7, "cmd":"FUTURE_EVENT", "data":data})
        );
    }
    let events = [
        LiveEvent::DanmakuMirror(Default::default()),
        LiveEvent::Gift(Default::default()),
        LiveEvent::SuperChat(Default::default()),
        LiveEvent::SuperChatDelete(Default::default()),
        LiveEvent::Guard(Default::default()),
        LiveEvent::Like(Default::default()),
        LiveEvent::RoomEnter(Default::default()),
        LiveEvent::LiveStart(Default::default()),
        LiveEvent::LiveEnd(Default::default()),
        LiveEvent::InteractionEnd(Default::default()),
    ];
    for event in events {
        let value = serde_json::to_value(ServerMessage::event(7, &event)).unwrap();
        assert_eq!(value["cmd"], event.cmd());
        assert!(value["data"].is_object());
    }
    assert_eq!(
        serde_json::to_value(ServerMessage::Closed).unwrap(),
        json!({"type":"closed"})
    );
    assert_eq!(
        subscription_code(r#"{"type":"subscribe","code":"  abc  "}"#),
        Ok("abc".into())
    );
    for invalid in [
        "no-json",
        "{}",
        r#"{"type":"subscribe","code":" "}"#,
        r#"{"type":"subscribe","code":1}"#,
        r#"{"type":"unsubscribe"}"#,
    ] {
        assert!(subscription_code(invalid).is_err());
    }
}

#[test]
fn retry_advice_is_optional_and_keeps_the_existing_error_shape() {
    for advice in [None, Some(1000)] {
        let value = serde_json::to_value(ServerMessage::Error {
            code: "server_busy",
            message: "busy",
            retry_after_ms: advice,
        })
        .unwrap();
        assert_eq!(value["type"], "error");
        assert_eq!(value["code"], "server_busy");
        if let Some(ms) = advice {
            assert_eq!(value["retry_after_ms"], ms);
        } else {
            assert!(value.get("retry_after_ms").is_none());
        }
    }
}
