use super::*;

#[test]
fn parses_start_result_and_redacts_auth_body() {
    let started = start_result_from_value(
        success_value(
            br#"{
            "code": 0,
            "message": "ok",
            "request_id": "req-1",
            "data": {
                "game_info": { "game_id": "game-1" },
                "websocket_info": {
                    "auth_body": "secret-auth-body",
                    "wss_link": ["wss://a.example", "", "wss://b.example"]
                },
                "anchor_info": { "room_id": 42 }
            }
        }"#,
        )
        .unwrap(),
    )
    .unwrap();

    assert_eq!(started.game_id(), "game-1");
    assert_eq!(started.auth_body(), "secret-auth-body");
    assert_eq!(
        started.wss_link(),
        ["wss://a.example".to_owned(), "wss://b.example".to_owned()]
    );
    assert_eq!(started.anchor().room_id, 42);
    assert_eq!(started.anchor().union_id, "");
    let rendered = format!("{started:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("secret-auth-body"));
    assert!(rendered.contains("game-1"));
}

#[test]
fn rejects_incomplete_start_payloads() {
    let missing_data = start_result_from_value(
        success_value(br#"{"code":0,"message":"ok","data":null}"#).unwrap(),
    )
    .unwrap_err();
    assert_eq!(missing_data.to_string(), "开放平台响应缺少 data");

    let missing_room = start_result_from_value(
        success_value(
            br#"{
            "code": 0,
            "message": "ok",
            "data": {
                "game_info": { "game_id": "game-1" },
                "websocket_info": { "auth_body": "auth", "wss_link": ["wss://a"] },
                "anchor_info": { "room_id": 0 }
            }
        }"#,
        )
        .unwrap(),
    )
    .unwrap_err();
    assert_eq!(missing_room.to_string(), "开放平台响应缺少房间号");
}

#[test]
fn parses_batch_heartbeat_failures() {
    let result = batch_result_from_value(
        success_value(br#"{"code":0,"message":"ok","data":{"failed_game_ids":["gone"]}}"#).unwrap(),
    )
    .unwrap();

    assert_eq!(result.failed_game_ids(), ["gone".to_owned()]);
    assert!(!result.all_succeeded());

    let empty = batch_result_from_value(Value::Null).unwrap();
    assert!(empty.all_succeeded());
}
