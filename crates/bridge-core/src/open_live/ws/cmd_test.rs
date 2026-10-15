use serde_json::json;

use super::*;

#[test]
fn parses_each_documented_command() {
    let cases = [
        (CMD_DM, r#"{"room_id":1,"msg":"hi","open_id":"user"}"#),
        (
            CMD_DM_MIRROR,
            r#"{"room_id":1,"msg":"mirror","msg_id":"m1","dm_type":1}"#,
        ),
        (
            CMD_GIFT,
            r#"{"gift_id":9,"gift_name":"star","gift_num":2,"price":1000,"paid":true,"combo_gift":true,"combo_info":{"combo_base_num":5,"combo_count":100,"combo_id":"c","combo_timeout":3},"blind_gift":{"blind_gift_id":10086,"status":true},"anchor_info":{"open_id":"anchor"}}"#,
        ),
        (
            CMD_SUPER_CHAT,
            r#"{"message_id":7,"message":"hello","rmb":30,"msg_id":"sc"}"#,
        ),
        (CMD_SUPER_CHAT_DEL, r#"{"room_id":1,"message_ids":[1,2]}"#),
        (
            CMD_GUARD,
            r#"{"guard_level":3,"guard_num":1,"guard_unit":"月","price":198000,"user_info":{"open_id":"fan"}}"#,
        ),
        (
            CMD_LIKE,
            r#"{"like_text":"为主播点赞了","like_count":114,"msg_id":"like"}"#,
        ),
        (CMD_ROOM_ENTER, r#"{"room_id":1,"uname":"viewer"}"#),
        (
            CMD_LIVE_START,
            r#"{"room_id":22018648,"area_name":"户外","title":"标题"}"#,
        ),
        (CMD_LIVE_END, r#"{"room_id":22018648,"title":"标题"}"#),
        (
            CMD_INTERACTION_END,
            r#"{"game_id":"game-1","timestamp":1714113037}"#,
        ),
    ];

    for (cmd, data) in cases {
        let event = parse_command(envelope(cmd, data).as_bytes()).unwrap();
        assert_eq!(event.cmd(), cmd);
        assert_eq!(event.ends_push(), cmd == CMD_INTERACTION_END);
    }
}

#[test]
fn parses_danmaku_gift_and_interaction_end_fields() {
    let danmaku = parse_command(
        br#"{
            "cmd": "LIVE_OPEN_PLATFORM_DM",
            "data": {
                "room_id": 42,
                "msg": "hello",
                "open_id": "user",
                "is_admin": 1,
                "dm_type": 0,
                "extra": true
            }
        }"#,
    )
    .unwrap();
    let LiveEvent::Danmaku(danmaku) = danmaku else {
        panic!("expected danmaku");
    };
    assert_eq!(danmaku.room_id, 42);
    assert_eq!(danmaku.msg, "hello");
    assert_eq!(danmaku.open_id, "user");
    assert_eq!(danmaku.is_admin, 1);
    assert_eq!(danmaku.uid, 0);

    let gift = parse_command(
        br#"{
            "cmd": "LIVE_OPEN_PLATFORM_SEND_GIFT",
            "data": {
                "gift_name": "star",
                "r_price": 1000,
                "combo_info": null,
                "anchor_info": { "uname": "anchor", "open_id": "a" }
            }
        }"#,
    )
    .unwrap();
    let LiveEvent::Gift(gift) = gift else {
        panic!("expected gift");
    };
    assert_eq!(gift.gift_name, "star");
    assert_eq!(gift.r_price, 1000);
    assert!(gift.combo_info.is_none());
    assert_eq!(gift.anchor_info.unwrap().uname, "anchor");

    let end = parse_command(
        br#"{"cmd":"LIVE_OPEN_PLATFORM_INTERACTION_END","data":{"game_id":"game-1"}}"#,
    )
    .unwrap();
    let LiveEvent::InteractionEnd(end) = end else {
        panic!("expected interaction end");
    };
    assert_eq!(end.game_id, "game-1");
    assert!(LiveEvent::InteractionEnd(end).ends_push());
}

#[test]
fn keeps_unknown_commands_and_rejects_a_missing_cmd() {
    let event = parse_command(br#"{"cmd":"FUTURE_CMD","data":{"n":1}}"#).unwrap();
    assert_eq!(
        event,
        LiveEvent::Unknown {
            cmd: "FUTURE_CMD".to_owned(),
            data: json!({ "n": 1 }),
        }
    );

    let missing = parse_command(br#"{"data":{}}"#).unwrap_err();
    assert_eq!(missing.to_string(), "开放平台推送缺少 cmd");

    let wrong_type =
        parse_command(br#"{"cmd":"LIVE_OPEN_PLATFORM_DM","data":{"room_id":"nope"}}"#).unwrap_err();
    assert!(
        wrong_type
            .to_string()
            .starts_with("LIVE_OPEN_PLATFORM_DM 无法解析")
    );
}

fn envelope(cmd: &str, data: &str) -> String {
    format!(r#"{{"cmd":"{cmd}","data":{data}}}"#)
}
