use super::*;

#[test]
fn error_codes_round_trip_known_and_unknown_values() {
    assert_eq!(ErrorCode::from_raw(7007), ErrorCode::IdentityCode);
    assert_eq!(ErrorCode::IdentityCode.raw(), 7007);
    assert_eq!(ErrorCode::from_raw(4002), ErrorCode::BadSignature);
    assert_eq!(ErrorCode::from_raw(9999), ErrorCode::Other(9999));
    assert_eq!(ErrorCode::Other(9999).raw(), 9999);
}
