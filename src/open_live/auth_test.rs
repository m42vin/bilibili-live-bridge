use super::*;

const OFFICIAL_CANONICAL: &str = "\
x-bili-accesskeyid:xxxx
x-bili-content-md5:fa6837e35b2f591865b288dfd859ce9d
x-bili-signature-method:HMAC-SHA256
x-bili-signature-nonce:ad184c09-095f-91c3-0849-230dd3744045
x-bili-signature-version:1.0
x-bili-timestamp:1624594467";

const OFFICIAL_AUTHORIZATION: &str =
    "a81c50234b6bbf15bc56e387ee4f19c6f871af2f70b837dc56db16517d4a341f";

#[test]
fn authorization_matches_the_official_sample() {
    assert!(!OFFICIAL_CANONICAL.ends_with('\n'));
    assert_eq!(
        hmac_sha256_hex("JzOzZfSHeYYnAMZ", OFFICIAL_CANONICAL),
        OFFICIAL_AUTHORIZATION
    );
}

#[test]
fn canonical_string_sorts_only_x_bili_headers() {
    let canonical = canonical_string(&[
        ("x-bili-timestamp", "1624594467"),
        ("Content-Type", "application/json"),
        ("x-bili-signature-version", "1.0"),
        ("x-bili-accesskeyid", "xxxx"),
        (
            "x-bili-signature-nonce",
            "ad184c09-095f-91c3-0849-230dd3744045",
        ),
        ("Authorization", "ignored"),
        ("x-bili-content-md5", "fa6837e35b2f591865b288dfd859ce9d"),
        ("x-bili-signature-method", "HMAC-SHA256"),
    ]);

    assert_eq!(canonical, OFFICIAL_CANONICAL);
}

#[test]
fn sign_binds_the_body_md5_into_the_authorization() {
    let body = r#"{"game_id":"g1"}"#;
    let signed = sign("key-id", "super-secret", body, 1_624_594_467, "nonce-1");

    assert_eq!(signed.content_md5, "7b104299c85be159545209b2a73257bf");
    let canonical = canonical_string(&[
        ("x-bili-accesskeyid", "key-id"),
        ("x-bili-content-md5", signed.content_md5.as_str()),
        ("x-bili-signature-method", SIGNATURE_METHOD),
        ("x-bili-signature-nonce", "nonce-1"),
        ("x-bili-signature-version", SIGNATURE_VERSION),
        ("x-bili-timestamp", "1624594467"),
    ]);
    assert_eq!(
        signed.authorization,
        hmac_sha256_hex("super-secret", &canonical)
    );
    assert_eq!(signed.nonce, "nonce-1");
    assert_eq!(signed.timestamp, "1624594467");
}
