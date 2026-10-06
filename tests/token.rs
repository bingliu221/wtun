use wtun::token::{ct_eq, extract_token_from_path, percent_encode};

#[test]
fn token_roundtrip_special_chars() {
    for t in ["simple", "a&b=c", "sp ace#x?y", "100%", "中文", ""] {
        let enc = percent_encode(t);
        let path = format!("/wtun?token={enc}");
        assert_eq!(extract_token_from_path(&path).as_deref(), Some(t));
    }
}

#[test]
fn token_extraction_edge_cases() {
    assert_eq!(extract_token_from_path("/wtun"), None);
    assert_eq!(extract_token_from_path("/wtun?a=1&token=x").as_deref(), Some("x"));
    assert_eq!(extract_token_from_path("/wtun?token=%zz"), None);
    assert_eq!(extract_token_from_path("/wtun?token=%+1"), None);
}

#[test]
fn ct_eq_works() {
    assert!(ct_eq(b"abc", b"abc"));
    assert!(!ct_eq(b"abc", b"abd"));
    assert!(!ct_eq(b"abc", b"ab"));
}
