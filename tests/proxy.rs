use wtun::parse_proxies;

#[test]
fn proxies_parse_ok() {
    let p = parse_proxies(&["web@127.0.0.1:80/tcp", "127.0.0.1:53/udp", "a_1@h:1"]).unwrap();
    assert_eq!(p[0].name, "web");
    assert!(!p[0].udp);
    assert_eq!(p[1].name, "");
    assert!(p[1].udp);
    assert_eq!(p[2].address, "h:1");
}

#[test]
fn proxies_parse_errors() {
    assert!(parse_proxies(&["host"]).is_err());
    assert!(parse_proxies(&["h:1/icmp"]).is_err());
    assert!(parse_proxies(&["toolongname@h:1"]).is_err());
    assert!(parse_proxies(&["1abc@h:1"]).is_err());
    assert!(parse_proxies(&["a-b@h:1"]).is_err());
}
