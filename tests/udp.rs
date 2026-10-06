use std::net::SocketAddr;
use wtun::udp::{encode_frame, parse_frame};

fn roundtrip(addr: SocketAddr, payload: &[u8]) {
    let frame = encode_frame(addr, payload).unwrap();
    let len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    assert_eq!(len, frame.len() - 2);
    let (a, off) = parse_frame(&frame[2..]).unwrap();
    assert_eq!(a, addr);
    assert_eq!(&frame[2 + off..], payload);
}

#[test]
fn frame_roundtrip_v4() {
    roundtrip("1.2.3.4:5678".parse().unwrap(), b"hello");
    roundtrip("1.2.3.4:5678".parse().unwrap(), b"");
}

#[test]
fn frame_roundtrip_v6() {
    roundtrip("[::1]:9".parse().unwrap(), b"world");
}

#[test]
fn oversized_frame_rejected() {
    let addr: SocketAddr = "1.2.3.4:1".parse().unwrap();
    assert!(encode_frame(addr, &vec![0u8; 65535 - 7 + 1]).is_none());
    assert!(encode_frame(addr, &vec![0u8; 65535 - 7]).is_some());
}

#[test]
fn parse_rejects_garbage() {
    assert!(parse_frame(&[]).is_err());
    assert!(parse_frame(&[4, 1, 2]).is_err());
    assert!(parse_frame(&[6, 0, 0]).is_err());
    assert!(parse_frame(&[9, 0, 0, 0, 0, 0, 0, 0]).is_err());
}
