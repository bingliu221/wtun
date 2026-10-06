/// Percent-encode everything except RFC 3986 unreserved characters.
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Decode a percent-encoded string; returns `None` on malformed escapes or invalid UTF-8.
pub fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Constant-time (for equal lengths) byte comparison to avoid timing side channels.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn extract_token_from_path(path: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=')
            && k == "token"
        {
            return percent_decode(v);
        }
    }
    None
}
