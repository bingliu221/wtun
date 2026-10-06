use std::{
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{io::AsyncReadExt, sync::mpsc, time::timeout};
use tracing::{debug, info, warn};
pub use wtransport::{Connection, RecvStream, SendStream, VarInt};

pub mod tcp;
pub mod token;
pub mod udp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proxy {
    pub name: String,
    pub address: String,
    pub udp: bool,
}

impl std::str::FromStr for Proxy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // [name@]host:port[/tcp|udp]
        let (name_opt, rest) = match s.find('@') {
            Some(i) => (Some(&s[..i]), &s[i + 1..]),
            None => (None, s),
        };
        let (address, udp) = match rest.rfind('/') {
            Some(i) => {
                let proto = &rest[i + 1..];
                let addr = &rest[..i];
                match proto {
                    "tcp" => (addr, false),
                    "udp" => (addr, true),
                    other => {
                        return Err(format!("unknown protocol '{}' in '{}', expected tcp or udp", other, s));
                    }
                }
            }
            None => (rest, false),
        };
        if !address.contains(':') {
            return Err(format!("invalid proxy '{}': expected host:port", s));
        }
        let name = name_opt.unwrap_or("").to_string();
        if !name.is_empty() {
            if name.len() > 8 {
                return Err(format!("proxy name '{}' exceeds 8 characters", name));
            }
            let mut chars = name.chars();
            let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_') && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !valid {
                return Err(format!("proxy name '{}' is invalid: must start with a letter or '_', and contain only letters, digits, or '_'", name));
            }
        }
        Ok(Proxy { name, address: address.to_string(), udp })
    }
}

pub fn parse_proxies<S: AsRef<str>>(values: &[S]) -> Result<Vec<Proxy>, String> {
    values.iter().map(|s| s.as_ref().parse::<Proxy>()).collect()
}

/// Timeout for opening a bidirectional stream on an existing connection.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for writing/reading the proxy-name header on a new stream.
const STREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

/// Capacity of the queue of accepted-but-not-yet-dispatched streams.
const ACCEPT_QUEUE: usize = 128;

/// Lock a std mutex, ignoring poisoning (the protected data stays consistent
/// for all of our short, non-panicking critical sections).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone)]
pub struct ConnSet {
    conns: Arc<Mutex<Vec<Connection>>>,
    cursor: Arc<AtomicUsize>,
    tx: mpsc::Sender<(SendStream, RecvStream)>,
}

/// Receiving half: yields streams opened by the remote peer on any connection of the [`ConnSet`].
pub struct StreamAcceptor {
    rx: mpsc::Receiver<(SendStream, RecvStream)>,
}

impl ConnSet {
    pub fn new() -> (Self, StreamAcceptor) {
        let (tx, rx) = mpsc::channel(ACCEPT_QUEUE);
        (Self { conns: Default::default(), cursor: Arc::new(AtomicUsize::new(0)), tx }, StreamAcceptor { rx })
    }

    /// Remove all connections from the set and close them so that their
    /// background accept tasks terminate promptly instead of leaking.
    pub fn clear(&self) {
        let mut conns = lock(&self.conns);
        let n = conns.len();
        for conn in conns.drain(..) {
            conn.close(VarInt::from_u32(0), b"replaced");
        }
        debug!(removed = n, "connection set cleared");
    }

    pub fn add(&self, conn: Connection) {
        let tx = self.tx.clone();
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            debug!("start accepting streams from connection");
            while let Ok(stream) = conn_clone.accept_bi().await {
                debug!("stream accepted from connection");
                if tx.send(stream).await.is_err() {
                    break;
                }
            }
            debug!("connection closed, stop accepting streams");
        });
        let mut conns = lock(&self.conns);
        conns.push(conn);
        info!(total = conns.len(), "new connection added to set");
    }

    pub fn remove(&self, conn: &Connection) {
        let mut conns = lock(&self.conns);
        let before = conns.len();
        conns.retain(|c| c.stable_id() != conn.stable_id());
        debug!(remaining = conns.len(), removed = before - conns.len(), "connection removed from set");
    }

    pub async fn open_connection(&self) -> Option<(SendStream, RecvStream)> {
        loop {
            let conn = {
                let conns = lock(&self.conns);
                if conns.is_empty() {
                    debug!("no available connections in set");
                    return None;
                }
                let i = self.cursor.fetch_add(1, Ordering::Relaxed) % conns.len();
                conns[i].clone()
            };

            let open_result = timeout(STREAM_OPEN_TIMEOUT, async {
                match conn.open_bi().await {
                    Ok(opening) => opening.await.ok(),
                    Err(_) => None,
                }
            })
            .await;

            match open_result {
                Ok(Some(stream)) => return Some(stream),
                _ => {
                    let mut conns = lock(&self.conns);
                    let before = conns.len();
                    conns.retain(|c| c.stable_id() != conn.stable_id());
                    warn!(remaining = conns.len(), removed = before - conns.len(), "dead connection removed from set");
                }
            }
        }
    }

    pub async fn open_named(&self, name: &str) -> Option<(SendStream, RecvStream)> {
        let name_bytes = name.as_bytes();
        let Ok(len) = u8::try_from(name_bytes.len()) else {
            warn!(%name, "proxy name too long (max 255 bytes)");
            return None;
        };
        let (mut tx, rx) = self.open_connection().await?;
        let mut header = Vec::with_capacity(1 + name_bytes.len());
        header.push(len);
        header.extend_from_slice(name_bytes);
        match timeout(STREAM_HEADER_TIMEOUT, tx.write_all(&header)).await {
            Ok(Ok(_)) => Some((tx, rx)),
            _ => {
                warn!(%name, "failed to write proxy name header");
                None
            }
        }
    }
}

impl StreamAcceptor {
    pub async fn accept_connection(&mut self) -> Option<(SendStream, RecvStream)> {
        let result = self.rx.recv().await;
        if result.is_none() {
            warn!("accept_connection: stream channel closed");
        } else {
            debug!("accepted incoming stream from connection set");
        }
        result
    }

    pub async fn accept_named(&mut self) -> Option<(String, SendStream, RecvStream)> {
        let (tx, mut rx) = self.accept_connection().await?;
        let read_header = async {
            let len = rx.read_u8().await.map_err(|e| e.to_string())? as usize;
            let mut bytes = vec![0u8; len];
            rx.read_exact(&mut bytes).await.map_err(|e| e.to_string())?;
            String::from_utf8(bytes).map_err(|_| "proxy name is not valid UTF-8".to_string())
        };
        match timeout(STREAM_HEADER_TIMEOUT, read_header).await {
            Ok(Ok(name)) => Some((name, tx, rx)),
            Ok(Err(e)) => {
                warn!(error = %e, "failed to read proxy name");
                None
            }
            Err(_) => {
                warn!("timed out reading proxy name header");
                None
            }
        }
    }
}
