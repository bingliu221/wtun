use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, mpsc},
    time::timeout,
};
use tracing::{debug, info, warn};
pub use wtransport::{Connection, RecvStream, SendStream, VarInt};

pub mod tcp;
pub mod udp;

#[derive(Debug, Clone)]
pub struct Proxy {
    pub name: String,
    pub address: String,
    pub udp: bool,
}

/// Timeout for opening a bidirectional stream on an existing connection.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for writing/reading the proxy-name header on a new stream.
const STREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ConnSet {
    conns: Arc<Mutex<Vec<Connection>>>,
    cursor: Arc<AtomicUsize>,
    tx: mpsc::UnboundedSender<(SendStream, RecvStream)>,
    rx: Arc<Mutex<mpsc::UnboundedReceiver<(SendStream, RecvStream)>>>,
}

impl Default for ConnSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnSet {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self { conns: Default::default(), cursor: Arc::new(AtomicUsize::new(0)), tx, rx: Arc::new(Mutex::new(rx)) }
    }

    /// Remove all connections from the set and close them so that their
    /// background accept tasks terminate promptly instead of leaking.
    pub async fn clear(&self) {
        let mut conns = self.conns.lock().await;
        let n = conns.len();
        for conn in conns.drain(..) {
            conn.close(VarInt::from_u32(0), b"replaced");
        }
        debug!(removed = n, "connection set cleared");
    }

    pub async fn add(&self, conn: Connection) {
        let tx = self.tx.clone();
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            debug!("start accepting streams from connection");
            while let Ok(stream) = conn_clone.accept_bi().await {
                debug!("stream accepted from connection");
                if tx.send(stream).is_err() {
                    break;
                }
            }
            debug!("connection closed, stop accepting streams");
        });
        let mut conns = self.conns.lock().await;
        conns.push(conn);
        info!(total = conns.len(), "new connection added to set");
    }

    pub async fn remove(&self, conn: &Connection) {
        let mut conns = self.conns.lock().await;
        let before = conns.len();
        conns.retain(|c| c.stable_id() != conn.stable_id());
        debug!(remaining = conns.len(), removed = before - conns.len(), "connection removed from set");
    }

    pub async fn open_connection(&self) -> Option<(SendStream, RecvStream)> {
        loop {
            let conn = {
                let conns = self.conns.lock().await;
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
                    let mut conns = self.conns.lock().await;
                    let before = conns.len();
                    conns.retain(|c| c.stable_id() != conn.stable_id());
                    warn!(remaining = conns.len(), removed = before - conns.len(), "dead connection removed from set");
                }
            }
        }
    }

    pub async fn accept_connection(&self) -> Option<(SendStream, RecvStream)> {
        let result = self.rx.lock().await.recv().await;
        if result.is_none() {
            warn!("accept_connection: stream channel closed");
        } else {
            debug!("accepted incoming stream from connection set");
        }
        result
    }

    pub async fn open_named(&self, name: &str) -> Option<(SendStream, RecvStream)> {
        let (mut tx, rx) = self.open_connection().await?;
        let name_bytes = name.as_bytes();
        let mut header = Vec::with_capacity(1 + name_bytes.len());
        header.push(name_bytes.len() as u8);
        header.extend_from_slice(name_bytes);
        match timeout(STREAM_HEADER_TIMEOUT, tx.write_all(&header)).await {
            Ok(Ok(_)) => Some((tx, rx)),
            _ => {
                warn!(%name, "failed to write proxy name header");
                None
            }
        }
    }

    pub async fn accept_named(&self) -> Option<(String, SendStream, RecvStream)> {
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
