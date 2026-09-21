use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use tokio::{
    io::AsyncReadExt,
    sync::{Mutex, mpsc},
};
use tracing::{debug, info, warn};
pub use wtransport::{Connection, RecvStream, SendStream};

pub mod tcp;
pub mod udp;

#[derive(Debug, Clone)]
pub struct Proxy {
    pub name: String,
    pub address: String,
    pub udp: bool,
}

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

    pub async fn clear(&self) {
        let mut conns = self.conns.lock().await;
        let n = conns.len();
        conns.clear();
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

            if let Ok(opening) = conn.open_bi().await {
                if let Ok(stream) = opening.await {
                    return Some(stream);
                }
            }

            let mut conns = self.conns.lock().await;
            let before = conns.len();
            conns.retain(|c| c.stable_id() != conn.stable_id());
            warn!(remaining = conns.len(), removed = before - conns.len(), "dead connection removed from set");
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
        if tx.write_all(&header).await.is_ok() {
            Some((tx, rx))
        } else {
            warn!(%name, "failed to write proxy name header");
            None
        }
    }

    pub async fn accept_named(&self) -> Option<(String, SendStream, RecvStream)> {
        let (tx, mut rx) = self.accept_connection().await?;
        let len = match rx.read_u8().await {
            Ok(n) => n as usize,
            Err(e) => {
                warn!(error = %e, "failed to read proxy name length");
                return None;
            }
        };
        let mut bytes = vec![0u8; len];
        if let Err(e) = rx.read_exact(&mut bytes).await {
            warn!(error = %e, "failed to read proxy name");
            return None;
        }
        let name = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                warn!("proxy name is not valid UTF-8");
                return None;
            }
        };
        Some((name, tx, rx))
    }
}
