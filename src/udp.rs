use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::AsyncReadExt,
    net::{UdpSocket, lookup_host},
    sync::mpsc::{self, error::TrySendError},
    time::{sleep, timeout},
};
use tracing::{debug, info, trace, warn};

use crate::{ConnSet, RecvStream, SendStream, lock};

/// Idle timeout for a per-client UDP session on the server side.
const UDP_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of encoded frames queued towards the tunnel; extra packets are dropped (UDP semantics).
const WRITE_QUEUE: usize = 1024;

/// Receive buffer size for UDP sockets (max UDP datagram).
const UDP_BUF_SIZE: usize = 65536;

#[derive(Debug, Clone, Copy)]
pub struct PacketParseError;

/// Returns an unspecified local [`SocketAddr`] of the same IP family as `target`,
/// suitable for binding a UDP socket that will communicate with `target`.
pub fn local_bind_addr(target: &SocketAddr) -> SocketAddr {
    if target.is_ipv4() { SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into() } else { SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into() }
}

/// Encode a packet into a length-prefixed wire frame using a single allocation.
///
/// Wire format:
/// ```text
/// [ u16 data_len ][ u8 family (4|6) ][ 4|16 bytes ip ][ u16 port ][ payload ]
/// ```
///
/// Returns `None` if the frame would not fit in the `u16` length prefix.
pub fn encode_frame(addr: SocketAddr, payload: &[u8]) -> Option<Vec<u8>> {
    let ip_len = if addr.is_ipv4() { 1 + 4 } else { 1 + 16 };
    let data_len = ip_len + 2 + payload.len(); // ip + port (2 bytes) + payload
    let prefix = u16::try_from(data_len).ok()?;
    let mut buf = Vec::with_capacity(2 + data_len);
    buf.extend_from_slice(&prefix.to_be_bytes());
    match addr.ip() {
        IpAddr::V4(ip) => {
            buf.push(4);
            buf.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            buf.push(6);
            buf.extend_from_slice(&ip.octets());
        }
    }
    buf.extend_from_slice(&addr.port().to_be_bytes());
    buf.extend_from_slice(payload);
    Some(buf)
}

/// Parse a frame body (without the length prefix). Returns the address and the
/// offset at which the payload starts in `buf`.
pub fn parse_frame(buf: &[u8]) -> Result<(SocketAddr, usize), PacketParseError> {
    match buf.first() {
        Some(4) if buf.len() >= 7 => {
            let ip = Ipv4Addr::new(buf[1], buf[2], buf[3], buf[4]);
            Ok((SocketAddr::new(ip.into(), u16::from_be_bytes([buf[5], buf[6]])), 7))
        }
        Some(6) if buf.len() >= 19 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&buf[1..17]);
            Ok((SocketAddr::new(Ipv6Addr::from(octets).into(), u16::from_be_bytes([buf[17], buf[18]])), 19))
        }
        _ => Err(PacketParseError),
    }
}

/// Read one frame into `buf` (reused across calls).
///
/// * `Err(_)` – the stream is broken/closed.
/// * `Ok(None)` – a malformed frame was skipped; the stream is still usable.
/// * `Ok(Some((addr, off)))` – payload is `&buf[off..]`.
async fn recv_packet(rx: &mut RecvStream, buf: &mut Vec<u8>) -> io::Result<Option<(SocketAddr, usize)>> {
    let size = rx.read_u16().await? as usize;
    buf.clear();
    buf.resize(size, 0);
    rx.read_exact(&mut buf[..]).await.map_err(|e| io::Error::other(e.to_string()))?;
    match parse_frame(buf) {
        Ok(v) => Ok(Some(v)),
        Err(_) => {
            warn!(size, "received packet failed to parse");
            Ok(None)
        }
    }
}

type SessionMap = Arc<Mutex<HashMap<SocketAddr, Arc<UdpSocket>>>>;

async fn session_task(socket: Arc<UdpSocket>, client: SocketAddr, target: SocketAddr, write_tx: mpsc::Sender<Vec<u8>>, sockets: SessionMap) {
    let mut buf = vec![0u8; UDP_BUF_SIZE];
    loop {
        match timeout(UDP_SESSION_IDLE_TIMEOUT, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, remote))) => {
                if remote != target {
                    trace!(%client, %remote, %target, "← ignored: packet not from target");
                    continue;
                }
                trace!(%client, payload_len = n, "← target: forwarding packet to tunnel");
                let Some(frame) = encode_frame(client, &buf[..n]) else {
                    debug!(%client, payload_len = n, "dropping oversized packet");
                    continue;
                };
                match write_tx.try_send(frame) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => trace!(%client, "tunnel write queue full, dropping packet"),
                    Err(TrySendError::Closed(_)) => break,
                }
            }
            Ok(Err(e)) => {
                warn!(%client, error = %e, "UDP recv_from error");
                break;
            }
            Err(_) => {
                debug!(%client, idle_secs = UDP_SESSION_IDLE_TIMEOUT.as_secs(), "UDP session timed out (idle)");
                break;
            }
        }
    }
    info!(%client, "UDP session removed");
    // Only remove our own entry: a newer session for the same client may already exist.
    let mut map = lock(&sockets);
    if map.get(&client).is_some_and(|s| Arc::ptr_eq(s, &socket)) {
        map.remove(&client);
    }
}

async fn handle_target_stream(mut tx: SendStream, mut rx: RecvStream, target: SocketAddr) {
    info!(%target, "target stream started");
    let sockets: SessionMap = Default::default();
    let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(WRITE_QUEUE);

    tokio::spawn(async move {
        while let Some(data) = write_rx.recv().await {
            if tx.write_all(&data).await.is_err() {
                break;
            }
        }
    });

    let mut buf = Vec::new();
    loop {
        let (client, off) = match recv_packet(&mut rx, &mut buf).await {
            Ok(Some(v)) => v,
            Ok(None) => continue,
            Err(_) => break,
        };
        let payload = &buf[off..];
        trace!(%client, payload_len = payload.len(), "→ target: received packet from tunnel");

        let existing = lock(&sockets).get(&client).cloned();
        let socket = match existing {
            Some(s) => s,
            None => {
                let sock = match UdpSocket::bind(local_bind_addr(&target)).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        warn!(%client, %target, error = %e, "failed to bind UDP socket, dropping packet");
                        continue;
                    }
                };
                info!(%client, local = ?sock.local_addr().ok(), %target, "new UDP session");
                lock(&sockets).insert(client, sock.clone());
                tokio::spawn(session_task(sock.clone(), client, target, write_tx.clone(), sockets.clone()));
                sock
            }
        };

        match socket.send_to(payload, target).await {
            Ok(n) => trace!(%client, sent = n, %target, "→ target: sent packet"),
            Err(e) => debug!(%client, %target, error = %e, "failed to send UDP packet to target"),
        }
    }
    info!(%target, "target stream ended");
}

pub async fn handle_stream(tx: SendStream, rx: RecvStream, target: String) {
    match lookup_host(&target).await {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                debug!(%target, resolved = %addr, "DNS resolved for UDP target");
                handle_target_stream(tx, rx, addr).await;
            } else {
                warn!(%target, "DNS resolved but no addresses found");
            }
        }
        Err(e) => warn!(%target, error = %e, "failed to resolve UDP target address"),
    }
}

pub async fn handle_entry(conns: ConnSet, entry: String, name: String) -> Result<(), io::Error> {
    let socket = Arc::new(UdpSocket::bind(&entry).await?);
    info!(%entry, %name, "UDP entry listening");
    loop {
        if let Some((mut tx, mut rx)) = conns.open_named(&name).await {
            info!(%entry, %name, "tunnel connection established, starting proxy");
            let mut t1 = tokio::spawn({
                let socket = socket.clone();
                async move {
                    let mut buf = Vec::new();
                    loop {
                        let (addr, off) = match recv_packet(&mut rx, &mut buf).await {
                            Ok(Some(v)) => v,
                            Ok(None) => continue,
                            Err(_) => break,
                        };
                        let payload = &buf[off..];
                        trace!(dst = %addr, payload_len = payload.len(), "← tunnel: forwarding packet to local");
                        match socket.send_to(payload, addr).await {
                            Ok(n) => trace!(dst = %addr, sent = n, "local send ok"),
                            Err(e) => debug!(dst = %addr, error = %e, "failed to send to local"),
                        }
                    }
                    debug!("tunnel recv stream ended");
                }
            });

            let mut t2 = tokio::spawn({
                let socket = socket.clone();
                async move {
                    let mut buf = vec![0u8; UDP_BUF_SIZE];
                    while let Ok((n, addr)) = socket.recv_from(&mut buf).await {
                        trace!(src = %addr, payload_len = n, "→ tunnel: received packet from local");
                        let Some(data) = encode_frame(addr, &buf[..n]) else {
                            debug!(src = %addr, payload_len = n, "dropping oversized packet");
                            continue;
                        };
                        if let Err(e) = tx.write_all(&data).await {
                            warn!(src = %addr, error = %e, "failed to write to tunnel");
                            break;
                        }
                    }
                    debug!("local recv loop ended");
                }
            });

            tokio::select! {
                _ = &mut t1 => { t2.abort(); }
                _ = &mut t2 => { t1.abort(); }
            }
            info!(%entry, "tunnel connection lost, will retry");
        } else {
            trace!(%entry, "no tunnel connection available, waiting 1s");
            sleep(Duration::from_secs(1)).await;
        }
    }
}
