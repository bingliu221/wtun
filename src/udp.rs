use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::AsyncReadExt,
    net::{UdpSocket, lookup_host},
    sync::{Mutex, mpsc},
    time::{sleep, timeout},
};
use tracing::{debug, info, trace, warn};

use crate::{ConnSet, RecvStream, SendStream};

/// Idle timeout for a per-client UDP session on the server side.
const UDP_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct PacketParseError;

pub struct Packet {
    pub addr: SocketAddr,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(addr: SocketAddr, payload: &[u8]) -> Self {
        Self { addr, payload: payload.to_vec() }
    }

    /// Returns an unspecified local [`SocketAddr`] of the same IP family as `target`,
    /// suitable for binding a UDP socket that will communicate with `target`.
    pub fn local_bind_addr(target: &SocketAddr) -> SocketAddr {
        if target.is_ipv4() {
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into()
        } else {
            SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into()
        }
    }

    /// Encode this packet into a length-prefixed wire frame using a single allocation.
    ///
    /// Wire format:
    /// ```text
    /// [ u16 data_len ][ u8 family (4|6) ][ 4|16 bytes ip ][ u16 port ][ payload ]
    /// ```
    pub fn into_framed(self) -> Vec<u8> {
        let ip_len = if self.addr.is_ipv4() { 1 + 4 } else { 1 + 16 };
        let data_len = ip_len + 2 + self.payload.len(); // ip + port (2 bytes) + payload
        let mut buf = Vec::with_capacity(2 + data_len);
        buf.extend_from_slice(&(data_len as u16).to_be_bytes()); // length prefix
        match self.addr.ip() {
            IpAddr::V4(ip) => {
                buf.push(4);
                buf.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                buf.push(6);
                buf.extend_from_slice(&ip.octets());
            }
        }
        buf.extend_from_slice(&self.addr.port().to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

impl TryFrom<&[u8]> for Packet {
    type Error = PacketParseError;

    fn try_from(buf: &[u8]) -> Result<Self, Self::Error> {
        if buf.is_empty() {
            return Err(PacketParseError);
        }

        match buf[0] {
            4 if buf.len() >= 7 => Ok(Packet {
                addr: SocketAddr::new(Ipv4Addr::from(<[u8; 4]>::try_from(&buf[1..5]).unwrap()).into(), u16::from_be_bytes([buf[5], buf[6]])),
                payload: buf[7..].to_vec(),
            }),
            6 if buf.len() >= 19 => Ok(Packet {
                addr: SocketAddr::new(Ipv6Addr::from(<[u8; 16]>::try_from(&buf[1..17]).unwrap()).into(), u16::from_be_bytes([buf[17], buf[18]])),
                payload: buf[19..].to_vec(),
            }),

            _ => Err(PacketParseError),
        }
    }
}

async fn recv_packet(rx: &mut RecvStream) -> Result<Option<Packet>, std::io::Error> {
    let size = rx.read_u16().await? as usize;
    let mut buf = vec![0u8; size];
    match rx.read_exact(&mut buf).await {
        Ok(_) => {
            let pkt = Packet::try_from(&buf[..]);
            if pkt.is_err() {
                warn!(size, "received packet failed to parse");
            }
            Ok(pkt.ok())
        }
        Err(err) => Err(std::io::Error::other(err.to_string())),
    }
}

async fn handle_target_stream(mut tx: SendStream, mut rx: RecvStream, target: SocketAddr) {
    info!(%target, "target stream started");
    let sockets = Arc::new(Mutex::new(HashMap::<SocketAddr, Arc<UdpSocket>>::new()));
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    tokio::spawn(async move {
        while let Some(data) = write_rx.recv().await {
            if tx.write_all(&data).await.is_err() {
                break;
            }
        }
    });

    while let Ok(Some(pkt)) = recv_packet(&mut rx).await {
        trace!(client = %pkt.addr, payload_len = pkt.payload.len(), "→ target: received packet from tunnel");
        let socket = {
            let mut map = sockets.lock().await;
            if let Some(sock) = map.get(&pkt.addr) {
                sock.clone()
            } else {
                let laddr = Packet::local_bind_addr(&target);
                let sock = Arc::new(UdpSocket::bind(laddr).await.unwrap());
                info!(client = %pkt.addr, local = ?sock.local_addr().ok(), %target, "new UDP session");
                map.insert(pkt.addr, sock.clone());

                tokio::spawn({
                    let addr = pkt.addr;
                    let write_tx = write_tx.clone();
                    let sockets = sockets.clone();
                    let socket = sock.clone();
                    async move {
                        let mut buf = [0u8; 65536];
                        loop {
                            match timeout(UDP_SESSION_IDLE_TIMEOUT, socket.recv_from(&mut buf)).await {
                                Ok(Ok((n, remote))) => {
                                    if remote != target {
                                        trace!(client = %addr, %remote, %target, "← ignored: packet not from target");
                                        continue;
                                    }
                                    trace!(client = %addr, payload_len = n, "← target: forwarding packet to tunnel");
                                    let _ = write_tx.send(Packet::new(addr, &buf[..n]).into_framed());
                                }
                                Ok(Err(e)) => {
                                    warn!(client = %addr, error = %e, "UDP recv_from error");
                                    break;
                                }
                                Err(_) => {
                                    debug!(client = %addr, idle_secs = UDP_SESSION_IDLE_TIMEOUT.as_secs(), "UDP session timed out (idle)");
                                    break;
                                }
                            }
                        }
                        info!(client = %addr, "UDP session removed");
                        sockets.lock().await.remove(&addr);
                    }
                });

                sock
            }
        };

        match socket.send_to(&pkt.payload, target).await {
            Ok(n) => trace!(client = %pkt.addr, sent = n, %target, "→ target: sent packet"),
            Err(e) => {
                debug!(client = %pkt.addr, %target, error = %e, "failed to send UDP packet to target")
            }
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

pub async fn handle_entry(conns: ConnSet, entry: String, name: String) -> Result<(), std::io::Error> {
    let socket = Arc::new(UdpSocket::bind(&entry).await?);
    info!(%entry, %name, "UDP entry listening");
    loop {
        if let Some((mut tx, mut rx)) = conns.open_named(&name).await {
            info!(%entry, %name, "tunnel connection established, starting proxy");
            let mut t1 = tokio::spawn({
                let socket = socket.clone();
                async move {
                    while let Ok(Some(pkt)) = recv_packet(&mut rx).await {
                        trace!(dst = %pkt.addr, payload_len = pkt.payload.len(), "← tunnel: forwarding packet to local");
                        match socket.send_to(&pkt.payload, pkt.addr).await {
                            Ok(n) => trace!(dst = %pkt.addr, sent = n, "local send ok"),
                            Err(e) => {
                                debug!(dst = %pkt.addr, error = %e, "failed to send to local")
                            }
                        }
                    }
                    debug!("tunnel recv stream ended");
                }
            });

            let mut t2 = tokio::spawn({
                let socket = socket.clone();
                async move {
                    let mut buf = [0u8; 65536];
                    while let Ok((n, addr)) = socket.recv_from(&mut buf).await {
                        trace!(src = %addr, payload_len = n, "→ tunnel: received packet from local");
                        let data = Packet::new(addr, &buf[..n]).into_framed();
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
