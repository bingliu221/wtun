use std::time::Duration;

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::{ConnSet, RecvStream, SendStream};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Retry up to 5 seconds total to allow the tunnel client to reconnect.
const TUNNEL_RETRY_COUNT: usize = 25;
const TUNNEL_RETRY_INTERVAL: Duration = Duration::from_millis(200);

async fn stream_copy(mut tx: SendStream, mut rx: RecvStream, conn: TcpStream) {
    let peer = conn.peer_addr().ok();
    debug!(peer = ?peer, "TCP stream_copy started");

    if let Err(e) = conn.set_nodelay(true) {
        debug!(peer = ?peer, error = %e, "failed to set TCP_NODELAY");
    }

    let (mut rh, mut wh) = conn.into_split();

    let up = async {
        match tokio::io::copy(&mut rh, &mut tx).await {
            Ok(n) => {
                debug!(peer = ?peer, bytes = n, "upstream copy done, shutting down tunnel write");
                if let Err(e) = tx.shutdown().await {
                    debug!(peer = ?peer, error = %e, "tunnel write shutdown error");
                }
            }
            Err(e) => debug!(peer = ?peer, error = %e, "upstream copy error"),
        }
    };

    let down = async {
        match tokio::io::copy(&mut rx, &mut wh).await {
            Ok(n) => {
                debug!(peer = ?peer, bytes = n, "downstream copy done, shutting down TCP write");
                if let Err(e) = wh.shutdown().await {
                    debug!(peer = ?peer, error = %e, "TCP write shutdown error");
                }
            }
            Err(e) => debug!(peer = ?peer, error = %e, "downstream copy error"),
        }
    };

    tokio::join!(up, down);
    debug!(peer = ?peer, "TCP stream_copy finished");
}

pub async fn handle_stream(tx: SendStream, rx: RecvStream, target: String) {
    debug!(%target, "connecting to TCP target");
    match timeout(CONNECT_TIMEOUT, TcpStream::connect(&target)).await {
        Ok(Ok(stream)) => {
            debug!(%target, "connected to target, starting stream copy");
            stream_copy(tx, rx, stream).await;
            debug!(%target, "stream copy done");
        }
        Ok(Err(e)) => warn!(%target, error = %e, "failed to connect to TCP target"),
        Err(_) => warn!(%target, timeout_secs = CONNECT_TIMEOUT.as_secs(), "TCP connect timed out"),
    }
}

pub async fn handle_entry(conns: ConnSet, entry: String, name: String) -> Result<(), std::io::Error> {
    let lis = TcpListener::bind(&entry).await?;
    info!(%entry, %name, "TCP entry listening");
    loop {
        match lis.accept().await {
            Ok((stream, addr)) => {
                debug!(%entry, peer = %addr, "accepted TCP connection from client");

                let tunnel = {
                    let mut result = None;
                    for attempt in 1..=TUNNEL_RETRY_COUNT {
                        if let Some(t) = conns.open_named(&name).await {
                            result = Some(t);
                            break;
                        }
                        debug!(%entry, peer = %addr, attempt, "no tunnel available, retrying");
                        sleep(TUNNEL_RETRY_INTERVAL).await;
                    }
                    result
                };

                if let Some((tx, rx)) = tunnel {
                    debug!(%entry, peer = %addr, %name, "tunnel acquired, starting stream copy");
                    tokio::spawn(stream_copy(tx, rx, stream));
                } else {
                    warn!(%entry, peer = %addr, "no tunnel available after retries, dropping connection");
                }
            }
            Err(err) => {
                use std::io::ErrorKind::*;
                match err.kind() {
                    ConnectionAborted | ConnectionReset | TimedOut | WouldBlock | Interrupted => {
                        debug!(%entry, error = %err, "transient TCP accept error, continuing");
                    }
                    _ => {
                        warn!(%entry, error = %err, "fatal TCP accept error, exiting");
                        return Err(err);
                    }
                }
            }
        }
    }
}
