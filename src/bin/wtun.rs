use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tokio::time::sleep;
use tracing::{Level, debug, info, warn};
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};
use wtun::{ConnSet, Proxy, tcp, udp};

#[derive(Clone, Debug, Default)]
struct Args {
    serve: Option<String>,
    connect: Option<String>,
    target: Vec<String>,
    token: Option<String>,
}

const HELP: &str = concat!(
    "wtun ",
    env!("CARGO_PKG_VERSION"),
    "\n\n\
Usage: wtun [OPTIONS]

Options:
  -S, --serve <ADDR>    Serve as wtun server, listen for incoming connections
  -C, --connect <ADDR>  Connect to a wtun server, maintain connection
  -P, --proxy <SPEC>    Proxy description (repeatable), format: [name@]host:port[/tcp|udp]
  -K, --token <TOKEN>   Shared secret key for authentication
  -h, --help            Print help
  -V, --version         Print version"
);

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-S" | "--serve" => {
                args.serve = Some(iter.next().ok_or("missing argument for --serve")?);
            }
            "-C" | "--connect" => {
                args.connect = Some(iter.next().ok_or("missing argument for --connect")?);
            }
            "-P" | "--proxy" => {
                args.target.push(iter.next().ok_or("missing argument for --proxy")?);
            }
            "-K" | "--token" => {
                args.token = Some(iter.next().ok_or("missing argument for --token")?);
            }
            "-h" | "--help" => {
                println!("{HELP}");
                std::process::exit(0);
            }
            "-V" | "--version" => {
                println!("wtun {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            _ => return Err(format!("unexpected argument: {arg}")),
        }
    }

    if args.serve.is_none() && args.connect.is_none() {
        return Err("either `serve` or `connect` must be specified".into());
    }
    if args.serve.is_some() && args.connect.is_some() {
        return Err("conflict args `serve` and `connect`".into());
    }

    Ok(args)
}

fn extract_token_from_path(path: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == "token" {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn host(conns: ConnSet, addr: String, token: Option<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sock_addr: SocketAddr = match addr.parse() {
        Ok(sa) => sa,
        Err(_) => tokio::net::lookup_host(&addr).await?.next().ok_or_else(|| format!("failed to resolve address: {}", addr))?,
    };
    let identity = Identity::self_signed(["localhost", "127.0.0.1"])?;
    let config = ServerConfig::builder().with_bind_address(sock_addr).with_identity(identity).keep_alive_interval(Some(Duration::from_secs(3))).build();

    let server = Endpoint::server(config)?;
    info!(addr = %sock_addr, "WebTransport server listening");

    loop {
        let incoming_session = server.accept().await;
        let conns = conns.clone();
        let token = token.clone();
        tokio::spawn(async move {
            match incoming_session.await {
                Ok(session_req) => {
                    if let Some(ref expected) = token {
                        let path = session_req.path();
                        let token_param = extract_token_from_path(path);
                        if token_param.as_deref() != Some(expected.as_str()) {
                            warn!(path, "unauthorized WebTransport connection attempt");
                            session_req.forbidden().await;
                            return;
                        }
                    }

                    match session_req.accept().await {
                        Ok(conn) => {
                            info!("new WebTransport client connected");
                            conns.add(conn.clone()).await;
                            let _ = conn.closed().await;
                            conns.remove(&conn).await;
                            info!("WebTransport client disconnected");
                        }
                        Err(e) => warn!(error = %e, "failed to accept WebTransport session"),
                    }
                }
                Err(e) => warn!(error = %e, "incoming session error"),
            }
        });
    }
}

async fn connect(conns: ConnSet, addr: String, token: Option<String>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_config = ClientConfig::builder().with_bind_default().with_no_cert_validation().build();

    let endpoint = Endpoint::client(client_config)?;

    let mut url = if addr.starts_with("https://") { addr.clone() } else { format!("https://{}/wtun", addr) };
    if let Some(ref t) = token {
        let separator = if url.contains('?') { '&' } else { '?' };
        url.push(separator);
        url.push_str(&format!("token={}", t));
    }

    loop {
        debug!(%url, "attempting to connect to host via WebTransport");
        match endpoint.connect(&url).await {
            Ok(conn) => {
                info!(addr = %addr, "connected to host via WebTransport");
                conns.clear().await;
                conns.add(conn.clone()).await;
                let _ = conn.closed().await;
                conns.remove(&conn).await;
                warn!(addr = %addr, "connection to host lost, reconnecting in 5s");
                sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                debug!(
                    addr = %addr,
                    error = %e,
                    "failed to connect to host, retrying every 5 seconds"
                );
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

fn parse_proxies(values: &Vec<String>) -> Result<Vec<Proxy>, String> {
    values
        .iter()
        .map(|s| {
            // [name@]host:port[/tcp|udp]
            let (name_opt, rest) = match s.find('@') {
                Some(i) => (Some(&s[..i]), &s[i + 1..]),
                None => (None, s.as_str()),
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
                let valid = chars.next().map_or(false, |c| c.is_ascii_alphabetic() || c == '_') && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
                if !valid {
                    return Err(format!("proxy name '{}' is invalid: must start with a letter or '_', and contain only letters, digits, or '_'", name));
                }
            }
            Ok(Proxy { name, address: address.to_string(), udp })
        })
        .collect()
}

async fn dispatch(conns: ConnSet, proxies: Vec<Proxy>) {
    let map: HashMap<String, Proxy> = proxies.into_iter().map(|p| (p.name.clone(), p)).collect();
    info!(proxies = map.len(), "dispatcher started");
    while let Some((name, tx, rx)) = conns.accept_named().await {
        match map.get(&name) {
            Some(proxy) => {
                let proxy = proxy.clone();
                debug!(%name, address = %proxy.address, "dispatching stream to proxy");
                tokio::spawn(async move {
                    if proxy.udp {
                        udp::handle_stream(tx, rx, proxy.address).await;
                    } else {
                        tcp::handle_stream(tx, rx, proxy.address).await;
                    }
                });
            }
            None => warn!(%name, "no proxy configured for stream name, dropping"),
        }
    }
    warn!("dispatcher exited (connection set closed)");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let level = std::env::var("RUST_LOG").ok().and_then(|s| s.parse::<Level>().ok()).unwrap_or(Level::INFO);
    tracing_subscriber::fmt().with_max_level(level).with_writer(std::io::stderr).init();

    let args = parse_args()?;
    let proxies = parse_proxies(&args.target)?;

    let server_mode = args.serve.is_some();
    let conns = ConnSet::new();

    let host_handle = args.serve.map(|addr| {
        let token = args.token.clone();
        let conns = conns.clone();
        tokio::spawn(async move {
            if let Err(e) = host(conns, addr, token).await {
                warn!(error = %e, "host exited with error");
            }
        })
    });

    let client_handle = args.connect.map(|addr| {
        let token = args.token.clone();
        let conns = conns.clone();
        tokio::spawn(async move {
            if let Err(e) = connect(conns, addr, token).await {
                warn!(error = %e, "connect exited with error");
            }
        })
    });

    let proxy_handles: Vec<_> = if server_mode {
        proxies
            .into_iter()
            .map(|proxy| {
                let conns = conns.clone();
                tokio::spawn(async move {
                    if proxy.udp {
                        let _ = udp::handle_entry(conns, proxy.address, proxy.name).await;
                    } else {
                        let _ = tcp::handle_entry(conns, proxy.address, proxy.name).await;
                    }
                })
            })
            .collect()
    } else {
        vec![tokio::spawn(dispatch(conns.clone(), proxies))]
    };

    tokio::select! {
        _ = async {
            if let Some(handle) = host_handle {
                let _ = handle.await;
            }
            if let Some(handle) = client_handle {
                let _ = handle.await;
            }
        } => {}
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C, shutting down...");
        }
    }

    for handle in proxy_handles {
        handle.abort();
    }

    Ok(())
}
