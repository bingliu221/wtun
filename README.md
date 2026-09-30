# wtun

**wtun** (WebTransport Tunnel) is a lightweight reverse proxy that tunnels TCP and UDP traffic over [WebTransport](https://www.w3.org/TR/webtransport/) (QUIC/HTTP3). It is designed for scenarios where a server-side process needs to expose internal services through a client-initiated outbound connection — no inbound firewall rules required.

## How It Works

```
[Local Client]
     │ TCP/UDP
     ▼
[wtun server]  ←──── WebTransport (QUIC) ────  [wtun client]
     │                                               │
     │                                          (runs inside
     │                                         target network)
     │
     └─ opens a named bi-directional stream
                                             [wtun client]
                                                  │ TCP/UDP
                                                  ▼
                                           [Target Service]
```

- **Server mode** (`--serve`): listens for WebTransport connections from clients. Accepts local TCP/UDP connections and tunnels them to the client over named QUIC streams.
- **Client mode** (`--connect`): connects to a server, maintains the connection with auto-reconnect. Receives named streams from the server and proxies them to the configured target services.

## Installation

```sh
cargo build --release --bin wtun
```

The binary is fully self-contained (pure Rust TLS via `rustls`, no OpenSSL dependency).

## Usage

```
wtun [OPTIONS]

Options:
  -S, --serve <ADDR>    Serve as wtun server, listen for incoming connections
  -C, --connect <ADDR>  Connect to a wtun server, maintain connection
  -P, --proxy <SPEC>    Proxy description (repeatable), format: [name@]host:port[/tcp|udp]
  -K, --token <TOKEN>   Shared secret key for authentication
  -h, --help            Print help
  -V, --version         Print version
```

### Proxy Format

```
[name@]host:port[/tcp|udp]
```

| Part | Description |
|------|-------------|
| `name` | Optional identifier (≤8 chars, `[a-zA-Z_][a-zA-Z0-9_]*`). Omit to use an empty name. |
| `host:port` | Target address to forward traffic to |
| `/tcp` or `/udp` | Protocol. Defaults to TCP if omitted. |

**Examples:**

```
db@127.0.0.1:5432/tcp      # named TCP proxy "db"
dns@8.8.8.8:53/udp         # named UDP proxy "dns"
192.168.1.10:80            # unnamed TCP proxy
```

## Examples

### Expose a remote PostgreSQL via reverse proxy

On the **target machine** (inside the private network):

```sh
wtun --connect example.com:4433 \
    --token mysecret \
    --proxy db@127.0.0.1:5432/tcp
```

On the **server** (publicly reachable):

```sh
wtun --serve 0.0.0.0:4433 \
    --token mysecret \
    --proxy db@0.0.0.0:15432/tcp
```

Local clients can now connect to `server:15432` and reach `127.0.0.1:5432` on the target machine.

---

### Expose a DNS server (UDP)

Target machine:

```sh
wtun --connect example.com:4433 \
    --proxy dns@127.0.0.1:53/udp
```

Server:

```sh
wtun --serve 0.0.0.0:4433 \
    --proxy dns@0.0.0.0:15053/udp
```

## Authentication

Pass `--token <SECRET>` on both sides. The client appends `?token=<SECRET>` to the WebTransport URL; the server verifies it and returns HTTP 403 if it does not match. Without `--token`, no authentication is performed.

## Logging

Log level is controlled via the `RUST_LOG` environment variable (defaults to `info`):

```sh
RUST_LOG=debug wtun --serve 0.0.0.0:4433
```

Logs are written to stderr.

## Docker

Pre-built multi-arch images are available for both musl (Alpine) and glibc (Debian) targets via GitHub Packages (GHCR):

```sh
# Alpine (musl, smaller image)
docker run --rm ghcr.io/bingliu221/wtun:latest-alpine \
    --serve 0.0.0.0:4433 \
    --token mysecret \
    --proxy db@db-host:5432/tcp

# Debian (glibc)
docker run --rm ghcr.io/bingliu221/wtun:latest \
    --serve 0.0.0.0:4433 \
    --token mysecret \
    --proxy db@db-host:5432/tcp
```

## Notes

- The server uses a **self-signed TLS certificate** generated at startup; the client skips certificate validation. This is intentional for private tunnel use — rely on `--token` for authentication.
- Multiple proxy names can be specified with repeated `--proxy` flags.
- The client auto-reconnects every 5 seconds on connection loss.
- TCP entries retry tunnel acquisition up to 3 times with 200 ms intervals before dropping the connection.
