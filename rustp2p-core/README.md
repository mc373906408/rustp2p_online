# rustp2p-core

[![Crates.io](https://img.shields.io/crates/v/rustp2p-core.svg)](https://crates.io/crates/rustp2p-core)
[![Docs.rs](https://docs.rs/rustp2p-core/badge.svg)](https://docs.rs/rustp2p-core)

`rustp2p-core` is the low-level transport crate in the `rustp2p` workspace. It
provides UDP/TCP tunnel primitives, route table utilities, STUN helpers, NAT
information types, and hole-punching primitives.

This crate does not provide the high-level PeerId QUIC overlay. For encrypted
application datagrams, reliable streams, discovery, and relay forwarding, use
`rustp2p-quic`.

## Features

- UDP five-tuple and TCP connection tunnels yielded from one `TunnelIncoming`.
- `Tunnel::split` for moving the receive and cloneable send halves into separate tasks.
- Cloneable `Puncher` for raw UDP sends, socket queries, NAT discovery, and punching.
- Consistent `Config::default_interface` selection for main/assistant UDP,
  TCP listener/punch connections, IPv4/IPv6, and STUN sockets.
- Explicit IPv4/IPv6 local-address binding for listeners, punching, and STUN.
- TCP framing through configurable codecs.
- Route table utilities with multiple routes per peer id and load balancing.
- STUN-based NAT type and port-range detection when explicitly configured.
- NAT model application for local assistant UDP sockets.
- Hole-punching primitives driven by remote `NatInfo`.

`Config::default()` does not include public STUN servers. Configure STUN
explicitly when NAT detection is needed.

## Quick Start

```toml
[dependencies]
rustp2p-core = "0.1"
```

### Echo Tunnels

```rust
use bytes::Bytes;
use rustp2p_core::endpoint::{Config, TunnelIncoming};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut incoming = TunnelIncoming::bind(Config::new().udp_port(3000).tcp_port(3000)).await?;

    while let Some(mut tunnel) = incoming.next().await {
        tokio::spawn(async move {
            while let Some(data) = tunnel.recv().await {
                println!("from={} protocol={:?} bytes={:?}",
                    tunnel.remote_addr(), tunnel.protocol(), data);
                tunnel.send(Bytes::from_static(b"echo")).await?;
            }
            Ok::<_, std::io::Error>(())
        });
    }

    Ok(())
}
```

### Send Through `Puncher`

```rust
use rustp2p_core::endpoint::{Config, TunnelIncoming};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let incoming = TunnelIncoming::bind(Config::new().udp_port(0)).await?;
    let puncher = incoming.puncher();

    puncher.send_to(b"hello", "127.0.0.1:3000".parse().unwrap())?;
    puncher.try_send_via_all(b"probe", "127.0.0.1:3000".parse().unwrap());

    Ok(())
}
```

### Bind Local Addresses

Bind one or both address families independently. IPv6 binding remains subject
to `enable_ipv6` (enabled by default).

```rust,no_run
use std::net::{Ipv4Addr, Ipv6Addr};
use rustp2p_core::endpoint::{Config, TunnelIncoming};

# #[tokio::main]
# async fn main() -> std::io::Result<()> {
let incoming = TunnelIncoming::bind(
    Config::new()
        .bind_ipv4(Ipv4Addr::new(192, 0, 2, 10))
        .bind_ipv6("2001:db8::10".parse::<Ipv6Addr>().unwrap()),
)
.await?;

println!("IPv4 TCP: {:?}", incoming.local_tcp_addr());
println!("IPv6 UDP: {:?}", incoming.local_udp_ipv6_addr());
println!("IPv6 TCP: {:?}", incoming.local_tcp_ipv6_addr());
# Ok(())
# }
```

## NAT And Punching

STUN is explicit:

```rust
use rustp2p_core::endpoint::{Config, TunnelIncoming};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let incoming = TunnelIncoming::bind(
        Config::new().stun_servers(vec![
            "stun.miwifi.com:3478".to_string(),
            "stun.chat.bilibili.com:3478".to_string(),
            "stun.hitv.com:3478".to_string(),
        ]),
    )
    .await?;

    let puncher = incoming.puncher();
    let nat_info = puncher.nat_info().await?;
    puncher.apply_nat_model(nat_info.nat_type)?;

    Ok(())
}
```

`Puncher::apply_nat_model` only applies an externally detected local `NatType`:

- `NatType::Symmetric` adds assistant UDP sockets up to
  `Config::max_assistant_sockets`.
- `NatType::Cone` removes assistant UDP sockets.

`Puncher` does not infer the local socket model: callers supply the detected
local `NatType`. It also uses the remote peer's `NatInfo` in `PunchInfo` when
executing hole punching.

## Core Types

| Type | Purpose |
| ---- | ------- |
| `TunnelIncoming` | Binds UDP/TCP sockets and yields logical tunnels through `next`. |
| `Tunnel` | Single-owner receive stream and send handle for one UDP five-tuple or TCP connection. |
| `TunnelReadHalf` / `TunnelWriteHalf` | Independently owned halves returned by `Tunnel::split`. |
| `Puncher` | Cloneable handle for UDP sends, socket queries, NAT discovery, and punching. |
| `RouteKey` | `(Protocol, local SocketAddr, peer SocketAddr)` route identity. |
| `RouteTable<T>` | Multi-route table keyed by caller-defined peer id type. |
| `NatInfo` / `NatType` | NAT shape, local/public addresses, and port metadata. |
| `Puncher` / `PunchInfo` | Low-level NAT punching primitive. |

## Route Table

`RouteTable<T>` is generic over your own peer id type. It stores confirmed
routes and can select a route based on the configured `LoadBalance` policy.

```rust
use rustp2p_core::endpoint::LoadBalance;
use rustp2p_core::route_table::{Protocol, RouteKey, RouteTable};

fn main() -> std::io::Result<()> {
    let routes: RouteTable<String> = RouteTable::new(LoadBalance::MinHopLowestLatency);
    let key = RouteKey::new(
        Protocol::UDP,
        "127.0.0.1:2000".parse().unwrap(),
        "127.0.0.1:3000".parse().unwrap(),
    );

    routes.add_route("peer-a".to_string(), (key, 0));
    let route = routes.get_route_by_id(&"peer-a".to_string())?;
    assert!(route.is_direct());

    Ok(())
}
```

## Design

See [DESIGN.md](DESIGN.md) for the implemented architecture, socket lifecycle,
route semantics, and NAT traversal boundaries.

## Validation

```bash
cargo check -p rustp2p-core
cargo test -p rustp2p-core
cargo test -p rustp2p-core --doc
cargo clippy --workspace --all-targets -- -D warnings
```

## License

Apache-2.0
