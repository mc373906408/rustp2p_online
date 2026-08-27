use crate::endpoint::config::Config;
use crate::endpoint::pool::SocketPool;
use crate::endpoint::transport::Transport;
use bytes::BytesMut;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;

/// A received message with data and source transport.
pub struct Received {
    /// The received data (already framed for TCP). Mutable: freeze it with
    /// [`BytesMut::freeze`] if an immutable `Bytes` is preferred.
    pub data: BytesMut,
    /// The source transport (can be used to send back).
    pub transport: Transport,
}

/// The main P2P endpoint for sending and receiving data.
///
/// # Examples
///
/// ```rust,no_run
/// use bytes::Bytes;
/// use rustp2p_core::endpoint::{EndPoint, Config};
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let mut ep = EndPoint::bind(Config::new().udp_port(3000)).await?;
/// println!("Listening on: {:?}", ep.local_addr());
///
/// while let Some(received) = ep.recv().await {
///     println!("From {}: {:?}", received.transport.remote_addr(), received.data);
///     received.transport.send(Bytes::from_static(b"echo")).await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct EndPoint {
    pool: Arc<SocketPool>,
    data_rx: mpsc::Receiver<(Transport, BytesMut)>,
    config: Config,
    local_tcp_port: u16,
}

impl EndPoint {
    /// Binds an endpoint with the given configuration.
    pub async fn bind(mut config: Config) -> io::Result<Self> {
        let codec: Box<dyn crate::endpoint::codec::InitCodec> = config
            .tcp_codec
            .take()
            .unwrap_or_else(|| Box::new(crate::endpoint::codec::LengthPrefixedInitCodec));

        let mut pool_opt = None;
        let mut data_rx_opt = None;

        if let Some(port) = config.udp_port {
            let (main_v4, main_v6) = bind_main_udp(port, config.enable_ipv6).await?;
            let (pool, data_rx) = SocketPool::new(
                main_v4,
                main_v6,
                codec.clone(),
                config.max_udp_datagram_size,
            );
            pool_opt = Some(Arc::new(pool));
            data_rx_opt = Some(data_rx);
        }

        let tcp_listener = if let Some(port) = config.tcp_port {
            let addr = format!("0.0.0.0:{port}");
            Some(TcpListener::bind(&addr).await?)
        } else {
            None
        };

        let local_tcp_port = tcp_listener
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|a| a.port())
            .unwrap_or(0);

        let (pool, data_rx) = match pool_opt {
            Some(p) => (p, data_rx_opt.unwrap()),
            None => {
                let (main_v4, main_v6) = bind_main_udp(0, config.enable_ipv6).await?;
                let (pool, data_rx) = SocketPool::new(
                    main_v4,
                    main_v6,
                    codec.clone(),
                    config.max_udp_datagram_size,
                );
                (Arc::new(pool), data_rx)
            }
        };

        let ep = Self {
            pool,
            data_rx,
            config,
            local_tcp_port,
        };

        // Start TCP accept loop
        if let Some(listener) = tcp_listener {
            let pool = ep.pool.clone();
            let mut shutdown_rx = pool.shutdown_rx();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, peer_addr)) => {
                                    log::debug!("TCP connection from {peer_addr}");
                                    if let Err(e) = pool.add_tcp(stream, peer_addr) {
                                        log::warn!("TCP setup error: {e}");
                                    }
                                }
                                Err(e) => {
                                    log::warn!("TCP accept error: {e}");
                                    // Back off on persistent errors (e.g. fd
                                    // exhaustion) so the loop does not spin at
                                    // 100% CPU and flood the logs.
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                }
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            log::debug!("TCP accept loop shutting down");
                            break;
                        }
                    }
                }
            });
        }

        Ok(ep)
    }

    /// Receives the next message from any peer.
    pub async fn recv(&mut self) -> Option<Received> {
        let (transport, data) = self.data_rx.recv().await?;
        Some(Received { data, transport })
    }

    /// Returns the local address this endpoint is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.pool.local_addr()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a Sender handle for sending data and querying socket state.
    ///
    /// `Sender` is lightweight and cloneable. It provides send methods
    /// and read-only query methods without exposing internal socket management.
    pub fn sender(&self) -> super::pool::Sender {
        super::pool::Sender(self.pool.clone())
    }

    /// Returns a Puncher for NAT hole-punching.
    ///
    /// The Puncher is constructed using the endpoint's internal socket pool.
    pub fn puncher(&self) -> crate::punch::Puncher {
        crate::punch::Puncher::new(self.pool.clone())
    }

    /// Get local UDP ports.
    pub fn local_udp_ports(&self) -> Vec<u16> {
        self.pool
            .udp_sockets()
            .iter()
            .filter_map(|s| s.local_addr().ok().map(|addr| addr.port()))
            .collect()
    }

    /// Get local TCP port (the actual bound port, not config value).
    pub fn local_tcp_port(&self) -> u16 {
        self.local_tcp_port
    }

    /// Get NAT information using configured STUN servers.
    ///
    /// Uses the stun servers from Config to detect NAT type and public addresses.
    pub async fn nat_info(&self) -> io::Result<crate::nat::NatInfo> {
        let stun_servers = self.config.stun_servers.clone();
        let default_interface = self.config.default_interface.as_ref();

        let stun_result = crate::stun::stun_test_nat(stun_servers, default_interface).await?;

        log::debug!(
            "nat_type:{:?},public_ipv4:{:?},public_ipv6:{:?},public_udp_ports:{:?},port_range:{}",
            stun_result.nat_type,
            stun_result.public_ipv4,
            stun_result.public_ipv6,
            stun_result.public_udp_ports,
            stun_result.port_range
        );

        let local_ipv4 = crate::util::addr::local_ipv4()
            .await
            .unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);

        let local_udp_ports = self.local_udp_ports();
        let local_tcp_port = self.local_tcp_port();

        // public_udp_ports starts empty — STUN uses a temporary socket so its
        // mapped ports do NOT correspond to the main QUIC socket.  The real
        // public ports are discovered via NatObserve (which observes the actual
        // QUIC connection's source address) and appended later.
        //
        // Previously this was `local_udp_ports.clone()` then `fill(0)`, which
        // produced [0, 0, ...].  Those zeros are harmful: in punch_udp's
        // Symmetric branch, base_port=0 with port_range=N yields a prediction
        // window of [1, N] (wrong) or [1, 0] (empty), wasting prediction slots.
        let public_udp_ports: Vec<u16> = Vec::new();

        Ok(crate::nat::NatInfo {
            nat_type: stun_result.nat_type,
            public_ips: stun_result.public_ipv4,
            public_udp_ports,
            mapping_tcp_addr: self.config.mapping_tcp_addr.clone(),
            mapping_udp_addr: self.config.mapping_udp_addr.clone(),
            public_port_range: stun_result.port_range,
            local_ipv4,
            local_ipv4s: vec![],
            ipv6: None,
            local_udp_ports,
            local_tcp_port,
            public_tcp_port: 0,
            stun_mapped_ports: Vec::new(),
        })
    }

    /// Apply the socket model for an externally detected NAT type.
    ///
    /// This method does not run STUN or any other NAT detection. Call
    /// [`nat_info`](Self::nat_info) or your own detector first, then pass the
    /// resulting [`NatType`](crate::nat::NatType) here.
    ///
    /// - `Symmetric`: add assistant sockets up to `Config::max_assistant_sockets`.
    /// - `Cone`: remove assistant sockets because extra source ports are not needed.
    pub fn apply_nat_model(&self, nat_type: crate::nat::NatType) -> io::Result<()> {
        match nat_type {
            crate::nat::NatType::Symmetric => {
                let current = self.pool.assistant_count();
                let target = self.config.max_assistant_sockets;
                if target > current {
                    log::debug!(
                        "Symmetric NAT model selected, adding {} assistant sockets",
                        target - current
                    );
                    for _ in current..target {
                        let socket = crate::socket::bind_udp("0.0.0.0:0".parse().unwrap(), None)?;
                        let std_socket: std::net::UdpSocket = socket.into();
                        let tokio_socket = tokio::net::UdpSocket::from_std(std_socket)?;
                        self.pool.add_assistant_udp(tokio_socket);
                    }
                }
            }
            crate::nat::NatType::Cone => {
                let count = self.pool.assistant_count();
                if count > 0 {
                    log::debug!("Cone NAT model selected, cleaning {count} assistant sockets");
                    self.pool.clean_assistant_udp();
                }
            }
        }

        Ok(())
    }
}

impl std::fmt::Debug for EndPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndPoint").finish_non_exhaustive()
    }
}

/// Bind the main UDP sockets: IPv4 on `0.0.0.0:port` and, when IPv6 is
/// enabled and the system supports it, an IPv6-only socket on `[::]:port`
/// sharing the same port.
///
/// With port 0, the IPv6 socket is bound first (its own random port); binding
/// it fails exactly when the system has no IPv6 support, in which case we
/// bind an IPv4 socket on port 0 directly - no retries needed. When v6 binds
/// successfully, a v4 socket is paired on v6's port, retrying with a fresh
/// v6 port on conflict (up to 20 attempts).
///
/// A missing second socket (IPv6 unsupported or disabled) is a silent
/// downgrade to IPv4 only, not an error.
async fn bind_main_udp(port: u16, enable_ipv6: bool) -> io::Result<(UdpSocket, Option<UdpSocket>)> {
    if !enable_ipv6 {
        let main_v4 = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
        return Ok((main_v4, None));
    }
    if port != 0 {
        let main_v4 = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port))).await?;
        return bind_v6_same_port(main_v4, port).await;
    }
    // Bind an IPv6-only socket on port 0 first. On systems without IPv6 the
    // bind fails, so this doubles as a capability probe - no retry needed.
    match bind_udp_v6(0) {
        Ok(mut main_v6) => {
            // IPv6 is supported. Pair the v4 socket on the same port; when
            // the v4 bind conflicts, re-bind v6 for a fresh port and retry,
            // up to 20 attempts.
            for _ in 0..20 {
                let port = main_v6.local_addr()?.port();
                if let Ok(main_v4) = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port))).await {
                    return Ok((main_v4, Some(main_v6)));
                }
                main_v6 = bind_udp_v6(0)?;
            }
            log::warn!(
                "failed to pair the main v4/v6 UDP ports after 20 attempts, using IPv4 only"
            );
            let main_v4 = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))).await?;
            Ok((main_v4, None))
        }
        Err(e) => {
            log::warn!("IPv6 main socket unavailable, using IPv4 only: {e}");
            let main_v4 = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))).await?;
            Ok((main_v4, None))
        }
    }
}

/// Bind the IPv6 socket on the v4 socket's port, downgrading to IPv4 only
/// when the system has no IPv6 support or the port is unavailable.
async fn bind_v6_same_port(
    main_v4: UdpSocket,
    port: u16,
) -> io::Result<(UdpSocket, Option<UdpSocket>)> {
    match bind_udp_v6(port) {
        Ok(main_v6) => Ok((main_v4, Some(main_v6))),
        Err(e) => {
            log::warn!("IPv6 main socket unavailable, falling back to IPv4 only: {e}");
            Ok((main_v4, None))
        }
    }
}

/// Bind an IPv6-only UDP socket on `[::]:port`.
///
/// v6-only so a v4 socket bound to the same port does not conflict.
fn bind_udp_v6(port: u16) -> io::Result<UdpSocket> {
    let socket = crate::socket::bind_udp_ops(format!("[::]:{port}").parse().unwrap(), true, None)?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

impl Drop for EndPoint {
    fn drop(&mut self) {
        self.pool.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::EndPoint;
    use crate::endpoint::Config;
    use crate::nat::NatType;

    #[tokio::test]
    async fn apply_nat_model_uses_external_nat_type() {
        let ep = EndPoint::bind(
            Config::new()
                .udp_port(0)
                .tcp_port(0)
                .max_assistant_sockets(2),
        )
        .await
        .unwrap();
        let sender = ep.sender();

        ep.apply_nat_model(NatType::Symmetric).unwrap();
        assert_eq!(sender.assistant_count(), 2);

        ep.apply_nat_model(NatType::Cone).unwrap();
        assert_eq!(sender.assistant_count(), 0);
    }
}
