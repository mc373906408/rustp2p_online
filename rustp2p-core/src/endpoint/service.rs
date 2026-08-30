use crate::endpoint::config::Config;
use crate::endpoint::pool::SocketPool;
use crate::endpoint::tunnel::Tunnel;
use crate::socket::LocalInterface;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;

/// Binds the shared UDP sockets and TCP listeners and yields logical tunnels.
///
/// # Examples
///
/// ```rust,no_run
/// use bytes::Bytes;
/// use rustp2p_core::endpoint::{Config, TunnelIncoming};
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let mut incoming = TunnelIncoming::bind(Config::new().udp_port(3000)).await?;
/// println!("Listening on: {:?}", incoming.local_addr());
///
/// while let Some(mut tunnel) = incoming.next().await {
///     tokio::spawn(async move {
///         while let Some(data) = tunnel.recv().await {
///             println!("From {}: {:?}", tunnel.remote_addr(), data);
///             tunnel.send(Bytes::from_static(b"echo")).await?;
///         }
///         Ok::<_, std::io::Error>(())
///     });
/// }
/// # Ok(())
/// # }
/// ```
pub struct TunnelIncoming {
    pool: Arc<SocketPool>,
    tunnel_rx: mpsc::Receiver<Tunnel>,
    config: Config,
    local_tcp_port: u16,
    local_tcp_port_v6: Option<u16>,
}

impl TunnelIncoming {
    /// Binds an incoming tunnel source with the given configuration.
    pub async fn bind(mut config: Config) -> io::Result<Self> {
        let codec: Box<dyn crate::endpoint::codec::InitCodec> = config
            .tcp_codec
            .take()
            .unwrap_or_else(|| Box::new(crate::endpoint::codec::LengthPrefixedInitCodec));

        let (accept_tx, tunnel_rx) = mpsc::channel(512);
        let (main_v4, main_v6) = bind_main_udp(
            config.udp_port.unwrap_or(0),
            config.enable_ipv6,
            config.default_interface.as_ref(),
        )
        .await?;
        let udp_v4_port = main_v4.local_addr()?.port();
        let udp_v6_port = main_v6
            .as_ref()
            .map(|socket| socket.local_addr().map(|addr| addr.port()))
            .transpose()?;
        let pool = Arc::new(SocketPool::new(
            main_v4,
            main_v6,
            accept_tx,
            codec.clone(),
            config.max_udp_datagram_size,
        ));

        let (tcp_listener, tcp_listener_v6) = if let Some(port) = config.tcp_port {
            let tcp_v4 = bind_tcp(port, udp_v4_port, config.default_interface.as_ref(), false)?;
            let tcp_v6 = if config.enable_ipv6 {
                match bind_tcp(
                    port,
                    udp_v6_port.unwrap_or(udp_v4_port),
                    config.default_interface.as_ref(),
                    true,
                ) {
                    Ok(listener) => Some(listener),
                    // IPv6 missing on this host (or the v6 bind failing for any
                    // other reason) is a silent downgrade to IPv4 only, matching
                    // the UDP main-socket fallback.
                    Err(e) => {
                        log::warn!("IPv6 TCP listener unavailable, using IPv4 only: {e}");
                        None
                    }
                }
            } else {
                None
            };
            (Some(tcp_v4), tcp_v6)
        } else {
            (None, None)
        };

        let local_tcp_port = tcp_listener
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|a| a.port())
            .unwrap_or(0);
        let local_tcp_port_v6 = tcp_listener_v6
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|a| a.port());

        let incoming = Self {
            pool,
            tunnel_rx,
            config,
            local_tcp_port,
            local_tcp_port_v6,
        };

        // Start a TCP accept loop per listener, mirroring the per-socket task
        // model already used for UDP.
        if let Some(tcp_listener) = tcp_listener {
            spawn_tcp_accept_loop(
                tcp_listener,
                incoming.pool.clone(),
                incoming.config.default_interface.clone(),
            );
        }
        if let Some(tcp_listener) = tcp_listener_v6 {
            spawn_tcp_accept_loop(
                tcp_listener,
                incoming.pool.clone(),
                incoming.config.default_interface.clone(),
            );
        }

        Ok(incoming)
    }

    /// Returns the next UDP five-tuple or inbound/punched TCP connection, or
    /// `None` after the incoming source is closed.
    pub async fn next(&mut self) -> Option<Tunnel> {
        self.tunnel_rx.recv().await
    }

    /// Returns the local address this incoming source is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.pool.local_addr()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a Puncher for NAT hole-punching.
    ///
    /// The handle also provides raw UDP sends, socket queries, and NAT discovery.
    pub fn puncher(&self) -> crate::punch::Puncher {
        crate::punch::Puncher::new(self.pool.clone(), &self.config, self.local_tcp_port)
    }

    /// Get local UDP ports.
    pub fn local_udp_ports(&self) -> Vec<u16> {
        self.pool
            .udp_sockets()
            .iter()
            .filter_map(|s| s.local_addr().ok().map(|addr| addr.port()))
            .collect()
    }

    /// Get the local IPv4 TCP port (the actual bound port, not config value).
    pub fn local_tcp_port(&self) -> u16 {
        self.local_tcp_port
    }

    /// Get the local IPv6 TCP port, when an IPv6 listener is active.
    ///
    /// `None` means IPv6 was disabled or unavailable and only IPv4 TCP is
    /// listened on.
    pub fn local_tcp_port_v6(&self) -> Option<u16> {
        self.local_tcp_port_v6
    }
}

impl std::fmt::Debug for TunnelIncoming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelIncoming").finish_non_exhaustive()
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
async fn bind_main_udp(
    port: u16,
    enable_ipv6: bool,
    default_interface: Option<&LocalInterface>,
) -> io::Result<(UdpSocket, Option<UdpSocket>)> {
    if !enable_ipv6 {
        let main_v4 = bind_udp_v4(port, default_interface).await?;
        return Ok((main_v4, None));
    }
    if port != 0 {
        let main_v4 = bind_udp_v4(port, default_interface).await?;
        return bind_v6_same_port(main_v4, port, default_interface).await;
    }
    // Bind an IPv6-only socket on port 0 first. On systems without IPv6 the
    // bind fails, so this doubles as a capability probe - no retry needed.
    match bind_udp_v6(0, default_interface) {
        Ok(mut main_v6) => {
            // IPv6 is supported. Pair the v4 socket on the same port; when
            // the v4 bind conflicts, re-bind v6 for a fresh port and retry,
            // up to 20 attempts.
            for _ in 0..20 {
                let port = main_v6.local_addr()?.port();
                if let Ok(main_v4) = bind_udp_v4(port, default_interface).await {
                    return Ok((main_v4, Some(main_v6)));
                }
                main_v6 = bind_udp_v6(0, default_interface)?;
            }
            let main_v4 = bind_udp_v4(0, default_interface).await?;
            Ok((main_v4, Some(main_v6)))
        }
        Err(e) => {
            log::warn!("IPv6 main socket unavailable, using IPv4 only: {e}");
            let main_v4 = bind_udp_v4(0, default_interface).await?;
            Ok((main_v4, None))
        }
    }
}

/// Bind the IPv6 socket on the v4 socket's port, downgrading to IPv4 only
/// when the system has no IPv6 support or the port is unavailable.
async fn bind_v6_same_port(
    main_v4: UdpSocket,
    port: u16,
    default_interface: Option<&LocalInterface>,
) -> io::Result<(UdpSocket, Option<UdpSocket>)> {
    match bind_udp_v6(port, default_interface) {
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
async fn bind_udp_v4(
    port: u16,
    default_interface: Option<&LocalInterface>,
) -> io::Result<UdpSocket> {
    if default_interface.is_none() {
        return UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port))).await;
    }
    let socket = crate::socket::bind_udp_ops(
        SocketAddr::from(([0, 0, 0, 0], port)),
        true,
        default_interface,
    )?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

fn bind_udp_v6(port: u16, default_interface: Option<&LocalInterface>) -> io::Result<UdpSocket> {
    let socket = crate::socket::bind_udp_ops(
        format!("[::]:{port}").parse().unwrap(),
        true,
        default_interface,
    )?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

impl Drop for TunnelIncoming {
    fn drop(&mut self) {
        self.pool.shutdown();
    }
}

/// Runs the accept loop for one TCP listener until global shutdown, publishing
/// each accepted connection to the pool.
fn spawn_tcp_accept_loop(
    tcp_listener: TcpListener,
    pool: Arc<SocketPool>,
    default_interface: Option<LocalInterface>,
) {
    let mut shutdown_rx = pool.shutdown_rx();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = tcp_listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            log::debug!("TCP connection from {peer_addr}");
                            if let Some(interface) = default_interface.as_ref() {
                                if let Err(e) = crate::socket::set_tcp_stream_interface(&stream, interface) {
                                    log::warn!("TCP interface setup error for {peer_addr}: {e}");
                                    continue;
                                }
                            }
                            if let Err(e) = pool.publish_tcp(stream, peer_addr, None).await {
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

/// Bind TCP to the configured port. A zero port first reuses the matching
/// family's main UDP port, keeping the externally visible protocol ports
/// aligned when possible. Only an address conflict triggers a fallback to an
/// OS-assigned port; other errors are returned to the caller.
fn bind_tcp(
    configured_port: u16,
    udp_port: u16,
    default_interface: Option<&LocalInterface>,
    v6: bool,
) -> io::Result<TcpListener> {
    let preferred_port = if configured_port == 0 {
        udp_port
    } else {
        configured_port
    };

    match crate::socket::bind_tcp_listener(tcp_wildcard_addr(preferred_port, v6), default_interface)
    {
        Ok(listener) => Ok(listener),
        Err(error) if configured_port == 0 && is_tcp_port_conflict(&error) => {
            log::debug!(
                "TCP port {preferred_port} is occupied, falling back to an OS-assigned port"
            );
            crate::socket::bind_tcp_listener(tcp_wildcard_addr(0, v6), default_interface)
        }
        Err(error) => Err(error),
    }
}

/// Wildcard listener address for the given family: `0.0.0.0` for IPv4 and
/// `[::]` (IPv6-only, see `socket::bind_tcp_listener`) for IPv6, so both
/// listeners of the two-family pair can share one port.
fn tcp_wildcard_addr(port: u16, v6: bool) -> SocketAddr {
    if v6 {
        SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], port))
    } else {
        SocketAddr::from(([0, 0, 0, 0], port))
    }
}

fn is_tcp_port_conflict(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::AddrInUse {
        return true;
    }

    // Windows can report WSAEACCES instead of WSAEADDRINUSE when another
    // socket has the port reserved with exclusive address use.
    #[cfg(windows)]
    if error.raw_os_error() == Some(10013) {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::TunnelIncoming;
    use crate::endpoint::Config;
    use crate::nat::NatType;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream, UdpSocket};

    async fn udp_incoming() -> TunnelIncoming {
        TunnelIncoming::bind(Config::udp(0).enable_ipv6(false))
            .await
            .unwrap()
    }

    fn loopback(addr: SocketAddr) -> SocketAddr {
        SocketAddr::new("127.0.0.1".parse().unwrap(), addr.port())
    }

    #[tokio::test]
    async fn zero_tcp_port_prefers_the_main_udp_port() {
        let listener =
            TunnelIncoming::bind(Config::new().udp_port(0).tcp_port(0).enable_ipv6(false))
                .await
                .unwrap();

        assert_ne!(listener.local_tcp_port(), 0);
        assert_eq!(
            listener.local_tcp_port(),
            listener.local_addr().unwrap().port()
        );
    }

    #[tokio::test]
    async fn zero_tcp_port_falls_back_when_the_udp_port_is_taken_for_tcp() {
        let blocker = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let occupied_port = blocker.local_addr().unwrap().port();
        let listener = TunnelIncoming::bind(
            Config::new()
                .udp_port(occupied_port)
                .tcp_port(0)
                .enable_ipv6(false),
        )
        .await
        .unwrap();

        assert_eq!(listener.local_addr().unwrap().port(), occupied_port);
        assert_ne!(listener.local_tcp_port(), 0);
        assert_ne!(listener.local_tcp_port(), occupied_port);
    }

    #[tokio::test]
    async fn apply_nat_model_uses_external_nat_type() {
        let listener = TunnelIncoming::bind(
            Config::new()
                .udp_port(0)
                .tcp_port(0)
                .max_assistant_sockets(2),
        )
        .await
        .unwrap();
        let puncher = listener.puncher();

        puncher.apply_nat_model(NatType::Symmetric).unwrap();
        assert_eq!(puncher.assistant_count(), 2);

        puncher.apply_nat_model(NatType::Cone).unwrap();
        assert_eq!(puncher.assistant_count(), 0);
    }

    #[tokio::test]
    async fn puncher_owns_udp_socket_queries_and_nat_info() {
        let mapping_tcp_addr: SocketAddr = "127.0.0.1:41000".parse().unwrap();
        let mapping_udp_addr: SocketAddr = "127.0.0.1:42000".parse().unwrap();
        let listener = TunnelIncoming::bind(
            Config::new()
                .udp_port(0)
                .tcp_port(0)
                .enable_ipv6(false)
                .mapping_tcp_addr(vec![mapping_tcp_addr])
                .mapping_udp_addr(vec![mapping_udp_addr]),
        )
        .await
        .unwrap();
        let puncher = listener.puncher();

        assert_eq!(
            puncher.local_addr().unwrap(),
            listener.local_addr().unwrap()
        );
        assert_eq!(puncher.assistant_count(), 0);
        assert_eq!(puncher.udp_sockets().len(), 1);

        let nat_info = puncher.nat_info().await.unwrap();
        assert_eq!(nat_info.mapping_tcp_addr, vec![mapping_tcp_addr]);
        assert_eq!(nat_info.mapping_udp_addr, vec![mapping_udp_addr]);
        assert_eq!(nat_info.local_tcp_port, listener.local_tcp_port());
        assert_eq!(nat_info.local_udp_ports, listener.local_udp_ports());
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn bind_applies_default_interface_to_the_main_socket() {
        let result = TunnelIncoming::bind(
            Config::udp(0)
                .enable_ipv6(false)
                .default_interface(crate::socket::LocalInterface::new("rp2pnone".to_owned())),
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn udp_five_tuples_are_accepted_once_and_demultiplexed() {
        let mut listener = udp_incoming().await;
        let target = loopback(listener.local_addr().unwrap());
        let client_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client_a.send_to(b"a1", target).await.unwrap();
        let mut tunnel_a = listener.next().await.unwrap();
        assert_eq!(tunnel_a.remote_addr(), client_a.local_addr().unwrap());
        assert_eq!(&tunnel_a.recv().await.unwrap()[..], b"a1");

        client_b.send_to(b"b1", target).await.unwrap();
        let mut tunnel_b = listener.next().await.unwrap();
        assert_eq!(tunnel_b.remote_addr(), client_b.local_addr().unwrap());
        assert_eq!(&tunnel_b.recv().await.unwrap()[..], b"b1");

        client_a.send_to(b"a2", target).await.unwrap();
        client_b.send_to(b"b2", target).await.unwrap();
        assert_eq!(&tunnel_a.recv().await.unwrap()[..], b"a2");
        assert_eq!(&tunnel_b.recv().await.unwrap()[..], b"b2");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dropping_udp_tunnel_allows_same_tuple_to_be_accepted_again() {
        let mut listener = udp_incoming().await;
        let target = loopback(listener.local_addr().unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client.send_to(b"first", target).await.unwrap();
        let mut first = listener.next().await.unwrap();
        assert_eq!(&first.recv().await.unwrap()[..], b"first");
        let route_key = first.route_key();
        drop(first);

        client.send_to(b"second", target).await.unwrap();
        let mut second = listener.next().await.unwrap();
        assert_eq!(second.route_key(), route_key);
        assert_eq!(&second.recv().await.unwrap()[..], b"second");
    }

    #[tokio::test]
    async fn udp_tunnel_split_supports_independent_read_and_write_halves() {
        let mut listener = udp_incoming().await;
        let target = loopback(listener.local_addr().unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client.send_to(b"open", target).await.unwrap();
        let tunnel = listener.next().await.unwrap();
        let route_key = tunnel.route_key();
        let (mut reader, writer) = tunnel.split();
        assert_eq!(reader.route_key(), route_key);
        assert_eq!(writer.route_key(), route_key);
        assert_eq!(&reader.recv().await.unwrap()[..], b"open");

        writer.send(Bytes::from_static(b"reply")).await.unwrap();
        let mut buffer = [0_u8; 16];
        let (len, peer) = client.recv_from(&mut buffer).await.unwrap();
        assert_eq!(peer, target);
        assert_eq!(&buffer[..len], b"reply");

        // Dropping only the reader unregisters the five-tuple. The independent
        // writer remains usable, while the next inbound packet creates a new tunnel.
        drop(reader);
        writer
            .send(Bytes::from_static(b"still-open"))
            .await
            .unwrap();
        client.recv_from(&mut buffer).await.unwrap();
        client.send_to(b"reopen", target).await.unwrap();
        let mut reopened = listener.next().await.unwrap();
        assert_eq!(reopened.route_key(), route_key);
        assert_eq!(&reopened.recv().await.unwrap()[..], b"reopen");
    }

    #[tokio::test]
    async fn full_udp_tunnel_does_not_block_other_five_tuples() {
        let mut listener = udp_incoming().await;
        let target = loopback(listener.local_addr().unwrap());
        let slow_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let fast_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        slow_client.send_to(b"start", target).await.unwrap();
        let _slow_tunnel = listener.next().await.unwrap();
        for _ in 0..crate::endpoint::tunnel::TUNNEL_CHANNEL_CAPACITY + 64 {
            slow_client.send_to(b"overflow", target).await.unwrap();
        }

        fast_client.send_to(b"fast", target).await.unwrap();
        let mut fast_tunnel = tokio::time::timeout(Duration::from_secs(1), listener.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fast_tunnel.remote_addr(), fast_client.local_addr().unwrap());
        assert_eq!(&fast_tunnel.recv().await.unwrap()[..], b"fast");
    }

    #[tokio::test]
    async fn tcp_accept_and_drop_follow_tunnel_lifetime() {
        let mut server = TunnelIncoming::bind(Config::tcp(0).enable_ipv6(false))
            .await
            .unwrap();
        let target = SocketAddr::new("127.0.0.1".parse().unwrap(), server.local_tcp_port());

        let mut client = TcpStream::connect(target).await.unwrap();
        let mut server_tunnel = server.next().await.unwrap();
        assert!(server_tunnel.is_tcp());

        client.write_all(&6_u32.to_be_bytes()).await.unwrap();
        client.write_all(b"client").await.unwrap();
        assert_eq!(&server_tunnel.recv().await.unwrap()[..], b"client");
        server_tunnel
            .send(Bytes::from_static(b"server"))
            .await
            .unwrap();
        let mut header = [0_u8; 4];
        client.read_exact(&mut header).await.unwrap();
        assert_eq!(u32::from_be_bytes(header), 6);
        let mut payload = [0_u8; 6];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"server");

        drop(client);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), server_tunnel.recv())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tcp_listens_on_ipv6_when_enabled() {
        let mut server = TunnelIncoming::bind(Config::tcp(0).enable_ipv6(true))
            .await
            .unwrap();
        let Some(v6_port) = server.local_tcp_port_v6() else {
            // Host without IPv6 support: silently downgraded to IPv4 only.
            return;
        };

        // An IPv6 connection is accepted and tunneled like any other TCP stream.
        let target = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], v6_port));
        let mut client = TcpStream::connect(target).await.unwrap();
        let mut server_tunnel = server.next().await.unwrap();
        assert!(server_tunnel.is_tcp());
        assert!(server_tunnel.remote_addr().is_ipv6());

        client.write_all(&2_u32.to_be_bytes()).await.unwrap();
        client.write_all(b"v6").await.unwrap();
        assert_eq!(&server_tunnel.recv().await.unwrap()[..], b"v6");
        server_tunnel
            .send(Bytes::from_static(b"pong"))
            .await
            .unwrap();
        let mut header = [0_u8; 4];
        client.read_exact(&mut header).await.unwrap();
        assert_eq!(u32::from_be_bytes(header), 4);
        let mut payload = [0_u8; 4];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"pong");

        // The IPv4 listener stays available alongside, sharing one port.
        let v4_target = SocketAddr::new("127.0.0.1".parse().unwrap(), server.local_tcp_port());
        let mut v4_client = TcpStream::connect(v4_target).await.unwrap();
        let mut v4_tunnel = server.next().await.unwrap();
        assert!(v4_tunnel.remote_addr().is_ipv4());
        v4_client.write_all(&2_u32.to_be_bytes()).await.unwrap();
        v4_client.write_all(b"v4").await.unwrap();
        assert_eq!(&v4_tunnel.recv().await.unwrap()[..], b"v4");
    }

    #[tokio::test]
    async fn dropping_incoming_closes_udp_tunnels() {
        let mut listener = udp_incoming().await;
        let target = loopback(listener.local_addr().unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"open", target).await.unwrap();
        let mut tunnel = listener.next().await.unwrap();
        assert_eq!(&tunnel.recv().await.unwrap()[..], b"open");

        drop(listener);
        assert!(tokio::time::timeout(Duration::from_secs(1), tunnel.recv())
            .await
            .unwrap()
            .is_none());
        tokio::task::yield_now().await;
        assert!(tunnel.send(Bytes::from_static(b"closed")).await.is_err());
    }

    #[tokio::test]
    async fn removing_assistant_socket_closes_its_udp_tunnels() {
        let mut listener =
            TunnelIncoming::bind(Config::udp(0).enable_ipv6(false).max_assistant_sockets(1))
                .await
                .unwrap();
        let puncher = listener.puncher();
        puncher.apply_nat_model(NatType::Symmetric).unwrap();
        let sockets = puncher.udp_sockets();
        let target = loopback(sockets[1].local_addr().unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"assistant", target).await.unwrap();
        let mut tunnel = listener.next().await.unwrap();
        assert_eq!(&tunnel.recv().await.unwrap()[..], b"assistant");

        puncher.apply_nat_model(NatType::Cone).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(1), tunnel.recv())
            .await
            .unwrap()
            .is_none());
    }
}
