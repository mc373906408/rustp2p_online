use std::net::SocketAddr;
use std::time::Duration;

use crate::endpoint::codec::InitCodec;
use crate::socket::LocalInterface;

/// Load balance strategy for route selection.
#[derive(Clone, Copy, Eq, PartialEq, Debug, Default)]
pub enum LoadBalance {
    #[default]
    MinHopLowestLatency,
    RoundRobin,
    MostRecent,
    LowestLatency,
}

/// Default maximum UDP datagram size (full 64 KiB, accepts any datagram).
pub const DEFAULT_MAX_UDP_DATAGRAM_SIZE: usize = 65_536;

/// Main configuration for creating a [`TunnelIncoming`](super::TunnelIncoming).
pub struct Config {
    pub(crate) stun_servers: Vec<String>,
    pub(crate) udp_port: Option<u16>,
    pub(crate) tcp_port: Option<u16>,
    pub(crate) tcp_codec: Option<Box<dyn InitCodec>>,
    pub(crate) load_balance: LoadBalance,
    pub(crate) route_idle_timeout: Duration,
    pub(crate) max_assistant_sockets: usize,
    pub(crate) mapping_tcp_addr: Vec<SocketAddr>,
    pub(crate) mapping_udp_addr: Vec<SocketAddr>,
    pub(crate) default_interface: Option<LocalInterface>,
    /// Whether to also bind a main IPv6 UDP socket. When true but the system
    /// has no IPv6 support, binding silently falls back to IPv4 only.
    pub(crate) enable_ipv6: bool,
    /// Maximum UDP datagram size the listener can receive.
    pub(crate) max_udp_datagram_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stun_servers: Vec::new(),
            udp_port: Some(0),
            tcp_port: Some(0),
            tcp_codec: None,
            load_balance: LoadBalance::MinHopLowestLatency,
            route_idle_timeout: Duration::from_secs(12),
            max_assistant_sockets: 0,
            mapping_tcp_addr: vec![],
            mapping_udp_addr: vec![],
            default_interface: None,
            enable_ipv6: true,
            max_udp_datagram_size: DEFAULT_MAX_UDP_DATAGRAM_SIZE,
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn udp(port: u16) -> Self {
        Self {
            udp_port: Some(port),
            tcp_port: None,
            ..Default::default()
        }
    }

    pub fn tcp(port: u16) -> Self {
        Self {
            udp_port: None,
            tcp_port: Some(port),
            ..Default::default()
        }
    }

    pub fn udp_port(mut self, port: u16) -> Self {
        self.udp_port = Some(port);
        self
    }

    /// Enables TCP on `port`. When `port` is zero, binding first tries the
    /// actual main UDP port and falls back to an OS-assigned port if that TCP
    /// port is already occupied.
    pub fn tcp_port(mut self, port: u16) -> Self {
        self.tcp_port = Some(port);
        self
    }

    pub fn tcp_codec(mut self, codec: Box<dyn InitCodec>) -> Self {
        self.tcp_codec = Some(codec);
        self
    }

    pub fn stun_servers(mut self, servers: Vec<String>) -> Self {
        self.stun_servers = servers;
        self
    }

    pub fn load_balance(mut self, lb: LoadBalance) -> Self {
        self.load_balance = lb;
        self
    }

    pub fn route_idle_timeout(mut self, timeout: Duration) -> Self {
        self.route_idle_timeout = timeout;
        self
    }

    pub fn max_assistant_sockets(mut self, max: usize) -> Self {
        self.max_assistant_sockets = max;
        self
    }

    pub fn mapping_tcp_addr(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.mapping_tcp_addr = addrs;
        self
    }

    pub fn mapping_udp_addr(mut self, addrs: Vec<SocketAddr>) -> Self {
        self.mapping_udp_addr = addrs;
        self
    }

    /// Selects the network interface used by main and assistant UDP sockets,
    /// the TCP listener, TCP hole-punch connections, and STUN queries.
    ///
    /// On Linux and Android, binding with `SO_BINDTODEVICE` also restricts
    /// inbound traffic. On platforms whose socket option only selects an
    /// outgoing interface, inbound wildcard listeners remain wildcard-bound.
    pub fn default_interface(mut self, interface: LocalInterface) -> Self {
        self.default_interface = Some(interface);
        self
    }

    /// Enable or disable binding a main IPv6 UDP socket.
    pub fn enable_ipv6(mut self, enable: bool) -> Self {
        self.enable_ipv6 = enable;
        self
    }

    /// Set the maximum UDP datagram size the listener can receive; larger
    /// datagrams are truncated. Defaults to 65536 (accepts anything).
    ///
    /// Lower it to what the application actually uses (e.g. 2048) to cut
    /// per-socket receive memory: with the `sendmmsg` feature on
    /// Linux/Android each UDP socket pre-allocates a batch of 16 buffers
    /// of this size (16 x 65536 = 1 MiB per socket by default).
    pub fn max_udp_datagram_size(mut self, size: usize) -> Self {
        self.max_udp_datagram_size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn default_config_has_no_stun_servers() {
        assert!(Config::default().stun_servers.is_empty());
    }
}
