use std::io;
use std::net::SocketAddr;
use std::sync::Weak;
use tokio::net::UdpSocket;

use crate::endpoint::pool::TcpConnection;
use crate::route_table::{Protocol, RouteKey};

/// A transport handle to a peer, holding a Weak reference to the socket.
///
/// Transport is a send handle - it does NOT store received data.
/// Data is stored in `Received` alongside the Transport.
///
/// When the socket is dropped by the pool (e.g., environment change),
/// the Weak reference fails and `send()` returns an error.
///
/// # Examples
///
/// ```rust,no_run
/// use rustp2p_core::endpoint::Transport;
///
/// # async fn example(transport: Transport) -> std::io::Result<()> {
/// transport.send(b"hello").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Transport {
    inner: TransportInner,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

#[derive(Clone)]
enum TransportInner {
    Udp(Weak<UdpSocket>),
    Tcp(Weak<TcpConnection>),
}

impl Transport {
    /// Creates a UDP transport.
    pub(crate) fn udp(weak: Weak<UdpSocket>, local_addr: SocketAddr, peer_addr: SocketAddr) -> Self {
        Self {
            inner: TransportInner::Udp(weak),
            local_addr,
            peer_addr,
        }
    }

    /// Creates a TCP transport.
    pub(crate) fn tcp(weak: Weak<TcpConnection>, local_addr: SocketAddr, peer_addr: SocketAddr) -> Self {
        Self {
            inner: TransportInner::Tcp(weak),
            local_addr,
            peer_addr,
        }
    }

    /// Send data to the peer this transport connects to.
    pub async fn send(&self, data: &[u8]) -> io::Result<()> {
        match &self.inner {
            TransportInner::Udp(weak) => {
                let socket = weak
                    .upgrade()
                    .ok_or_else(|| io::Error::other("UDP socket dropped"))?;
                socket.send_to(data, self.peer_addr).await?;
                Ok(())
            }
            TransportInner::Tcp(weak) => {
                let conn = weak
                    .upgrade()
                    .ok_or_else(|| io::Error::other("TCP connection dropped"))?;
                conn.send(data).await
            }
        }
    }

    /// Returns the protocol (UDP or TCP).
    pub fn protocol(&self) -> Protocol {
        match self.inner {
            TransportInner::Udp(_) => Protocol::UDP,
            TransportInner::Tcp(_) => Protocol::TCP,
        }
    }

    /// Returns the remote address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Returns the local (socket) address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the `RouteKey` identifying this transport's route.
    pub fn route_key(&self) -> RouteKey {
        RouteKey::new(self.protocol(), self.local_addr, self.peer_addr)
    }

    pub fn is_udp(&self) -> bool {
        matches!(self.inner, TransportInner::Udp(_))
    }

    pub fn is_tcp(&self) -> bool {
        matches!(self.inner, TransportInner::Tcp(_))
    }
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("protocol", &self.protocol())
            .field("local_addr", &self.local_addr)
            .field("peer_addr", &self.peer_addr)
            .finish()
    }
}
