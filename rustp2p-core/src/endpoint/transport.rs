use std::io;
use std::net::SocketAddr;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::route_table::{Protocol, RouteKey};

/// A transport handle to a peer.
///
/// Transport is a send handle - it does NOT store received data.
/// Data is stored in `Received` alongside the Transport.
///
/// UDP transports send through a channel to the socket's writer task.
/// When the socket is dropped by the pool (e.g., environment change),
/// the channel closes and `send()` returns an error.
///
/// # Examples
///
/// ```rust,no_run
/// use bytes::Bytes;
/// use rustp2p_core::endpoint::Transport;
///
/// # async fn example(transport: Transport) -> std::io::Result<()> {
/// transport.send(Bytes::from_static(b"hello")).await?;
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
    Udp(mpsc::Sender<(Bytes, SocketAddr)>),
    Tcp(mpsc::Sender<Bytes>),
}

impl Transport {
    /// Creates a UDP transport.
    pub(crate) fn udp(
        write_tx: mpsc::Sender<(Bytes, SocketAddr)>,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    ) -> Self {
        Self {
            inner: TransportInner::Udp(write_tx),
            local_addr,
            peer_addr,
        }
    }

    /// Creates a TCP transport.
    pub(crate) fn tcp(
        write_tx: mpsc::Sender<Bytes>,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    ) -> Self {
        Self {
            inner: TransportInner::Tcp(write_tx),
            local_addr,
            peer_addr,
        }
    }

    /// Send data to the peer this transport connects to.
    pub async fn send(&self, data: Bytes) -> io::Result<()> {
        match &self.inner {
            TransportInner::Udp(write_tx) => {
                write_tx
                    .send((data, self.peer_addr))
                    .await
                    .map_err(|_| io::Error::other("UDP socket dropped"))?;
                Ok(())
            }
            TransportInner::Tcp(write_tx) => {
                write_tx
                    .send(data)
                    .await
                    .map_err(|_| io::Error::other("TCP connection closed"))
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
