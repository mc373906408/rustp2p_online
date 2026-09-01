use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{broadcast, mpsc};

use crate::route_table::{Protocol, RouteKey};

/// Maximum number of pending datagrams retained by one UDP tunnel.
pub(crate) const TUNNEL_CHANNEL_CAPACITY: usize = 128;

struct UdpRegistration {
    id: usize,
    data_tx: mpsc::Sender<BytesMut>,
}

/// Routes datagrams from shared UDP sockets to their five-tuple tunnel.
pub(crate) struct UdpDispatcher {
    routes: DashMap<RouteKey, UdpRegistration>,
    next_id: AtomicUsize,
}

impl UdpDispatcher {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            routes: DashMap::new(),
            next_id: AtomicUsize::new(1),
        })
    }

    /// Dispatch one UDP datagram without allowing a slow tunnel to block the
    /// shared socket reader. Returns false when the incoming source has gone away.
    pub(crate) fn dispatch(
        self: &Arc<Self>,
        accept_tx: &mpsc::Sender<Tunnel>,
        write_tx: &mpsc::Sender<(Bytes, SocketAddr)>,
        route_key: RouteKey,
        mut data: BytesMut,
    ) -> bool {
        loop {
            match self.routes.entry(route_key) {
                Entry::Occupied(entry) => match entry.get().data_tx.try_send(data) {
                    Ok(()) => return true,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        log::debug!(
                            "UDP tunnel receive queue full, dropping datagram: {route_key}"
                        );
                        return true;
                    }
                    Err(mpsc::error::TrySendError::Closed(returned)) => {
                        data = returned;
                        entry.remove();
                    }
                },
                Entry::Vacant(entry) => {
                    let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                    let (data_tx, data_rx) = mpsc::channel(TUNNEL_CHANNEL_CAPACITY);
                    // A fresh bounded channel always has room for the first packet.
                    data_tx
                        .try_send(data)
                        .expect("fresh UDP tunnel queue must accept its first datagram");
                    entry.insert(UdpRegistration {
                        id,
                        data_tx: data_tx.clone(),
                    });
                    let tunnel = Tunnel::udp(
                        write_tx.clone(),
                        route_key,
                        data_rx,
                        Arc::downgrade(self),
                        id,
                    );
                    return match accept_tx.try_send(tunnel) {
                        Ok(()) => true,
                        Err(mpsc::error::TrySendError::Full(tunnel)) => {
                            log::debug!(
                                "tunnel incoming queue full, dropping new UDP tunnel: {route_key}"
                            );
                            drop(tunnel);
                            true
                        }
                        Err(mpsc::error::TrySendError::Closed(tunnel)) => {
                            drop(tunnel);
                            false
                        }
                    };
                }
            }
        }
    }

    fn unregister(&self, route_key: RouteKey, id: usize) {
        if let Entry::Occupied(entry) = self.routes.entry(route_key) {
            if entry.get().id == id {
                entry.remove();
            }
        }
    }

    pub(crate) fn clear(&self) {
        self.routes.clear();
    }

    pub(crate) fn unregister_local(&self, local_addr: SocketAddr) {
        self.routes
            .retain(|route_key, _| route_key.local_addr() != local_addr);
    }
}

#[derive(Clone)]
enum TunnelWriterInner {
    Udp(mpsc::Sender<(Bytes, SocketAddr)>),
    Tcp(mpsc::Sender<Bytes>),
}

enum TunnelReadLifecycle {
    Udp {
        dispatcher: Weak<UdpDispatcher>,
        id: usize,
    },
    Tcp {
        _read_shutdown: broadcast::Sender<()>,
    },
}

/// A single UDP five-tuple or TCP connection.
///
/// A `Tunnel` has one receive owner and is intentionally not cloneable. It
/// sends to, and receives from, the fixed addresses exposed by `route_key()`.
pub struct Tunnel {
    reader: TunnelReadHalf,
    writer: TunnelWriteHalf,
}

/// The single-owner receive half of a [`Tunnel`].
pub struct TunnelReadHalf {
    route_key: RouteKey,
    data_rx: mpsc::Receiver<BytesMut>,
    lifecycle: TunnelReadLifecycle,
}

/// The send half of a [`Tunnel`].
///
/// Cloning a write half creates another handle to the same UDP socket route or
/// TCP connection. The underlying writer remains open until all clones drop.
#[derive(Clone)]
pub struct TunnelWriteHalf {
    inner: TunnelWriterInner,
    route_key: RouteKey,
}

/// Maps an outbound queue `try_send` failure onto an `io::Error`: full queues
/// yield `WouldBlock` (the data was dropped), closed channels yield a
/// transport-specific message.
fn map_send_error<T>(error: mpsc::error::TrySendError<T>, closed: &str) -> io::Error {
    match error {
        mpsc::error::TrySendError::Full(_) => io::Error::from(io::ErrorKind::WouldBlock),
        mpsc::error::TrySendError::Closed(_) => io::Error::other(closed),
    }
}

impl Tunnel {
    pub(crate) fn udp(
        write_tx: mpsc::Sender<(Bytes, SocketAddr)>,
        route_key: RouteKey,
        data_rx: mpsc::Receiver<BytesMut>,
        dispatcher: Weak<UdpDispatcher>,
        id: usize,
    ) -> Self {
        Self {
            reader: TunnelReadHalf {
                route_key,
                data_rx,
                lifecycle: TunnelReadLifecycle::Udp { dispatcher, id },
            },
            writer: TunnelWriteHalf {
                inner: TunnelWriterInner::Udp(write_tx),
                route_key,
            },
        }
    }

    pub(crate) fn tcp(
        write_tx: mpsc::Sender<Bytes>,
        route_key: RouteKey,
        data_rx: mpsc::Receiver<BytesMut>,
        read_shutdown: broadcast::Sender<()>,
    ) -> Self {
        Self {
            reader: TunnelReadHalf {
                route_key,
                data_rx,
                lifecycle: TunnelReadLifecycle::Tcp {
                    _read_shutdown: read_shutdown,
                },
            },
            writer: TunnelWriteHalf {
                inner: TunnelWriterInner::Tcp(write_tx),
                route_key,
            },
        }
    }

    /// Splits this tunnel into independently owned receive and send halves.
    pub fn split(self) -> (TunnelReadHalf, TunnelWriteHalf) {
        (self.reader, self.writer)
    }

    /// Receives the next datagram or decoded TCP frame from this tunnel.
    pub async fn recv(&mut self) -> Option<BytesMut> {
        self.reader.recv().await
    }

    /// Sends data to this tunnel's peer.
    pub async fn send(&self, data: Bytes) -> io::Result<()> {
        self.writer.send(data).await
    }

    /// Sends data to this tunnel's peer without blocking.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the outbound queue is full;
    /// the data is then dropped and must be retried by the caller.
    pub fn try_send(&self, data: Bytes) -> io::Result<()> {
        self.writer.try_send(data)
    }

    /// Receives the next datagram or decoded TCP frame without blocking.
    ///
    /// Returns `None` when the tunnel is closed (matching
    /// [`Self::recv`]), and [`io::ErrorKind::WouldBlock`] when no data
    /// is available yet.
    pub fn try_recv(&mut self) -> io::Result<Option<BytesMut>> {
        self.reader.try_recv()
    }

    pub fn protocol(&self) -> Protocol {
        self.writer.protocol()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.writer.remote_addr()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.writer.local_addr()
    }

    pub fn route_key(&self) -> RouteKey {
        self.writer.route_key()
    }

    pub fn is_udp(&self) -> bool {
        self.writer.is_udp()
    }

    pub fn is_tcp(&self) -> bool {
        self.writer.is_tcp()
    }
}

impl TunnelReadHalf {
    /// Receives the next datagram or decoded TCP frame.
    pub async fn recv(&mut self) -> Option<BytesMut> {
        self.data_rx.recv().await
    }

    /// Receives the next datagram or decoded TCP frame without blocking.
    ///
    /// Returns `None` when the tunnel is closed (matching
    /// [`Self::recv`]), and [`io::ErrorKind::WouldBlock`] when no data
    /// is available yet.
    pub fn try_recv(&mut self) -> io::Result<Option<BytesMut>> {
        match self.data_rx.try_recv() {
            Ok(data) => Ok(Some(data)),
            Err(mpsc::error::TryRecvError::Empty) => {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(mpsc::error::TryRecvError::Disconnected) => Ok(None),
        }
    }

    pub fn protocol(&self) -> Protocol {
        self.route_key.protocol()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.route_key.peer_addr()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.route_key.local_addr()
    }

    pub fn route_key(&self) -> RouteKey {
        self.route_key
    }

    pub fn is_udp(&self) -> bool {
        self.protocol().is_udp()
    }

    pub fn is_tcp(&self) -> bool {
        self.protocol().is_tcp()
    }
}

impl TunnelWriteHalf {
    /// Sends data to this tunnel's peer.
    pub async fn send(&self, data: Bytes) -> io::Result<()> {
        match &self.inner {
            TunnelWriterInner::Udp(write_tx) => write_tx
                .send((data, self.remote_addr()))
                .await
                .map_err(|_| io::Error::other("UDP socket dropped")),
            TunnelWriterInner::Tcp(write_tx) => write_tx
                .send(data)
                .await
                .map_err(|_| io::Error::other("TCP connection closed")),
        }
    }

    /// Sends data to this tunnel's peer without blocking.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when the outbound queue is full;
    /// the data is then dropped and must be retried by the caller.
    pub fn try_send(&self, data: Bytes) -> io::Result<()> {
        match &self.inner {
            TunnelWriterInner::Udp(write_tx) => write_tx
                .try_send((data, self.remote_addr()))
                .map_err(|error| map_send_error(error, "UDP socket dropped")),
            TunnelWriterInner::Tcp(write_tx) => write_tx
                .try_send(data)
                .map_err(|error| map_send_error(error, "TCP connection closed")),
        }
    }

    pub fn protocol(&self) -> Protocol {
        self.route_key.protocol()
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.route_key.peer_addr()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.route_key.local_addr()
    }

    pub fn route_key(&self) -> RouteKey {
        self.route_key
    }

    pub fn is_udp(&self) -> bool {
        self.protocol().is_udp()
    }

    pub fn is_tcp(&self) -> bool {
        self.protocol().is_tcp()
    }
}

impl Drop for TunnelReadHalf {
    fn drop(&mut self) {
        if let TunnelReadLifecycle::Udp { dispatcher, id } = &self.lifecycle {
            if let Some(dispatcher) = dispatcher.upgrade() {
                dispatcher.unregister(self.route_key, *id);
            }
        }
        // For TCP, dropping the read shutdown sender wakes the reader task.
    }
}

impl std::fmt::Debug for Tunnel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tunnel")
            .field("protocol", &self.protocol())
            .field("local_addr", &self.local_addr())
            .field("peer_addr", &self.remote_addr())
            .finish()
    }
}

impl std::fmt::Debug for TunnelReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelReadHalf")
            .field("protocol", &self.protocol())
            .field("local_addr", &self.local_addr())
            .field("peer_addr", &self.remote_addr())
            .finish()
    }
}

impl std::fmt::Debug for TunnelWriteHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelWriteHalf")
            .field("protocol", &self.protocol())
            .field("local_addr", &self.local_addr())
            .field("peer_addr", &self.remote_addr())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{UdpDispatcher, TUNNEL_CHANNEL_CAPACITY};
    use crate::route_table::{Protocol, RouteKey};
    use bytes::{Bytes, BytesMut};
    use std::net::SocketAddr;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn udp_tunnel_drops_datagrams_beyond_capacity() {
        let dispatcher = UdpDispatcher::new();
        let (accept_tx, mut accept_rx) = mpsc::channel(2);
        let (write_tx, _write_rx) = mpsc::channel::<(Bytes, SocketAddr)>(1);
        let route_key = RouteKey::new(
            Protocol::UDP,
            "0.0.0.0:3000".parse().unwrap(),
            "127.0.0.1:4000".parse().unwrap(),
        );

        for sequence in 0..TUNNEL_CHANNEL_CAPACITY + 32 {
            assert!(dispatcher.dispatch(
                &accept_tx,
                &write_tx,
                route_key,
                BytesMut::from(sequence.to_string().as_bytes()),
            ));
        }

        let mut tunnel = accept_rx.recv().await.unwrap();
        for sequence in 0..TUNNEL_CHANNEL_CAPACITY {
            assert_eq!(
                tunnel.recv().await.unwrap(),
                BytesMut::from(sequence.to_string().as_bytes())
            );
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), tunnel.recv())
                .await
                .is_err()
        );
    }
}
