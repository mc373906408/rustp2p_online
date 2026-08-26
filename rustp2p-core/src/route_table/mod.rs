//! Routing logic for selecting optimal paths between peers.
//!
//! This module manages routing decisions, tracking metrics like RTT (Round Trip Time)
//! and hop count to select the best available path for communication.
//!
//! # Examples
//!
//! ```rust
//! use rustp2p_core::route_table::{RouteKey, Protocol};
//!
//! # fn example() {
//! // Create a RouteKey from protocol and addresses
//! let key = RouteKey::new(
//!     Protocol::UDP,
//!     "127.0.0.1:2000".parse().unwrap(),
//!     "127.0.0.1:3000".parse().unwrap(),
//! );
//! assert!(key.protocol().is_udp());
//! assert_eq!(key.peer_addr().port(), 3000);
//! # }
//! ```

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::endpoint::transport::Transport;

mod table;

pub use table::{Route, RouteTable};

pub const DEFAULT_RTT: u32 = 9999;

/// Identifies a specific route to a peer.
///
/// `RouteKey` uniquely identifies a path by combining the
/// protocol (UDP/TCP), local (socket) address and remote address.
///
/// # Examples
///
/// ```rust
/// use rustp2p_core::route_table::{RouteKey, Protocol};
///
/// # fn example() {
/// // Create from protocol and addresses
/// let key = RouteKey::new(
///     Protocol::UDP,
///     "127.0.0.1:2000".parse().unwrap(),
///     "127.0.0.1:3000".parse().unwrap(),
/// );
///
/// // Or from a Transport
/// // let key = transport.route_key();
/// # }
/// ```
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct RouteKey {
    protocol: Protocol,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}
impl Default for RouteKey {
    fn default() -> Self {
        Self {
            protocol: Protocol::TCP,
            local_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            peer_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        }
    }
}
impl RouteKey {
    /// Creates a new RouteKey from protocol, local and remote addresses.
    pub const fn new(protocol: Protocol, local_addr: SocketAddr, peer_addr: SocketAddr) -> Self {
        Self {
            protocol,
            local_addr,
            peer_addr,
        }
    }

    /// Creates a RouteKey from a Transport handle.
    pub fn from_transport(transport: &Transport) -> Self {
        Self {
            protocol: transport.protocol(),
            local_addr: transport.local_addr(),
            peer_addr: transport.remote_addr(),
        }
    }

    /// Returns the protocol (UDP or TCP).
    #[inline]
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// Returns the local (socket) address.
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the remote peer address.
    #[inline]
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

/// Sorting key for comparing route quality.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct RouteSortKey {
    metric: u8,
    rtt: u32,
}

/// Protocol type (UDP or TCP).
///
/// # Examples
///
/// ```rust
/// use rustp2p_core::route_table::Protocol;
///
/// let proto = Protocol::UDP;
/// assert!(proto.is_udp());
/// assert!(!proto.is_tcp());
/// ```
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Protocol {
    UDP,
    TCP,
}
impl Protocol {
    /// Returns true if this is TCP.
    #[inline]
    pub fn is_tcp(&self) -> bool {
        self == &Protocol::TCP
    }

    /// Returns true if this is UDP.
    #[inline]
    pub fn is_udp(&self) -> bool {
        self == &Protocol::UDP
    }
}

impl fmt::Display for RouteKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let protocol = self.protocol();
        write!(f, "{}://{}", protocol, self.peer_addr())
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::UDP => write!(f, "udp"),
            Protocol::TCP => write!(f, "tcp"),
        }
    }
}
