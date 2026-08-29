//! Low-level socket creation and management.
//!
//! This module provides functions for creating and configuring UDP and TCP sockets
//! with support for interface binding, port reuse, and other socket options.
//!
//! # Examples
//!
//! ```rust,no_run
//! use rustp2p_core::socket::{bind_udp, LocalInterface};
//! use std::net::SocketAddr;
//!
//! # fn main() -> std::io::Result<()> {
//! let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
//! let socket = bind_udp(addr, None)?;
//! # Ok(())
//! # }
//! ```

#[cfg(windows)]
use crate::socket::windows::ignore_conn_reset;
use socket2::Protocol;
use std::io;
use std::net::SocketAddr;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
pub(crate) trait SocketTrait {
    fn set_ip_unicast_if(&self, _interface: &LocalInterface, _is_ipv6: bool) -> io::Result<()> {
        Ok(())
    }
}

/// Network interface identifier for binding sockets.
///
/// On Linux/Android, this uses the interface name (e.g., "eth0").
/// On Windows, macOS, and iOS, this uses the interface index. Platforms
/// without a supported socket option return [`io::ErrorKind::Unsupported`]
/// when the interface is applied.
///
/// # Examples
///
/// ```rust
/// use rustp2p_core::socket::LocalInterface;
///
/// // On Linux/Android
/// #[cfg(any(target_os = "linux", target_os = "android"))]
/// let iface = LocalInterface::new("eth0".to_string());
///
/// // On Windows, macOS, and iOS
/// #[cfg(any(windows, target_os = "macos", target_os = "ios"))]
/// let iface = LocalInterface::new(2); // interface index
/// ```
#[derive(Clone, Debug)]
pub struct LocalInterface {
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub index: u32,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub name: String,
}

impl LocalInterface {
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    pub fn new(index: u32) -> Self {
        Self { index }
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub(crate) fn bind_udp_ops(
    addr: SocketAddr,
    only_v6: bool,
    default_interface: Option<&LocalInterface>,
) -> io::Result<socket2::Socket> {
    let socket = if addr.is_ipv4() {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        if let Some(default_interface) = default_interface {
            socket.set_ip_unicast_if(default_interface, false)?;
        }
        socket
    } else {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV6,
            socket2::Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        socket.set_only_v6(only_v6)?;
        if let Some(default_interface) = default_interface {
            socket.set_ip_unicast_if(default_interface, true)?;
        }
        socket
    };
    #[cfg(windows)]
    if let Err(e) = ignore_conn_reset(&socket) {
        log::warn!("ignore_conn_reset {e:?}")
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    Ok(socket)
}

pub fn bind_udp(
    addr: SocketAddr,
    default_interface: Option<&LocalInterface>,
) -> io::Result<socket2::Socket> {
    bind_udp_ops(addr, true, default_interface)
}

/// Binds a non-blocking TCP listener, applying the configured interface before
/// the socket is bound.
pub(crate) fn bind_tcp_listener(
    addr: SocketAddr,
    default_interface: Option<&LocalInterface>,
) -> io::Result<tokio::net::TcpListener> {
    // Preserve the platform defaults used by std/Tokio when no interface is
    // requested. In particular, Windows has stricter port-allocation behavior
    // for manually created listener sockets around recently used ephemeral
    // ports.
    if default_interface.is_none() {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        return tokio::net::TcpListener::from_std(listener);
    }

    let is_ipv6 = addr.is_ipv6();
    let domain = if is_ipv6 {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(Protocol::TCP))?;
    if is_ipv6 {
        socket.set_only_v6(true)?;
    }
    if let Some(default_interface) = default_interface {
        socket.set_ip_unicast_if(default_interface, is_ipv6)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    tokio::net::TcpListener::from_std(socket.into())
}

/// Applies the selected interface to an accepted TCP stream. Listener socket
/// option inheritance varies by platform, so accepted connections are
/// configured explicitly as well.
pub(crate) fn set_tcp_stream_interface(
    stream: &tokio::net::TcpStream,
    interface: &LocalInterface,
) -> io::Result<()> {
    let socket = socket2::SockRef::from(stream);
    socket.set_ip_unicast_if(interface, stream.peer_addr()?.is_ipv6())
}

/// Upper bound for a single non-blocking TCP connect attempt. Without this,
/// a peer that silently drops SYNs would leave the connect pending until the
/// OS-level TCP timeout (minutes), blocking callers for that whole time.
const TCP_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) async fn connect_tcp(
    addr: SocketAddr,
    bind_port: u16,
    default_interface: Option<&LocalInterface>,
    ttl: Option<u8>,
) -> io::Result<tokio::net::TcpStream> {
    let socket = create_tcp0(addr, bind_port, default_interface, ttl)?;
    tokio::time::timeout(TCP_CONNECT_TIMEOUT, socket.writable())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP connect timed out"))??;
    // A failed non-blocking connect (e.g. ECONNREFUSED) also makes the socket
    // writable; only SO_ERROR reveals the real outcome. Without this check a
    // dead connection would be reported as successfully established.
    if let Some(err) = socket.take_error()? {
        return Err(err);
    }
    Ok(socket)
}

pub(crate) fn create_tcp0(
    addr: SocketAddr,
    bind_port: u16,
    default_interface: Option<&LocalInterface>,
    ttl: Option<u8>,
) -> io::Result<tokio::net::TcpStream> {
    let v4 = addr.is_ipv4();
    let socket = if v4 {
        socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(Protocol::TCP),
        )?
    } else {
        socket2::Socket::new(
            socket2::Domain::IPV6,
            socket2::Type::STREAM,
            Some(Protocol::TCP),
        )?
    };
    if let Some(interface) = default_interface {
        socket.set_ip_unicast_if(interface, !v4)?;
    }
    if bind_port != 0 {
        _ = socket.set_reuse_address(true);
        #[cfg(unix)]
        {
            _ = socket.set_reuse_port(true);
        }
        if v4 {
            let addr: SocketAddr = format!("0.0.0.0:{bind_port}").parse().unwrap();
            socket.bind(&addr.into())?;
        } else {
            socket.set_only_v6(true)?;
            let addr: SocketAddr = format!("[::]:{bind_port}").parse().unwrap();
            socket.bind(&addr.into())?;
        }
    }
    if let Some(ttl) = ttl {
        _ = socket.set_ttl_v4(ttl as _);
    }
    socket.set_nonblocking(true)?;
    socket.set_tcp_nodelay(true)?;
    let res = socket.connect(&addr.into());
    match res {
        Ok(()) => {}
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
        #[cfg(unix)]
        Err(ref e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) => Err(e)?,
    }
    tokio::net::TcpStream::from_std(socket.into())
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests {
    use super::{bind_tcp_listener, bind_udp, create_tcp0, LocalInterface};

    fn missing_interface() -> LocalInterface {
        LocalInterface::new("rp2pnone".to_owned())
    }

    #[test]
    fn socket_creation_applies_default_interface() {
        let interface = missing_interface();
        assert!(bind_udp("0.0.0.0:0".parse().unwrap(), Some(&interface)).is_err());
        assert!(bind_tcp_listener("0.0.0.0:0".parse().unwrap(), Some(&interface)).is_err());
        assert!(create_tcp0("127.0.0.1:9".parse().unwrap(), 0, Some(&interface), None).is_err());
    }
}
