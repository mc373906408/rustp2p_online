//! STUN (Session Traversal Utilities for NAT) protocol implementation.
//!
//! This module provides STUN client functionality for NAT type detection and
//! public address discovery. STUN is used to determine how the local network
//! appears from the public internet.
//!
//! # Examples
//!
//! ```rust,no_run
//! use rustp2p_core::stun::stun_test_nat;
//!
//! # #[tokio::main]
//! # async fn main() -> std::io::Result<()> {
//! let stun_servers = vec![
//!     "stun.miwifi.com:3478".to_string(),
//!     "stun.chat.bilibili.com:3478".to_string(),
//!     "stun.hitv.com:3478".to_string(),
//! ];
//!
//! let result = stun_test_nat(stun_servers, None).await?;
//! println!("NAT Type: {:?}", result.nat_type);
//! println!("Public IPv4: {:?}", result.public_ipv4);
//! println!("Public IPv6: {:?}", result.public_ipv6);
//! # Ok(())
//! # }
//! ```

use std::collections::HashSet;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::time::Duration;

use crate::nat::NatType;
use crate::socket::{bind_udp, LocalInterface};
use rand::Rng;
use stun_format::Attr;
use tokio::net::UdpSocket;

/// Result of STUN NAT detection.
#[derive(Debug, Clone)]
pub struct StunResult {
    /// Detected NAT type (Cone or Symmetric)
    pub nat_type: NatType,
    /// Public IPv4 addresses discovered
    pub public_ipv4: Vec<Ipv4Addr>,
    /// Public IPv6 address if discovered
    pub public_ipv6: Option<Ipv6Addr>,
    /// Public UDP ports discovered (NAT mapped ports)
    pub public_udp_ports: Vec<u16>,
    /// Port range for symmetric NAT (max_port - min_port)
    pub port_range: u16,
}

/// Tests NAT type and discovers public addresses using STUN servers.
///
/// This function queries multiple STUN servers to determine the NAT type,
/// discover public IP addresses, and measure port allocation patterns.
///
/// # Arguments
///
/// * `stun_servers` - List of STUN server addresses (e.g., "stun.example.com:3478")
/// * `default_interface` - Optional network interface to bind to
///
/// # Returns
///
/// A tuple containing:
/// - `NatType` - Detected NAT type (Cone or Symmetric)
/// - `Vec<Ipv4Addr>` - List of discovered public IPv4 addresses
/// - `u16` - Port allocation range (for symmetric NAT prediction)
///
/// # Examples
///
/// ```rust,no_run
/// use rustp2p_core::stun::stun_test_nat;
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let stun_servers = vec![
///     "stun.miwifi.com:3478".to_string(),
///     "stun.chat.bilibili.com:3478".to_string(),
///     "stun.hitv.com:3478".to_string(),
/// ];
/// let result = stun_test_nat(stun_servers, None).await?;
/// println!("NAT Type: {:?}", result.nat_type);
/// println!("Public IPv4: {:?}", result.public_ipv4);
/// println!("Public IPv6: {:?}", result.public_ipv6);
/// # Ok(())
/// # }
/// ```
pub async fn stun_test_nat(
    stun_servers: Vec<String>,
    default_interface: Option<&LocalInterface>,
) -> io::Result<StunResult> {
    stun_test_nat_bound(stun_servers, default_interface, None, None, true).await
}

/// Tests NAT while binding temporary sockets to configured local addresses.
pub async fn stun_test_nat_bound(
    stun_servers: Vec<String>,
    default_interface: Option<&LocalInterface>,
    bind_ipv4: Option<Ipv4Addr>,
    bind_ipv6: Option<Ipv6Addr>,
    enable_ipv6: bool,
) -> io::Result<StunResult> {
    let mut nat_type = NatType::Cone;
    let mut port_range = 0;
    let mut ipv4_set = HashSet::new();
    let mut public_ports = HashSet::new();
    let mut ipv6_addr = None;
    for _ in 0..2 {
        let stun_servers = stun_servers.clone();
        match stun_test_nat0(
            stun_servers,
            default_interface,
            bind_ipv4,
            bind_ipv6,
            enable_ipv6,
        )
        .await
        {
            Ok(result) => {
                if result.nat_type == NatType::Symmetric {
                    nat_type = NatType::Symmetric;
                    // Extract data BEFORE breaking — port_range is critical for
                    // Symmetric NAT port prediction.  Without this, port_range
                    // stays at 0 and Phase-1 predicted-range punching is useless.
                    for ip in result.public_ipv4 {
                        ipv4_set.insert(ip);
                    }
                    for port in result.public_udp_ports {
                        public_ports.insert(port);
                    }
                    if result.public_ipv6.is_some() && ipv6_addr.is_none() {
                        ipv6_addr = result.public_ipv6;
                    }
                    if port_range < result.port_range {
                        port_range = result.port_range;
                    }
                    break;
                }
                for ip in result.public_ipv4 {
                    ipv4_set.insert(ip);
                }
                for port in result.public_udp_ports {
                    public_ports.insert(port);
                }
                if result.public_ipv6.is_some() && ipv6_addr.is_none() {
                    ipv6_addr = result.public_ipv6;
                }
                if port_range < result.port_range {
                    port_range = result.port_range;
                }
            }
            Err(e) => {
                log::warn!("{e:?}");
            }
        }
    }
    Ok(StunResult {
        nat_type,
        public_ipv4: ipv4_set.into_iter().collect(),
        public_ipv6: ipv6_addr,
        public_udp_ports: public_ports.into_iter().collect(),
        port_range,
    })
}

pub(crate) async fn stun_test_nat0(
    stun_servers: Vec<String>,
    default_interface: Option<&LocalInterface>,
    bind_ipv4: Option<Ipv4Addr>,
    bind_ipv6: Option<Ipv6Addr>,
    enable_ipv6: bool,
) -> io::Result<StunResult> {
    let mut udp_v4 = None;
    let mut udp_v6 = None;
    let mut ipv6_unavailable = false;
    let mut ipv4_set = HashSet::new();
    let mut public_ports = HashSet::new();
    let mut ipv6_addr = None;
    let mut pub_addrs_v4 = HashSet::new();
    let mut pub_addrs_v6 = HashSet::new();
    for x in &stun_servers {
        let server_addrs: Vec<_> = match x.to_socket_addrs() {
            Ok(addrs) => addrs.collect(),
            Err(error) => {
                log::warn!("stun {x} resolve error {error:?}");
                continue;
            }
        };
        if server_addrs.is_empty() {
            log::warn!("stun {x} resolves to no address");
            continue;
        }
        for server_addr in server_addrs {
            let udp = match server_addr {
                SocketAddr::V4(_) => {
                    if udp_v4.is_none() {
                        let bind_addr =
                            SocketAddr::from((bind_ipv4.unwrap_or(Ipv4Addr::UNSPECIFIED), 0));
                        let socket = bind_udp(bind_addr, default_interface)?;
                        udp_v4 = Some(UdpSocket::from_std(socket.into())?);
                    }
                    udp_v4.as_ref().unwrap()
                }
                SocketAddr::V6(_) if enable_ipv6 && !ipv6_unavailable => {
                    if udp_v6.is_none() {
                        let bind_addr =
                            SocketAddr::from((bind_ipv6.unwrap_or(Ipv6Addr::UNSPECIFIED), 0));
                        let socket = match bind_udp(bind_addr, default_interface) {
                            Ok(socket) => socket,
                            Err(error) if bind_ipv6.is_none() => {
                                log::warn!(
                                    "IPv6 STUN socket unavailable, skipping IPv6 queries: {error}"
                                );
                                ipv6_unavailable = true;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                        udp_v6 = Some(UdpSocket::from_std(socket.into())?);
                    }
                    udp_v6.as_ref().unwrap()
                }
                SocketAddr::V6(_) => continue,
            };
            match test_nat(udp, server_addr, x).await {
                Ok(addrs) => {
                    for addr in addrs {
                        match addr {
                            SocketAddr::V4(_) => {
                                pub_addrs_v4.insert(addr);
                            }
                            SocketAddr::V6(_) => {
                                pub_addrs_v6.insert(addr);
                            }
                        }
                    }
                }
                Err(e) => log::warn!("stun {x} error {e:?} "),
            }
        }
    }
    let nat_type = mapped_nat_type(&pub_addrs_v4, &pub_addrs_v6);
    for addr in pub_addrs_v4.iter().chain(&pub_addrs_v6) {
        match addr {
            SocketAddr::V4(v4) => {
                ipv4_set.insert(*v4.ip());
                public_ports.insert(v4.port());
            }
            SocketAddr::V6(v6) => {
                if ipv6_addr.is_none() {
                    ipv6_addr = Some(*v6.ip());
                }
                public_ports.insert(v6.port());
            }
        }
    }
    Ok(StunResult {
        nat_type,
        public_ipv4: ipv4_set.into_iter().collect(),
        public_ipv6: ipv6_addr,
        public_udp_ports: public_ports.into_iter().collect(),
        port_range: mapped_port_range(&pub_addrs_v4).max(mapped_port_range(&pub_addrs_v6)),
    })
}

fn mapped_port_range(addrs: &HashSet<SocketAddr>) -> u16 {
    let min = addrs.iter().map(SocketAddr::port).min().unwrap_or(0);
    let max = addrs.iter().map(SocketAddr::port).max().unwrap_or(0);
    max.saturating_sub(min)
}

fn mapped_nat_type(ipv4_addrs: &HashSet<SocketAddr>, ipv6_addrs: &HashSet<SocketAddr>) -> NatType {
    if ipv4_addrs.len() > 1 || ipv6_addrs.len() > 1 {
        NatType::Symmetric
    } else {
        NatType::Cone
    }
}

async fn test_nat(
    udp: &UdpSocket,
    server_addr: SocketAddr,
    stun_server: &str,
) -> io::Result<HashSet<SocketAddr>> {
    udp.connect(server_addr).await?;
    let tid = rand::rng().next_u64() as u128;
    let mut addr = HashSet::new();
    let (mapped_addr1, changed_addr1) = test_nat_(udp, stun_server, true, true, tid).await?;
    // Collect both IPv4 and IPv6 mapped addresses
    addr.insert(mapped_addr1);
    if let Some(changed_addr1) = changed_addr1 {
        if udp.connect(changed_addr1).await.is_ok() {
            match test_nat_(udp, stun_server, false, false, tid + 1).await {
                Ok((mapped_addr2, _)) => {
                    addr.insert(mapped_addr2);
                }
                Err(e) => {
                    log::warn!("stun {stun_server} error {e:?} ");
                }
            }
        }
    }
    log::debug!("stun {stun_server} mapped_addr {addr:?}  changed_addr {changed_addr1:?}",);

    Ok(addr)
}

async fn test_nat_(
    udp: &UdpSocket,
    stun_server: &str,
    change_ip: bool,
    change_port: bool,
    tid: u128,
) -> io::Result<(SocketAddr, Option<SocketAddr>)> {
    for _ in 0..2 {
        let mut buf = [0u8; 28];
        let mut msg = stun_format::MsgBuilder::from(buf.as_mut_slice());
        msg.typ(stun_format::MsgType::BindingRequest);
        msg.tid(tid);
        msg.add_attr(Attr::ChangeRequest {
            change_ip,
            change_port,
        });
        udp.send(msg.as_bytes()).await?;
        let mut buf = [0; 10240];
        let (len, _addr) =
            match tokio::time::timeout(Duration::from_secs(3), udp.recv_from(&mut buf)).await {
                Ok(rs) => rs?,
                Err(e) => {
                    log::warn!("stun {stun_server} error {e:?}");
                    continue;
                }
            };
        let msg = stun_format::Msg::from(&buf[..len]);
        let mut mapped_addr = None;
        let mut changed_addr = None;
        for x in msg.attrs_iter() {
            match x {
                Attr::MappedAddress(addr) if mapped_addr.is_none() => {
                    let _ = mapped_addr.insert(stun_addr(addr));
                }
                Attr::ChangedAddress(addr) if changed_addr.is_none() => {
                    let _ = changed_addr.insert(stun_addr(addr));
                }
                Attr::XorMappedAddress(addr) if mapped_addr.is_none() => {
                    let _ = mapped_addr.insert(stun_addr(addr));
                }
                _ => {}
            }
            if let Some(mapped_addr) = mapped_addr {
                if changed_addr.is_some() {
                    return Ok((mapped_addr, changed_addr));
                }
            }
        }
        if let Some(addr) = mapped_addr {
            return Ok((addr, changed_addr));
        }
    }
    Err(io::Error::other("stun response err"))
}

fn stun_addr(addr: stun_format::SocketAddr) -> SocketAddr {
    match addr {
        stun_format::SocketAddr::V4(ip, port) => {
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip), port))
        }
        stun_format::SocketAddr::V6(ip, port) => {
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::from(ip), port, 0, 0))
        }
    }
}

const TAG: u128 = 1827549368 << 64;

pub fn send_stun_request() -> Vec<u8> {
    let mut buf = [0u8; 28];
    let mut msg = stun_format::MsgBuilder::from(buf.as_mut_slice());
    msg.typ(stun_format::MsgType::BindingRequest);
    let id = rand::rng().next_u64() as u128;
    msg.tid(id | TAG);
    msg.add_attr(Attr::ChangeRequest {
        change_ip: false,
        change_port: false,
    });
    msg.as_bytes().to_vec()
}
pub fn is_stun_response(buf: &[u8]) -> bool {
    !buf.is_empty() && buf[0] == 0x01
}
pub fn recv_stun_response(buf: &[u8]) -> Option<SocketAddr> {
    let msg = stun_format::Msg::from(buf);
    if let Some(tid) = msg.tid() {
        if tid & TAG != TAG {
            return None;
        }
    }
    for x in msg.attrs_iter() {
        match x {
            Attr::MappedAddress(addr) => {
                return Some(stun_addr(addr));
            }
            Attr::XorMappedAddress(addr) => {
                return Some(stun_addr(addr));
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{mapped_nat_type, stun_test_nat0};
    use crate::nat::NatType;
    use std::collections::HashSet;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use tokio::net::UdpSocket;

    async fn mock_stun_server(
        mapped_addr: stun_format::SocketAddr,
    ) -> (String, tokio::task::JoinHandle<SocketAddr>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = socket.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut request = [0_u8; 1024];
            let (len, peer_addr) = socket.recv_from(&mut request).await.unwrap();
            let request = stun_format::Msg::from(&request[..len]);
            let tid = request.tid().unwrap();

            let mut response = [0_u8; 64];
            let mut response = stun_format::MsgBuilder::from(response.as_mut_slice());
            response.typ(stun_format::MsgType::BindingResponse);
            response.tid(tid);
            response.add_attr(stun_format::Attr::MappedAddress(mapped_addr));
            socket
                .send_to(response.as_bytes(), peer_addr)
                .await
                .unwrap();
            peer_addr
        });
        (server_addr.to_string(), task)
    }

    #[test]
    fn mapped_addresses_from_different_families_do_not_imply_symmetric_nat() {
        let ipv4 = HashSet::from([SocketAddr::from((Ipv4Addr::LOCALHOST, 1000))]);
        let ipv6 = HashSet::from([SocketAddr::from((Ipv6Addr::LOCALHOST, 2000))]);

        assert_eq!(mapped_nat_type(&ipv4, &ipv6), NatType::Cone);
    }

    #[tokio::test]
    async fn stun_reuses_one_socket_per_family_and_skips_resolution_failures() {
        let mapped = stun_format::SocketAddr::V4([203, 0, 113, 10], 40000);
        let (server1, peer1) = mock_stun_server(mapped).await;
        let (server2, peer2) = mock_stun_server(mapped).await;

        let result = stun_test_nat0(
            vec!["not a socket address".to_owned(), server1, server2],
            None,
            Some(Ipv4Addr::LOCALHOST),
            None,
            false,
        )
        .await
        .unwrap();

        let peer1 = peer1.await.unwrap();
        let peer2 = peer2.await.unwrap();
        assert_eq!(peer1.port(), peer2.port());
        assert_eq!(result.nat_type, NatType::Cone);
        assert_eq!(result.public_ipv4, vec![Ipv4Addr::new(203, 0, 113, 10)]);
        assert_eq!(result.public_udp_ports, vec![40000]);
    }
}
