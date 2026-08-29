use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::net::UdpSocket;

use crate::socket::LocalInterface;

/// Local addresses discovered by enumerating the system interfaces.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LocalIps {
    /// Physical, operationally-up, non-loopback IPv4 addresses, sorted and
    /// deduplicated.
    pub ipv4s: Vec<Ipv4Addr>,
    /// Publicly routable (global unicast) IPv6 addresses, sorted and
    /// deduplicated.
    pub ipv6s: Vec<Ipv6Addr>,
}

/// Enumerates the local IP addresses of the physical network interfaces.
///
/// When `interface` is set only the addresses of that interface are
/// returned; otherwise every physical, operationally-up, non-loopback
/// interface is scanned (see [`is_physical_interface`] for how "physical"
/// is decided per platform). IPv6 addresses are kept only when globally
/// routable (see [`is_ipv6_global`]); unspecified/loopback/multicast/
/// broadcast IPv4 addresses are always skipped.
pub fn local_ips(interface: Option<&LocalInterface>) -> LocalIps {
    let Ok(ifaces) = if_addrs::get_if_addrs() else {
        return LocalIps::default();
    };
    let mut v4s = Vec::new();
    let mut v6s = Vec::new();
    for iface in ifaces {
        // Bound to a specific NIC: take that interface's addresses only.
        // Otherwise enforce the physical interface rules.
        if let Some(bound) = interface {
            if !interface_matches(&iface, bound) {
                continue;
            }
        } else if !is_physical_interface(&iface) || !iface.is_oper_up() || iface.is_p2p() {
            continue;
        }
        if iface.is_loopback() {
            continue;
        }
        match iface.addr {
            if_addrs::IfAddr::V4(v4) => {
                let ip = v4.ip;
                if !ip.is_unspecified()
                    && !ip.is_loopback()
                    && !ip.is_multicast()
                    && !ip.is_broadcast()
                {
                    v4s.push(ip);
                }
            }
            if_addrs::IfAddr::V6(v6) => {
                let ip = v6.ip;
                if is_ipv6_global(&ip) {
                    v6s.push(ip);
                }
            }
        }
    }
    v4s.sort_unstable();
    v4s.dedup();
    v6s.sort_unstable();
    v6s.dedup();
    LocalIps {
        ipv4s: v4s,
        ipv6s: v6s,
    }
}

/// Whether `iface` is the interface identified by `bound`.
fn interface_matches(iface: &if_addrs::Interface, bound: &LocalInterface) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        iface.name == bound.name
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        iface.index == Some(bound.index)
    }
}

/// Resolves the primary local IPv4 source address by probing, used as a
/// fallback when interface enumeration comes up empty.
///
/// Binds a socket (applying `interface` when set, the same mechanism the
/// tunnel sockets use) and connects to a routable destination; the OS then
/// reports in `local_addr()` the source address it picked. `stun_servers`
/// are tried first — wherever STUN succeeded through the bound interface
/// they are routable by construction. Well-known public addresses are used
/// as a fallback for networks that cannot reach the configured servers.
pub async fn local_ipv4(
    interface: Option<&LocalInterface>,
    stun_servers: &[String],
) -> Option<Ipv4Addr> {
    let probes = stun_servers
        .iter()
        .map(String::as_str)
        .chain(LOCAL_IPV4_PROBES.iter().copied());
    match resolve_source_ip(interface, "0.0.0.0:0", probes).await {
        Ok(IpAddr::V4(ip)) => Some(ip),
        Ok(IpAddr::V6(_)) => None,
        Err(e) => {
            log::warn!("could not resolve local IPv4: {e}");
            None
        }
    }
}

/// Resolves the local IPv6 source address by probing, used as a fallback
/// when interface enumeration comes up empty. Kept only when it is a global
/// unicast address.
pub async fn local_ipv6(
    interface: Option<&LocalInterface>,
    stun_servers: &[String],
) -> Option<Ipv6Addr> {
    let probes = stun_servers
        .iter()
        .map(String::as_str)
        .chain(LOCAL_IPV6_PROBES.iter().copied());
    match resolve_source_ip(interface, "[::]:0", probes).await {
        Ok(IpAddr::V6(ip)) if is_ipv6_global(&ip) => Some(ip),
        Ok(ip) => {
            log::debug!("local ipv6 {ip} is not a global address, dropping");
            None
        }
        Err(e) => {
            log::warn!("could not resolve local IPv6: {e}");
            None
        }
    }
}

/// Binds a socket (applying `interface` when set) and returns the source
/// address the OS selects for the first reachable probe destination.
async fn resolve_source_ip(
    interface: Option<&LocalInterface>,
    bind_addr: &str,
    probes: impl IntoIterator<Item = &str>,
) -> io::Result<IpAddr> {
    for dest in probes {
        // A fresh socket per attempt: a failed connect must not leave a
        // half-configured socket behind for the next destination.
        let socket = crate::socket::bind_udp(bind_addr.parse().unwrap(), interface)?;
        let socket = UdpSocket::from_std(socket.into())?;
        if socket.connect(dest).await.is_err() {
            continue;
        }
        if let Ok(addr) = socket.local_addr() {
            let ip = addr.ip();
            if !ip.is_unspecified() && !ip.is_loopback() {
                return Ok(ip);
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NetworkUnreachable,
        "no reachable route to resolve the local source address",
    ))
}

/// Fallback destinations tried when none of the configured STUN servers are
/// routable.
const LOCAL_IPV4_PROBES: &[&str] = &[
    "8.8.8.8:53",
    "1.1.1.1:53",
    "223.5.5.5:53",
    "114.114.114.114:53",
];

/// Fallback destinations tried when none of the configured STUN servers are
/// routable over IPv6.
const LOCAL_IPV6_PROBES: &[&str] = &[
    "[2001:4860:4860::8888]:53", // Google Public DNS
    "[2606:4700:4700::1111]:53", // Cloudflare
    "[2400:3200::1]:53",         // AliDNS
];

/// Returns all local non-loopback IPv4 addresses of **physical** network
/// interfaces that are operationally **Up**, deduplicated and sorted.
///
/// Enumerates the system interfaces with the cross-platform `if-addrs`
/// crate (getifaddrs on POSIX/Android/iOS, `GetAdaptersAddresses` on
/// Windows). Interfaces that are not Up (carrier/hardware missing on POSIX,
/// real oper-status on Windows), loopback and point-to-point (tunnel)
/// interfaces are excluded via its flags, virtual devices (bridges, veth,
/// TAP, ...) via well-known name conventions, and
/// unspecified/loopback/multicast/broadcast addresses are always skipped —
/// they can never be used as a local source for punching.
pub fn local_ipv4s() -> Vec<Ipv4Addr> {
    let mut addrs: Vec<Ipv4Addr> = if_addrs::get_if_addrs()
        .map(|ifaces| {
            ifaces
                .into_iter()
                .filter(|iface| {
                    iface.is_oper_up()
                        && !iface.is_loopback()
                        && !iface.is_p2p()
                        && is_physical_interface(iface)
                })
                .filter_map(|iface| match iface.addr {
                    if_addrs::IfAddr::V4(v4) => Some(v4.ip),
                    if_addrs::IfAddr::V6(_) => None,
                })
                .filter(|ip| {
                    !ip.is_unspecified()
                        && !ip.is_loopback()
                        && !ip.is_multicast()
                        && !ip.is_broadcast()
                })
                .collect()
        })
        .unwrap_or_default();
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

/// Whether `name` matches a well-known virtual/software interface naming
/// convention (tunnels, bridges, container and virtualization adapters).
///
/// This is the name-level fallback used where no platform API can tell a
/// physical device from a virtual one (see [`is_physical_interface`]).
#[cfg(any(
    test,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
    ))
))]
fn is_virtual_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    const VIRTUAL_PREFIXES: &[&str] = &[
        // tunnels and VPNs
        "tun",
        "tap",
        "vpn",
        "utun",
        "wg",
        "wireguard",
        "ppp",
        "tailscale",
        "zerotier",
        "zt",
        "gif",
        "stf",
        "ipsec",
        "teredo",
        "6to4",
        "isatap",
        // bridges and container/virtualization networking
        "br",
        "bridge",
        "veth",
        "virbr",
        "docker",
        "vmnet",
        "vbox",
        "vethernet",
        "hyper-v",
        "wsl",
        "npcap",
        "dummy",
        // Apple virtual interfaces
        "awdl",
        "llw",
        "p2p",
    ];
    VIRTUAL_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

/// Whether `iface` is a physical network interface.
///
/// No portable API reports "physical hardware", so each platform uses its
/// most reliable signal:
/// - Linux/Android: the sysfs `device` symlink — physical NICs live on a
///   real bus, every virtual device resolves under `devices/virtual/net`.
/// - Windows: `GetAdaptersAddresses` interface type (Ethernet/Wi-Fi), with
///   virtual-friendly-name markers catching TAP/Wintun that fake Ethernet.
/// - Apple: the `en*` family, which is how the system groups physical
///   adapters (Ethernet and Wi-Fi) in the first place.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn is_physical_interface(iface: &if_addrs::Interface) -> bool {
    // Physical NICs live on a real bus in sysfs. Every virtual device (lo,
    // tun, tap, veth, bridge, docker0, wireguard, bond, ...) also has a
    // `device` symlink, but it resolves under `devices/virtual/net` — the
    // absence of that marker is the reliable discriminator.
    let on_real_bus = std::fs::read_link(format!("/sys/class/net/{}/device", iface.name))
        .map(|target| !target.to_string_lossy().contains("devices/virtual"))
        .unwrap_or(false);
    on_real_bus
        || iface.name.starts_with("eth")
        || iface.name.starts_with("en")
        || iface.name.starts_with("wlan")
        || iface.name.starts_with("wifi")
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn is_physical_interface(iface: &if_addrs::Interface) -> bool {
    // Apple groups every physical adapter (Ethernet and Wi-Fi) under the
    // `en*` family; tunnels (utun*), bridges and Apple-link devices
    // (awdl0, llw0, p2p0) use other prefixes. This matches what
    // `SCNetworkInterfaceCopyAll` reports without adding a framework.
    iface.name.starts_with("en")
}

#[cfg(target_os = "windows")]
fn is_physical_interface(iface: &if_addrs::Interface) -> bool {
    use windows_sys::Win32::NetworkManagement::IpHelper::{
        GetAdaptersAddresses, GAA_FLAG_SKIP_ANYCAST, GAA_FLAG_SKIP_DNS_SERVER,
        IF_TYPE_ETHERNET_CSMACD, IF_TYPE_IEEE80211, IP_ADAPTER_ADDRESSES_LH,
    };
    use windows_sys::Win32::NetworkManagement::Ndis::TUNNEL_TYPE_NONE;
    use windows_sys::Win32::Networking::WinSock::AF_UNSPEC;

    let Some(index) = iface.index else {
        return false;
    };
    let flags = GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_DNS_SERVER;

    // First call only queries the required buffer size.
    let mut size: u32 = 0;
    let _ = unsafe {
        GetAdaptersAddresses(
            AF_UNSPEC as u32,
            flags,
            std::ptr::null(),
            std::ptr::null_mut(),
            &mut size,
        )
    };

    // `GetAdaptersAddresses` writes variable-length records into the buffer,
    // so allocate raw memory at the struct's alignment instead of a Vec<u8>.
    let layout = std::alloc::Layout::from_size_align(
        size.max(15000) as usize,
        std::mem::align_of::<IP_ADAPTER_ADDRESSES_LH>(),
    )
    .unwrap();
    let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
    let ret = unsafe {
        GetAdaptersAddresses(
            AF_UNSPEC as u32,
            flags,
            std::ptr::null(),
            ptr as *mut IP_ADAPTER_ADDRESSES_LH,
            &mut size,
        )
    };
    if ret != 0 {
        unsafe { std::alloc::dealloc(ptr, layout) };
        return false;
    }

    let mut physical = false;
    let mut cur = ptr as *const IP_ADAPTER_ADDRESSES_LH;
    while !cur.is_null() {
        unsafe {
            let adapter = &*cur;
            if adapter.Anonymous1.Anonymous.IfIndex == index {
                // Physical adapters report an Ethernet or Wi-Fi type and no
                // tunnel type; tunnels and loopback use other values.
                // TAP/Wintun (OpenVPN, WireGuard) fake the Ethernet type,
                // so their friendly names are checked against virtual
                // markers too.
                physical = (adapter.IfType == IF_TYPE_ETHERNET_CSMACD
                    || adapter.IfType == IF_TYPE_IEEE80211)
                    && adapter.TunnelType == TUNNEL_TYPE_NONE
                    && !friendly_name_is_virtual(adapter.FriendlyName);
                break;
            }
            cur = adapter.Next;
        }
    }
    unsafe { std::alloc::dealloc(ptr, layout) };
    physical
}

#[cfg(target_os = "windows")]
fn friendly_name_is_virtual(friendly: windows_sys::core::PWSTR) -> bool {
    if friendly.is_null() {
        return false;
    }
    let name = unsafe {
        let mut len = 0;
        while *friendly.add(len) != 0 {
            len += 1;
        }
        String::from_utf16_lossy(std::slice::from_raw_parts(friendly, len))
    }
    .to_ascii_lowercase();
    const VIRTUAL_MARKERS: &[&str] = &[
        "tap",
        "tun",
        "vpn",
        "virtual",
        "wireguard",
        "wintun",
        "openvpn",
        "tailscale",
        "zerotier",
        "docker",
        "hyper-v",
        "wsl",
        "npcap",
        "loopback",
        "bluetooth",
        "vethernet",
    ];
    VIRTUAL_MARKERS.iter().any(|marker| name.contains(marker))
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
)))]
fn is_physical_interface(iface: &if_addrs::Interface) -> bool {
    // No platform signal available: fall back to excluding well-known
    // virtual names.
    !is_virtual_name(&iface.name)
}

pub const fn is_ipv4_global(ipv4: &Ipv4Addr) -> bool {
    !(ipv4.octets()[0] == 0 // "This network"
        || ipv4.is_private()
        || ipv4.octets()[0] == 100 && (ipv4.octets()[1] & 0b1100_0000 == 0b0100_0000)//ipv4.is_shared()
        || ipv4.is_loopback()
        || ipv4.is_link_local()
        // addresses reserved for future protocols (`192.0.0.0/24`)
        // .9 and .10 are documented as globally reachable so they're excluded
        || (
        ipv4.octets()[0] == 192 && ipv4.octets()[1] == 0 && ipv4.octets()[2] == 0
            && ipv4.octets()[3] != 9 && ipv4.octets()[3] != 10
    )
        || ipv4.is_documentation()
        || ipv4.octets()[0] == 198 && (ipv4.octets()[1] & 0xfe) == 18//ipv4.is_benchmarking()
        || ipv4.octets()[0] & 240 == 240 && !ipv4.is_broadcast()//ipv4.is_reserved()
        || ipv4.is_broadcast())
}

pub const fn is_ipv6_global(ipv6addr: &Ipv6Addr) -> bool {
    !(ipv6addr.is_unspecified()
        || ipv6addr.is_loopback()
        // IPv4-mapped Address (`::ffff:0:0/96`)
        || matches!(ipv6addr.segments(), [0, 0, 0, 0, 0, 0xffff, _, _])
        // IPv4-IPv6 Translat. (`64:ff9b:1::/48`)
        || matches!(ipv6addr.segments(), [0x64, 0xff9b, 1, _, _, _, _, _])
        // Discard-Only Address Block (`100::/64`)
        || matches!(ipv6addr.segments(), [0x100, 0, 0, 0, _, _, _, _])
        // IETF Protocol Assignments (`2001::/23`)
        || (matches!(ipv6addr.segments(), [0x2001, b, _, _, _, _, _, _] if b < 0x200)
        && !(
        // Port Control Protocol Anycast (`2001:1::1`)
        u128::from_be_bytes(ipv6addr.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0001
            // Traversal Using Relays around NAT Anycast (`2001:1::2`)
            || u128::from_be_bytes(ipv6addr.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0002
            // AMT (`2001:3::/32`)
            || matches!(ipv6addr.segments(), [0x2001, 3, _, _, _, _, _, _])
            // AS112-v6 (`2001:4:112::/48`)
            || matches!(ipv6addr.segments(), [0x2001, 4, 0x112, _, _, _, _, _])
            // ORCHIDv2 (`2001:20::/28`)
            || matches!(ipv6addr.segments(), [0x2001, b, _, _, _, _, _, _] if b >= 0x20 && b <= 0x2F)
    ))
        || (ipv6addr.segments()[0] == 0x2001) && (ipv6addr.segments()[1] == 0xdb8)//ipv6addr.is_documentation()
        || (ipv6addr.segments()[0] & 0xfe00) == 0xfc00//ipv6addr.is_unique_local()
        || (ipv6addr.segments()[0] & 0xffc0) == 0xfe80) //ipv6addr.is_unicast_link_local())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_ips_are_sane() {
        let ips = local_ips(None);
        assert!(ips.ipv4s.iter().all(|ip| {
            !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast() && !ip.is_broadcast()
        }));
        assert!(ips.ipv6s.iter().all(is_ipv6_global));
        let mut v4 = ips.ipv4s.clone();
        v4.sort_unstable();
        v4.dedup();
        assert_eq!(ips.ipv4s, v4);
        let mut v6 = ips.ipv6s.clone();
        v6.sort_unstable();
        v6.dedup();
        assert_eq!(ips.ipv6s, v6);
    }

    #[test]
    fn bound_interface_filters_to_that_nic_only() {
        // Bound to a NIC that cannot exist: the scan must come up empty
        // rather than falling back to all physical interfaces.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let fake = LocalInterface::new("rp2p-nonexistent".to_owned());
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let fake = LocalInterface::new(u32::MAX);
        let ips = local_ips(Some(&fake));
        assert!(ips.ipv4s.is_empty() && ips.ipv6s.is_empty());
    }

    #[test]
    fn virtual_name_detection() {
        for name in [
            "eth0",
            "en0",
            "enp3s0",
            "eno1",
            "wlan0",
            "wlp2s0",
            "Ethernet",
            "Wi-Fi",
            "Local Area Connection",
        ] {
            assert!(!is_virtual_name(name), "{name} misclassified as virtual");
        }
        for name in [
            "tun0",
            "tap0",
            "utun3",
            "wg0",
            "wireguard",
            "ppp0",
            "docker0",
            "br0",
            "bridge0",
            "veth123",
            "virbr0",
            "vmnet8",
            "vethernet",
            "awdl0",
            "llw0",
            "p2p0",
            "isatap.foo",
            "6to4",
            "teredo",
        ] {
            assert!(is_virtual_name(name), "{name} misclassified as physical");
        }
    }

    #[test]
    fn is_ipv6_global_classifies_well_known_blocks() {
        assert!(!is_ipv6_global(&Ipv6Addr::UNSPECIFIED));
        assert!(!is_ipv6_global(&Ipv6Addr::LOCALHOST));
        assert!(!is_ipv6_global(&"fe80::1".parse().unwrap()));
        assert!(!is_ipv6_global(&"fd12:3456::1".parse().unwrap()));
        assert!(!is_ipv6_global(&"::ffff:192.0.2.1".parse().unwrap()));
        assert!(is_ipv6_global(&"2400:3200::1".parse().unwrap()));
        assert!(is_ipv6_global(&"2001:4860:4860::8888".parse().unwrap()));
    }
}
