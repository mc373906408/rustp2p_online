use std::io;
use std::os::windows::io::AsRawSocket;

use windows_sys::core::PCSTR;
use windows_sys::Win32::Networking::WinSock::{
    htonl, setsockopt, WSAIoctl, IPPROTO_IP, IPPROTO_IPV6, IPV6_UNICAST_IF, IP_UNICAST_IF,
    SIO_UDP_CONNRESET, SOCKET_ERROR,
};

use crate::socket::{LocalInterface, SocketTrait};

impl SocketTrait for socket2::Socket {
    fn set_ip_unicast_if(&self, interface: &LocalInterface, is_ipv6: bool) -> io::Result<()> {
        let index = interface.index;
        let raw_socket = self.as_raw_socket();
        let result = unsafe {
            // Windows expects the IPv4 interface index in network byte order,
            // while IPV6_UNICAST_IF takes it in host byte order.
            let best_interface = if is_ipv6 { index } else { htonl(index) };
            let (level, option) = if is_ipv6 {
                (IPPROTO_IPV6, IPV6_UNICAST_IF)
            } else {
                (IPPROTO_IP, IP_UNICAST_IF)
            };
            setsockopt(
                raw_socket as usize,
                level,
                option,
                &best_interface as *const _ as PCSTR,
                std::mem::size_of_val(&best_interface) as i32,
            )
        };
        if result == SOCKET_ERROR {
            Err(io::Error::last_os_error())?;
        }
        Ok(())
    }
}

pub(crate) fn ignore_conn_reset(socket: &socket2::Socket) -> io::Result<()> {
    let socket_raw = socket.as_raw_socket() as usize;
    let mut bytes_returned: u32 = 0;
    let mut flag: u32 = 0;

    // Set SIO_UDP_CONNRESET to FALSE (0) to ignore ICMP errors
    let result = unsafe {
        WSAIoctl(
            socket_raw,
            SIO_UDP_CONNRESET,
            &mut flag as *mut _ as *mut _,
            std::mem::size_of_val(&flag) as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned as *mut _,
            std::ptr::null_mut(),
            None,
        )
    };

    if result == SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
