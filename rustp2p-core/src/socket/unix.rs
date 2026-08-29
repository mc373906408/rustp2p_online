use crate::socket::{LocalInterface, SocketTrait};

#[cfg(target_os = "freebsd")]
impl SocketTrait for socket2::Socket {
    fn set_ip_unicast_if(
        &self,
        _interface: &LocalInterface,
        _is_ipv6: bool,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "interface binding is not supported on FreeBSD",
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
impl SocketTrait for socket2::Socket {
    fn set_ip_unicast_if(&self, interface: &LocalInterface, _is_ipv6: bool) -> std::io::Result<()> {
        self.bind_device(Some(interface.name.as_bytes()))?;
        Ok(())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios",))]
impl SocketTrait for socket2::Socket {
    fn set_ip_unicast_if(&self, interface: &LocalInterface, is_ipv6: bool) -> std::io::Result<()> {
        let index = std::num::NonZeroU32::new(interface.index);
        if is_ipv6 {
            self.bind_device_by_index_v6(index)?;
        } else {
            self.bind_device_by_index_v4(index)?;
        }
        Ok(())
    }
}
