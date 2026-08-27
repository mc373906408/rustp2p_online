use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
use std::{mem, os::fd::AsRawFd};
#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, mpsc};

use crate::endpoint::codec::InitCodec;

#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
const UDP_BATCH_SIZE: usize = 16;
#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
const UDP_RECV_BUFFER_SIZE: usize = 65_536;

/// A managed assistant UDP socket entry with its own shutdown signal.
struct UdpEntry {
    socket: Arc<UdpSocket>,
    /// Per-socket shutdown sender for the assistant socket.
    _shutdown: broadcast::Sender<()>,
}

/// A TCP connection with Encoder for writing.
pub struct TcpConnection {
    pub peer_addr: SocketAddr,
    write_tx: mpsc::Sender<Bytes>,
}

impl TcpConnection {
    pub async fn send(&self, data: &[u8]) -> io::Result<()> {
        self.write_tx
            .send(Bytes::copy_from_slice(data))
            .await
            .map_err(|_| io::Error::other("TCP connection closed"))
    }
}

/// A shared pool of sockets. Owns all Arcs.
///
/// The main IPv4 UDP socket is fixed at construction time and never changes,
/// so it needs no lock. A main IPv6 UDP socket is optionally bound when IPv6
/// is enabled and supported. Assistant UDP sockets are created and removed
/// dynamically (e.g. symmetric NAT probing) and live in a locked list.
pub struct SocketPool {
    main_udp_v4: Arc<UdpSocket>,
    main_udp_v6: Option<Arc<UdpSocket>>,
    assistant_udp: parking_lot::RwLock<Vec<UdpEntry>>,
    tcp_conns: parking_lot::RwLock<HashMap<SocketAddr, Arc<TcpConnection>>>,
    data_tx: mpsc::Sender<(super::transport::Transport, Bytes)>,
    /// Global shutdown - kills ALL tasks (main + sub)
    global_shutdown: broadcast::Sender<()>,
    init_codec: Box<dyn InitCodec>,
    connect_lock: tokio::sync::Mutex<()>,
}

impl SocketPool {
    /// Create a pool from the main IPv4 UDP socket and an optional main IPv6
    /// UDP socket.
    pub fn new(
        main_udp_v4: UdpSocket,
        main_udp_v6: Option<UdpSocket>,
        init_codec: Box<dyn InitCodec>,
    ) -> (Self, mpsc::Receiver<(super::transport::Transport, Bytes)>) {
        let (data_tx, data_rx) = mpsc::channel(512);
        let (global_shutdown, _) = broadcast::channel(4);

        let main_udp_v4 = Arc::new(main_udp_v4);
        Self::spawn_udp_tasks(main_udp_v4.clone(), &data_tx, &global_shutdown);
        let main_udp_v6 = main_udp_v6.map(|s| {
            let s = Arc::new(s);
            Self::spawn_udp_tasks(s.clone(), &data_tx, &global_shutdown);
            s
        });

        let pool = Self {
            main_udp_v4,
            main_udp_v6,
            assistant_udp: parking_lot::RwLock::new(Vec::new()),
            tcp_conns: parking_lot::RwLock::new(HashMap::new()),
            data_tx,
            global_shutdown,
            init_codec,
            connect_lock: tokio::sync::Mutex::new(()),
        };
        (pool, data_rx)
    }

    /// Spawn the read and write tasks for one UDP socket.
    fn spawn_udp_tasks(
        socket: Arc<UdpSocket>,
        data_tx: &mpsc::Sender<(super::transport::Transport, Bytes)>,
        global_shutdown: &broadcast::Sender<()>,
    ) {
        let (write_tx, write_rx) = mpsc::channel::<(Bytes, SocketAddr)>(64);

        let mut global_shutdown_rx = global_shutdown.subscribe();
        let mut socket_shutdown_rx = global_shutdown.subscribe();
        let data_tx_clone = data_tx.clone();
        let s = socket.clone();
        tokio::spawn(async move {
            Self::run_udp_reader(
                s,
                write_tx,
                data_tx_clone,
                &mut global_shutdown_rx,
                &mut socket_shutdown_rx,
            )
            .await;
        });

        let mut global_shutdown_rx = global_shutdown.subscribe();
        let mut socket_shutdown_rx = global_shutdown.subscribe();
        let s = socket.clone();
        tokio::spawn(async move {
            Self::run_udp_writer(
                s,
                write_rx,
                &mut global_shutdown_rx,
                &mut socket_shutdown_rx,
            )
            .await;
        });
    }

    /// Add an assistant UDP socket (for symmetric NAT probing).
    /// Its reader task exits when the assistant socket is removed.
    pub fn add_assistant_udp(&self, socket: UdpSocket) -> Weak<UdpSocket> {
        let socket = Arc::new(socket);
        let weak = Arc::downgrade(&socket);

        // Per-socket shutdown for this assistant socket
        let (socket_shutdown, mut socket_shutdown_rx) = broadcast::channel(4);
        let (write_tx, write_rx) = mpsc::channel::<(Bytes, SocketAddr)>(64);
        let data_tx = self.data_tx.clone();
        let mut global_shutdown_rx = self.global_shutdown.subscribe();
        let s = socket.clone();

        tokio::spawn(async move {
            Self::run_udp_reader(
                s,
                write_tx,
                data_tx,
                &mut global_shutdown_rx,
                &mut socket_shutdown_rx,
            )
            .await;
        });

        let mut writer_shutdown_rx = socket_shutdown.subscribe();
        let mut writer_global_shutdown_rx = self.global_shutdown.subscribe();
        let s = socket.clone();
        tokio::spawn(async move {
            Self::run_udp_writer(
                s,
                write_rx,
                &mut writer_global_shutdown_rx,
                &mut writer_shutdown_rx,
            )
            .await;
        });

        let entry = UdpEntry {
            socket,
            _shutdown: socket_shutdown,
        };

        let mut sockets = self.assistant_udp.write();
        sockets.push(entry);
        drop(sockets);

        weak
    }

    /// Clean all assistant UDP sockets and cancel their reader tasks.
    pub fn clean_assistant_udp(&self) {
        let mut sockets = self.assistant_udp.write();
        // Dropping UdpEntry drops _shutdown Sender, reader task exits.
        sockets.clear();
    }

    /// Remove a TCP connection from the pool by peer address.
    pub(crate) fn remove_tcp(&self, addr: SocketAddr) {
        self.tcp_conns.write().remove(&addr);
    }

    /// Add a TCP connection with Decoder/Encoder.
    pub fn add_tcp(
        self: &Arc<Self>,
        stream: tokio::net::TcpStream,
        peer_addr: SocketAddr,
    ) -> io::Result<Weak<TcpConnection>> {
        let (read_half, mut write_half) = stream.into_split();
        let local_addr = read_half
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let (mut decoder, mut encoder) = self.init_codec.codec(peer_addr)?;
        let (write_tx, mut write_rx) = mpsc::channel::<Bytes>(64);
        let data_tx = self.data_tx.clone();
        let mut shutdown_rx = self.global_shutdown.subscribe();

        // Create Arc<TcpConnection> first so we can get a real Weak reference
        let conn = Arc::new(TcpConnection {
            peer_addr,
            write_tx,
        });
        let conn_write_tx = conn.write_tx.clone();

        // Read loop using Decoder
        let pool_for_read = self.clone();
        tokio::spawn(async move {
            let mut read = read_half;
            let mut data_buf = vec![0u8; 65536];
            loop {
                tokio::select! {
                    result = decoder.decode(&mut read, &mut data_buf) => {
                        match result {
                            Ok(len) => {
                                let data = Bytes::copy_from_slice(&data_buf[..len]);
                                let route = super::transport::Transport::tcp(conn_write_tx.clone(), local_addr, peer_addr);
                                let _ = data_tx.send((route, data)).await;
                            }
                            Err(e) => {
                                if e.kind() != io::ErrorKind::UnexpectedEof {
                                    log::warn!("TCP decode error: {e}");
                                }
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::debug!("TCP read task shutting down");
                        break;
                    }
                }
            }
            pool_for_read.remove_tcp(peer_addr);
        });

        // Write loop using Encoder
        let pool_for_write = self.clone();
        let mut shutdown_rx = self.global_shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    data = write_rx.recv() => {
                        match data {
                            Some(data) => {
                                if let Err(e) = encoder.encode(&mut write_half, &data).await {
                                    log::warn!("TCP encode error: {e}");
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        log::debug!("TCP write task shutting down");
                        break;
                    }
                }
            }
            pool_for_write.remove_tcp(peer_addr);
        });

        let weak = Arc::downgrade(&conn);
        self.tcp_conns.write().insert(peer_addr, conn);
        Ok(weak)
    }

    /// Send data through ALL assistant UDP sockets to a specific address.
    pub fn try_send_via_assistants(&self, buf: &[u8], addr: SocketAddr) -> io::Result<()> {
        let sockets = self.assistant_udp.read();
        for entry in sockets.iter() {
            entry
                .socket
                .try_send_to(buf, addr)
                .map_err(|e| io::Error::other(format!("assistant send failed: {e}")))?;
        }
        Ok(())
    }

    /// Send data to an address via the matching family main UDP socket.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<()> {
        if addr.is_ipv4() {
            return self
                .main_udp_v4
                .try_send_to(buf, addr)
                .map(|_| ())
                .map_err(|e| io::Error::other(format!("send failed: {e}")));
        }
        let v6 = self
            .main_udp_v6
            .as_ref()
            .ok_or_else(|| io::Error::other("IPv6 main socket not available"))?;
        v6.try_send_to(buf, addr)
            .map(|_| ())
            .map_err(|e| io::Error::other(format!("send failed: {e}")))
    }

    /// Send data through ALL UDP sockets (main v4/v6 + assistant) to a specific address.
    pub fn try_send_via_all(&self, buf: &[u8], addr: SocketAddr) {
        let _ = self.main_udp_v4.try_send_to(buf, addr);
        if let Some(v6) = &self.main_udp_v6 {
            let _ = v6.try_send_to(buf, addr);
        }
        let sockets = self.assistant_udp.read();
        for entry in sockets.iter() {
            let _ = entry.socket.try_send_to(buf, addr);
        }
    }

    /// Shutdown all tasks (program exit).
    pub fn shutdown(&self) {
        let _ = self.global_shutdown.send(());
    }

    /// Get a shutdown receiver to listen for shutdown signals.
    pub fn shutdown_rx(&self) -> broadcast::Receiver<()> {
        self.global_shutdown.subscribe()
    }

    /// Get local address of the main IPv4 UDP socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.main_udp_v4.local_addr()
    }

    /// Find a TCP connection by peer address.
    pub fn find_tcp(&self, addr: SocketAddr) -> Option<Arc<TcpConnection>> {
        self.tcp_conns.read().get(&addr).cloned()
    }

    /// Get or create a TCP connection to the given address (with concurrency protection).
    pub async fn connect_tcp_internal(
        self: &Arc<Self>,
        addr: SocketAddr,
    ) -> io::Result<Arc<TcpConnection>> {
        if let Some(conn) = self.find_tcp(addr) {
            return Ok(conn);
        }
        let _guard = self.connect_lock.lock().await;
        if let Some(conn) = self.find_tcp(addr) {
            return Ok(conn);
        }
        let stream = crate::socket::connect_tcp(addr, 0, None, None).await?;
        let weak = self.add_tcp(stream, addr)?;
        weak.upgrade()
            .ok_or_else(|| io::Error::other("connection dropped immediately"))
    }

    /// Get all TCP connections.
    pub fn tcp_connections(&self) -> Vec<Arc<TcpConnection>> {
        self.tcp_conns.read().values().cloned().collect()
    }

    /// Get a UDP socket by index: 0 = main IPv4, then main IPv6 (if present),
    /// then assistants.
    pub fn udp_socket(&self, index: usize) -> Option<Arc<UdpSocket>> {
        let main_count = 1 + usize::from(self.main_udp_v6.is_some());
        match index {
            0 => Some(self.main_udp_v4.clone()),
            1 if self.main_udp_v6.is_some() => self.main_udp_v6.clone(),
            _ => self
                .assistant_udp
                .read()
                .get(index - main_count)
                .map(|e| e.socket.clone()),
        }
    }

    /// Get all UDP sockets: main IPv4, main IPv6 (if present), then assistants.
    pub fn udp_sockets(&self) -> Vec<Arc<UdpSocket>> {
        let mut sockets = vec![self.main_udp_v4.clone()];
        sockets.extend(self.main_udp_v6.iter().cloned());
        sockets.extend(self.assistant_udp.read().iter().map(|e| e.socket.clone()));
        sockets
    }

    /// Get the number of assistant sockets.
    pub fn assistant_count(&self) -> usize {
        self.assistant_udp.read().len()
    }

    /// Get the main IPv4 UDP socket.
    pub fn main_socket(&self) -> Option<Arc<UdpSocket>> {
        Some(self.main_udp_v4.clone())
    }

    async fn run_udp_reader(
        socket: Arc<UdpSocket>,
        write_tx: mpsc::Sender<(Bytes, SocketAddr)>,
        data_tx: mpsc::Sender<(super::transport::Transport, Bytes)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        #[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
        {
            Self::run_udp_reader_mmsg(
                socket,
                write_tx,
                data_tx,
                global_shutdown_rx,
                socket_shutdown_rx,
            )
            .await;
        }
        #[cfg(not(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android"))))]
        {
            Self::run_udp_reader_single(
                socket,
                write_tx,
                data_tx,
                global_shutdown_rx,
                socket_shutdown_rx,
            )
            .await;
        }
    }

    #[cfg(not(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android"))))]
    async fn run_udp_reader_single(
        socket: Arc<UdpSocket>,
        write_tx: mpsc::Sender<(Bytes, SocketAddr)>,
        data_tx: mpsc::Sender<(super::transport::Transport, Bytes)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        let mut buf = [0u8; 65536];
        let local_addr = socket
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, addr)) => {
                            let data = Bytes::copy_from_slice(&buf[..len]);
                            let route = super::transport::Transport::udp(write_tx.clone(), local_addr, addr);
                            if data_tx.send((route, data)).await.is_err() {
                                return;
                            }
                        }
                        Err(e) if is_recoverable_udp_recv_error(&e) => {
                            log::debug!("Ignoring recoverable UDP recv error: {e}");
                            continue;
                        }
                        Err(e) => {
                            log::warn!("UDP recv error: {e}");
                            break;
                        }
                    }
                }
                _ = global_shutdown_rx.recv() => {
                    log::debug!("UDP read task shutting down (global)");
                    break;
                }
                _ = socket_shutdown_rx.recv() => {
                    log::debug!("UDP read task shutting down (socket)");
                    break;
                }
            }
        }
    }

    #[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
    async fn run_udp_reader_mmsg(
        socket: Arc<UdpSocket>,
        write_tx: mpsc::Sender<(Bytes, SocketAddr)>,
        data_tx: mpsc::Sender<(super::transport::Transport, Bytes)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        let mut buffers = (0..UDP_BATCH_SIZE)
            .map(|_| Vec::with_capacity(UDP_RECV_BUFFER_SIZE))
            .collect::<Vec<_>>();
        let mut peer_addrs = [SocketAddr::from(([0, 0, 0, 0], 0)); UDP_BATCH_SIZE];
        let local_addr = socket
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let fd = socket.as_raw_fd();

        loop {
            let count = tokio::select! {
                result = socket.async_io(Interest::READABLE, || {
                    recv_mmsg(fd, &mut buffers, &mut peer_addrs)
                }) => {
                    match result {
                        Ok(count) => count,
                        Err(e) if is_recoverable_udp_recv_error(&e) => {
                            log::debug!("Ignoring recoverable UDP recvmmsg error: {e}");
                            continue;
                        }
                        Err(e) => {
                            log::warn!("UDP recvmmsg error: {e}");
                            break;
                        }
                    }
                }
                _ = global_shutdown_rx.recv() => {
                    log::debug!("UDP read task shutting down (global)");
                    break;
                }
                _ = socket_shutdown_rx.recv() => {
                    log::debug!("UDP read task shutting down (socket)");
                    break;
                }
            };

            for index in 0..count {
                let data = Bytes::copy_from_slice(&buffers[index]);
                let route = super::transport::Transport::udp(
                    write_tx.clone(),
                    local_addr,
                    peer_addrs[index],
                );
                if data_tx.send((route, data)).await.is_err() {
                    return;
                }
            }
        }
    }

    /// Writer task for a UDP socket: drains `(data, peer)` messages from the
    /// channel and sends them through the socket. Exits when the channel
    /// closes or either shutdown signal fires.
    async fn run_udp_writer(
        socket: Arc<UdpSocket>,
        write_rx: mpsc::Receiver<(Bytes, SocketAddr)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        #[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
        {
            Self::run_udp_writer_mmsg(socket, write_rx, global_shutdown_rx, socket_shutdown_rx)
                .await;
        }
        #[cfg(not(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android"))))]
        {
            Self::run_udp_writer_single(socket, write_rx, global_shutdown_rx, socket_shutdown_rx)
                .await;
        }
    }

    #[cfg(not(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android"))))]
    async fn run_udp_writer_single(
        socket: Arc<UdpSocket>,
        mut write_rx: mpsc::Receiver<(Bytes, SocketAddr)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                msg = write_rx.recv() => {
                    match msg {
                        Some((data, addr)) => {
                            if let Err(e) = socket.send_to(&data, addr).await {
                                log::warn!("UDP send error: {e}");
                            }
                        }
                        None => break,
                    }
                }
                _ = global_shutdown_rx.recv() => {
                    log::debug!("UDP write task shutting down (global)");
                    break;
                }
                _ = socket_shutdown_rx.recv() => {
                    log::debug!("UDP write task shutting down (socket)");
                    break;
                }
            }
        }
        // Keep socket alive until writer exits
        drop(socket);
    }

    #[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
    async fn run_udp_writer_mmsg(
        socket: Arc<UdpSocket>,
        mut write_rx: mpsc::Receiver<(Bytes, SocketAddr)>,
        global_shutdown_rx: &mut broadcast::Receiver<()>,
        socket_shutdown_rx: &mut broadcast::Receiver<()>,
    ) {
        let fd = socket.as_raw_fd();
        let mut batch = Vec::with_capacity(UDP_BATCH_SIZE);

        'writer: loop {
            batch.clear();
            let first = tokio::select! {
                msg = write_rx.recv() => msg,
                _ = global_shutdown_rx.recv() => {
                    log::debug!("UDP write task shutting down (global)");
                    break;
                }
                _ = socket_shutdown_rx.recv() => {
                    log::debug!("UDP write task shutting down (socket)");
                    break;
                }
            };
            let Some(first) = first else {
                break;
            };
            batch.push(first);
            while batch.len() < UDP_BATCH_SIZE {
                match write_rx.try_recv() {
                    Ok(message) => batch.push(message),
                    Err(_) => break,
                }
            }

            let mut sent = 0;
            while sent < batch.len() {
                if batch.len() - sent == 1 {
                    let (data, addr) = &batch[sent];
                    let result = tokio::select! {
                        result = socket.send_to(data, *addr) => Some(result),
                        _ = global_shutdown_rx.recv() => {
                            log::debug!("UDP write task shutting down (global)");
                            None
                        }
                        _ = socket_shutdown_rx.recv() => {
                            log::debug!("UDP write task shutting down (socket)");
                            None
                        }
                    };
                    let Some(result) = result else {
                        break 'writer;
                    };
                    if let Err(e) = result {
                        log::warn!("UDP send error: {e}");
                    }
                    sent += 1;
                    continue;
                }

                let result = tokio::select! {
                    result = socket.async_io(Interest::WRITABLE, || {
                        send_mmsg(fd, &batch[sent..])
                    }) => Some(result),
                    _ = global_shutdown_rx.recv() => {
                        log::debug!("UDP write task shutting down (global)");
                        None
                    }
                    _ = socket_shutdown_rx.recv() => {
                        log::debug!("UDP write task shutting down (socket)");
                        None
                    }
                };
                let Some(result) = result else {
                    break 'writer;
                };
                match result {
                    Ok(count) => sent += count,
                    Err(batch_error) => {
                        // sendmmsg returns an error only if it sent no messages.
                        // Retry the first message separately so one bad datagram
                        // does not cause the rest of the batch to be discarded.
                        let (data, addr) = &batch[sent];
                        let result = tokio::select! {
                            result = socket.send_to(data, *addr) => Some(result),
                            _ = global_shutdown_rx.recv() => {
                                log::debug!("UDP write task shutting down (global)");
                                None
                            }
                            _ = socket_shutdown_rx.recv() => {
                                log::debug!("UDP write task shutting down (socket)");
                                None
                            }
                        };
                        let Some(result) = result else {
                            break 'writer;
                        };
                        if let Err(e) = result {
                            log::warn!(
                                "UDP sendmmsg error: {batch_error}; single send to {addr} failed: {e}"
                            );
                        } else {
                            log::warn!(
                                "UDP sendmmsg error: {batch_error}; retried {addr} successfully"
                            );
                        }
                        sent += 1;
                    }
                }
            }
        }
        drop(socket);
    }
}

fn is_recoverable_udp_recv_error(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::Interrupted {
        return true;
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Some(code) = error.raw_os_error() {
        return matches!(
            code,
            libc::ECONNREFUSED
                | libc::ECONNRESET
                | libc::ECONNABORTED
                | libc::EHOSTUNREACH
                | libc::ENETUNREACH
                | libc::EADDRNOTAVAIL
                | libc::EPROTO
                | libc::EMSGSIZE
        );
    }

    false
}

#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
fn recv_mmsg(
    fd: std::os::fd::RawFd,
    buffers: &mut [Vec<u8>],
    peer_addrs: &mut [SocketAddr],
) -> io::Result<usize> {
    let count = buffers.len().min(peer_addrs.len()).min(UDP_BATCH_SIZE);
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "recvmmsg requires at least one buffer",
        ));
    }

    let mut iovecs: [libc::iovec; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };
    let mut messages: [libc::mmsghdr; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };
    let mut addresses: [libc::sockaddr_storage; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };

    for index in 0..count {
        buffers[index].clear();
        iovecs[index].iov_base = buffers[index].as_mut_ptr().cast();
        iovecs[index].iov_len = buffers[index].capacity();
        messages[index].msg_hdr.msg_iov = &mut iovecs[index];
        messages[index].msg_hdr.msg_iovlen = 1;
        messages[index].msg_hdr.msg_name =
            (&mut addresses[index] as *mut libc::sockaddr_storage).cast::<libc::c_void>();
        messages[index].msg_hdr.msg_namelen =
            mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    }

    let received = loop {
        let result = unsafe {
            libc::recvmmsg(
                fd,
                messages.as_mut_ptr(),
                count as libc::c_uint,
                libc::MSG_DONTWAIT as _,
                std::ptr::null_mut(),
            )
        };
        if result >= 0 {
            break result as usize;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    if received == 0 {
        return Err(io::Error::from(io::ErrorKind::WouldBlock));
    }

    for index in 0..received {
        peer_addrs[index] =
            sockaddr_to_socket_addr(&addresses[index], messages[index].msg_hdr.msg_namelen)?;
        let length = (messages[index].msg_len as usize).min(buffers[index].capacity());
        // SAFETY: recvmmsg initialized exactly `msg_len` bytes in this
        // buffer, capped above by the iovec capacity supplied to the kernel.
        unsafe {
            buffers[index].set_len(length);
        }
    }
    Ok(received)
}

#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
fn send_mmsg(fd: std::os::fd::RawFd, buffers: &[(Bytes, SocketAddr)]) -> io::Result<usize> {
    let count = buffers.len().min(UDP_BATCH_SIZE);
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sendmmsg requires at least one message",
        ));
    }

    let mut iovecs: [libc::iovec; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };
    let mut messages: [libc::mmsghdr; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };
    let mut addresses: [libc::sockaddr_storage; UDP_BATCH_SIZE] = unsafe { mem::zeroed() };

    for (index, (buffer, addr)) in buffers[..count].iter().enumerate() {
        let (storage, addr_len) = socket_addr_to_sockaddr(addr);
        addresses[index] = storage;
        iovecs[index].iov_base = buffer.as_ptr() as *mut libc::c_void;
        iovecs[index].iov_len = buffer.len();
        messages[index].msg_hdr.msg_iov = &mut iovecs[index];
        messages[index].msg_hdr.msg_iovlen = 1;
        messages[index].msg_hdr.msg_name =
            (&mut addresses[index] as *mut libc::sockaddr_storage).cast::<libc::c_void>();
        messages[index].msg_hdr.msg_namelen = addr_len;
    }

    loop {
        let sent = unsafe {
            libc::sendmmsg(
                fd,
                messages.as_mut_ptr(),
                count as libc::c_uint,
                libc::MSG_DONTWAIT as _,
            )
        };
        if sent > 0 {
            return Ok(sent as usize);
        }
        if sent == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
fn socket_addr_to_sockaddr(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    match addr {
        SocketAddr::V4(addr) => {
            let raw = (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in>();
            unsafe {
                (*raw).sin_family = libc::AF_INET as libc::sa_family_t;
                (*raw).sin_port = addr.port().to_be();
                (*raw).sin_addr.s_addr = u32::from_ne_bytes(addr.ip().octets());
            }
            (
                storage,
                mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(addr) => {
            let raw = (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>();
            unsafe {
                (*raw).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*raw).sin6_port = addr.port().to_be();
                (*raw).sin6_flowinfo = addr.flowinfo();
                (*raw).sin6_addr.s6_addr = addr.ip().octets();
                (*raw).sin6_scope_id = addr.scope_id();
            }
            (
                storage,
                mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

#[cfg(all(feature = "sendmmsg", any(target_os = "linux", target_os = "android")))]
fn sockaddr_to_socket_addr(
    storage: &libc::sockaddr_storage,
    len: libc::socklen_t,
) -> io::Result<SocketAddr> {
    match storage.ss_family as libc::c_int {
        libc::AF_INET if (len as usize) >= mem::size_of::<libc::sockaddr_in>() => {
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            Ok(SocketAddr::V4(std::net::SocketAddrV4::new(
                std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr)),
                u16::from_be(addr.sin_port),
            )))
        }
        libc::AF_INET6 if (len as usize) >= mem::size_of::<libc::sockaddr_in6>() => {
            let addr = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::from(addr.sin6_addr.s6_addr),
                u16::from_be(addr.sin6_port),
                addr.sin6_flowinfo,
                addr.sin6_scope_id,
            )))
        }
        family => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported sockaddr family or length: family={family}, len={len}"),
        )),
    }
}

/// A lightweight handle for sending data and querying socket state.
///
/// `Sender` is cloneable and can be moved into async tasks.
/// It provides send methods and read-only query methods without
/// exposing internal socket management (add/remove/clean).
///
/// # Examples
///
/// ```rust,no_run
/// use rustp2p_core::endpoint::{EndPoint, Config};
///
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// let ep = EndPoint::bind(Config::new().udp_port(3000)).await?;
/// let sender = ep.sender();
///
/// // Send to a known address
/// sender.try_send_via_all(b"hello", "127.0.0.1:4000".parse().unwrap());
///
/// // Query local address
/// println!("Listening on: {:?}", sender.local_addr());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Sender(pub(crate) Arc<SocketPool>);

impl Sender {
    // === Send methods ===

    /// Send data through ALL UDP sockets (main + assistant) to a specific address.
    pub fn try_send_via_all(&self, buf: &[u8], addr: SocketAddr) {
        self.0.try_send_via_all(buf, addr);
    }

    /// Send data to an address via the main UDP socket.
    pub fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<()> {
        self.0.send_to(buf, addr)
    }

    /// Send data through ALL assistant UDP sockets to a specific address.
    pub fn try_send_via_assistants(&self, buf: &[u8], addr: SocketAddr) -> io::Result<()> {
        self.0.try_send_via_assistants(buf, addr)
    }

    // === Read-only query methods ===

    /// Get local address of the main IPv4 UDP socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    /// Get the number of assistant sockets.
    pub fn assistant_count(&self) -> usize {
        self.0.assistant_count()
    }

    /// Get all UDP sockets.
    pub fn udp_sockets(&self) -> Vec<Arc<UdpSocket>> {
        self.0.udp_sockets()
    }

    /// Get a UDP socket by index.
    pub fn udp_socket(&self, index: usize) -> Option<Arc<UdpSocket>> {
        self.0.udp_socket(index)
    }

    /// Find a TCP connection by peer address.
    pub fn find_tcp(&self, addr: SocketAddr) -> Option<Arc<TcpConnection>> {
        self.0.find_tcp(addr)
    }

    /// Get all TCP connections.
    pub fn tcp_connections(&self) -> Vec<Arc<TcpConnection>> {
        self.0.tcp_connections()
    }

    // === TCP connection methods ===

    /// Establish a TCP connection to the given address.
    /// If a connection already exists, returns immediately.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.0.connect_tcp_internal(addr).await?;
        Ok(())
    }

    /// Send data to the given address via TCP.
    /// Automatically establishes a connection if none exists.
    pub async fn write_to(&self, data: &[u8], addr: SocketAddr) -> io::Result<()> {
        let conn = self.0.connect_tcp_internal(addr).await?;
        conn.send(data).await
    }
}

#[cfg(all(
    test,
    feature = "sendmmsg",
    any(target_os = "linux", target_os = "android")
))]
mod mmsg_tests {
    use super::{
        is_recoverable_udp_recv_error, recv_mmsg, send_mmsg, sockaddr_to_socket_addr,
        socket_addr_to_sockaddr, Interest, SocketAddr, UdpSocket, UDP_BATCH_SIZE,
        UDP_RECV_BUFFER_SIZE,
    };
    use bytes::Bytes;
    use std::io;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::os::fd::AsRawFd;
    use std::time::Duration;

    #[test]
    fn sockaddr_round_trip_preserves_ipv4_and_ipv6_fields() -> io::Result<()> {
        let addresses = [
            SocketAddr::from(([127, 0, 0, 1], 32123)),
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 32124, 0x12345, 7)),
        ];

        for expected in addresses {
            let (storage, len) = socket_addr_to_sockaddr(&expected);
            if let SocketAddr::V6(expected) = expected {
                let raw = unsafe {
                    &*(&storage as *const libc::sockaddr_storage as *const libc::sockaddr_in6)
                };
                assert_eq!(raw.sin6_flowinfo, expected.flowinfo());
            }
            assert_eq!(sockaddr_to_socket_addr(&storage, len)?, expected);
        }
        Ok(())
    }

    #[test]
    fn classifies_async_network_errors_as_recoverable() {
        for code in [
            libc::ECONNREFUSED,
            libc::ECONNRESET,
            libc::EHOSTUNREACH,
            libc::ENETUNREACH,
            libc::EMSGSIZE,
        ] {
            assert!(is_recoverable_udp_recv_error(
                &io::Error::from_raw_os_error(code)
            ));
        }
        assert!(!is_recoverable_udp_recv_error(
            &io::Error::from_raw_os_error(libc::EBADF)
        ));
    }

    #[tokio::test]
    async fn sendmmsg_and_recvmmsg_round_trip_batch() -> io::Result<()> {
        let receiver = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let sender = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let receiver_addr = receiver.local_addr()?;
        let sender_addr = sender.local_addr()?;
        let messages = [
            (Bytes::from_static(b"first"), receiver_addr),
            (Bytes::new(), receiver_addr),
            (Bytes::from_static(b"third"), receiver_addr),
        ];

        let mut sent = 0;
        while sent < messages.len() {
            sent += sender
                .async_io(Interest::WRITABLE, || {
                    send_mmsg(sender.as_raw_fd(), &messages[sent..])
                })
                .await?;
        }

        let received = tokio::time::timeout(Duration::from_secs(1), async {
            let mut buffers = (0..UDP_BATCH_SIZE)
                .map(|_| Vec::with_capacity(UDP_RECV_BUFFER_SIZE))
                .collect::<Vec<_>>();
            let mut peer_addrs = [SocketAddr::from(([0, 0, 0, 0], 0)); UDP_BATCH_SIZE];
            let mut received = Vec::new();

            while received.len() < messages.len() {
                let count = receiver
                    .async_io(Interest::READABLE, || {
                        recv_mmsg(receiver.as_raw_fd(), &mut buffers, &mut peer_addrs)
                    })
                    .await?;
                for index in 0..count {
                    received.push((Bytes::copy_from_slice(&buffers[index]), peer_addrs[index]));
                }
            }
            Ok::<_, io::Error>(received)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "mmsg round trip timed out"))??;

        assert_eq!(
            received,
            vec![
                (Bytes::from_static(b"first"), sender_addr),
                (Bytes::new(), sender_addr),
                (Bytes::from_static(b"third"), sender_addr),
            ]
        );
        Ok(())
    }
}
