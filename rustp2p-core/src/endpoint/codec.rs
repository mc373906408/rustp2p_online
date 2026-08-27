use std::io::{self, IoSlice};
use std::net::SocketAddr;

use bytes::{Buf, Bytes, BytesMut};
use dyn_clone::DynClone;

/// Maximum payload size of a single length-prefixed frame.
const MAX_FRAME_LEN: usize = 64 * 1024;

/// Factory for creating codec pairs.
pub trait InitCodec: Send + Sync + DynClone {
    fn codec(&self, addr: SocketAddr) -> io::Result<(Box<dyn Decoder>, Box<dyn Encoder>)>;
}
dyn_clone::clone_trait_object!(InitCodec);

/// Decoder for reading framed data.
///
/// The read task appends bytes received from the stream to `buf` and calls
/// `decode` in a loop until it returns `Ok(None)`:
/// - `Ok(Some(frame))`: one complete frame was consumed from the front of
///   `buf`. The returned [`Bytes`] is zero-copy, it shares `buf`'s allocation.
/// - `Ok(None)`: more bytes are needed; unconsumed bytes stay in `buf`.
/// - `Err(_)`: unrecoverable framing error, the connection is closed.
pub trait Decoder: Send {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Bytes>>;
}

/// Encoder for writing framed data.
///
/// Implementations push the framed representation of `data` onto `iov` as
/// scatter-gather slices, which the write task sends with a single vectored
/// write. Slices may borrow from `data` directly or from `self` (e.g. an
/// internal header buffer), so the payload is never copied.
pub trait Encoder: Send {
    fn encode<'a>(&'a mut self, data: &'a [u8], iov: &mut Vec<IoSlice<'a>>) -> io::Result<()>;
}

/// Raw bytes codec (no framing).
#[derive(Clone, Default)]
pub struct BytesCodec;

impl Decoder for BytesCodec {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
        if buf.is_empty() {
            return Ok(None);
        }
        Ok(Some(buf.split().freeze()))
    }
}

impl Encoder for BytesCodec {
    fn encode<'a>(&'a mut self, data: &'a [u8], iov: &mut Vec<IoSlice<'a>>) -> io::Result<()> {
        iov.push(IoSlice::new(data));
        Ok(())
    }
}

/// InitCodec for raw bytes (no framing).
#[derive(Clone)]
pub struct BytesInitCodec;

impl InitCodec for BytesInitCodec {
    fn codec(&self, _addr: SocketAddr) -> io::Result<(Box<dyn Decoder>, Box<dyn Encoder>)> {
        Ok((Box::new(BytesCodec), Box::new(BytesCodec)))
    }
}

/// Length-prefixed codec (4-byte big-endian length prefix).
#[derive(Clone, Default)]
pub struct LengthPrefixedCodec {
    head: [u8; 4],
}

impl Decoder for LengthPrefixedCodec {
    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {len}"),
            ));
        }
        if buf.len() < 4 + len {
            // Pre-reserve so the rest of the frame is read with few syscalls.
            buf.reserve(4 + len - buf.len());
            return Ok(None);
        }
        buf.advance(4);
        Ok(Some(buf.split_to(len).freeze()))
    }
}

impl Encoder for LengthPrefixedCodec {
    fn encode<'a>(&'a mut self, data: &'a [u8], iov: &mut Vec<IoSlice<'a>>) -> io::Result<()> {
        // Enforce the same MAX_FRAME_LEN limit as the decoder: sending a
        // larger frame would make the peer close the connection with no
        // error reported on this side.
        if data.len() > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("frame too large: {}", data.len()),
            ));
        }
        let len = u32::try_from(data.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
        self.head = len.to_be_bytes();
        iov.push(IoSlice::new(&self.head));
        iov.push(IoSlice::new(data));
        Ok(())
    }
}

/// InitCodec for length-prefixed framing.
#[derive(Clone)]
pub struct LengthPrefixedInitCodec;

impl InitCodec for LengthPrefixedInitCodec {
    fn codec(&self, _addr: SocketAddr) -> io::Result<(Box<dyn Decoder>, Box<dyn Encoder>)> {
        Ok((
            Box::new(LengthPrefixedCodec::default()),
            Box::new(LengthPrefixedCodec::default()),
        ))
    }
}
