//! # Tunnel API - P2P Networking Interface
//!
//! This module provides the primary API for P2P networking.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use bytes::Bytes;
//! use rustp2p_core::endpoint::{Config, TunnelIncoming};
//!
//! # #[tokio::main]
//! # async fn main() -> std::io::Result<()> {
//! let mut incoming = TunnelIncoming::bind(Config::new().udp_port(3000)).await?;
//!
//! while let Some(mut tunnel) = incoming.next().await {
//!     tokio::spawn(async move {
//!         while let Some(data) = tunnel.recv().await {
//!             println!("From {}: {:?}", tunnel.remote_addr(), data);
//!             tunnel.send(Bytes::from_static(b"echo")).await?;
//!         }
//!         Ok::<_, std::io::Error>(())
//!     });
//! }
//! # Ok(())
//! # }
//! ```

mod codec;
mod config;
pub(crate) mod pool;
mod service;
pub(crate) mod tunnel;

pub use crate::route_table::Protocol;
pub use codec::{BytesInitCodec, Decoder, Encoder, InitCodec, LengthPrefixedInitCodec};
pub use config::{Config, LoadBalance};
pub use service::TunnelIncoming;
pub use tunnel::{Tunnel, TunnelReadHalf, TunnelWriteHalf};
