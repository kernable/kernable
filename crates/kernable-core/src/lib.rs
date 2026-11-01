//! Core types shared by the kernel and its tools.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

/// Generated protocol types and gRPC stubs (`proto/kernable/v1`).
#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("kernable.v1");
}

#[cfg(feature = "certs")]
pub mod certs;

pub mod signing;

pub mod transport;

/// The protocol version this build speaks; see "Protocol versions" in `kernable.proto`.
pub const PROTOCOL: u32 = 2;
