//! Object-store transport for aetelier collectors.
//!
//! Reads market-data archives out of S3-compatible object stores over signed
//! HTTP, verifies what it retrieves, and decodes the archive container. The
//! crate provides transport and integrity only; it parses no market data and
//! writes no columnar output.
//!
//! # Modules
//!
//! - `s3` — the client. Lists a bucket prefix, retrieves objects, verifies each
//!   body against its ETag and content length, and reports per-transfer
//!   statistics. Multipart ETags are recognized and checked accordingly.
//! - `sign` — AWS Signature Version 4. Canonical request construction, URI
//!   encoding, and credential handling. Credentials are read from the
//!   environment and redacted in debug output.
//! - `source` — the `ObjectSource` abstraction over a listable, fetchable
//!   store, with a local-directory implementation for offline replay.
//! - `codec` — archive container decoding. LZ4 frame decode, capped at 256 MiB
//!   of output so a compression bomb is refused rather than allocated, and
//!   UTF-8 line splitting over the decoded bytes.
//! - `retry` — retry classification. Maps HTTP status codes and transport
//!   errors to a retry verdict, and parses `Retry-After`.
//! - `pacing` — jittered delay between attempts.
//! - `download` — file-backed fetch helper. Unmaintained: its resume check
//!   compares file size only and skips integrity re-verification, so a
//!   same-size corrupted cache file is accepted. Prefer `s3`.
//! - `error` — `EntrepotError`, the crate's single error type.
//!
//! # Entry points
//!
//! `S3Client` and `S3Config` construct and drive a transfer. `FetchedObject`
//! carries a retrieved body, `TransferStats` and `TransferSnapshot` carry the
//! counters, and `verify_integrity` is the check applied to every body.
//! `ObjectSource` and `LocalDirSource` cover the offline path.

pub mod codec;
pub mod download;
pub mod error;
pub mod pacing;
pub mod retry;
pub mod s3;
pub mod sign;
pub mod source;

pub use error::EntrepotError;
pub use s3::{
    FetchedObject, S3Client, S3Config, TransferSnapshot, TransferStats, verify_integrity,
};
pub use source::{LocalDirSource, ObjectMeta, ObjectSource};
