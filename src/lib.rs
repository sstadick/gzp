//! Parallel compression.
//!
//! This module provides implementations of [`std::io::Write`] that are backed by an async
//! threadpool that compresses blocks and writes to the underlying writer. This is very similar to how
//! [`pigz`](https://zlib.net/pigz/) works.
//!
//! The supported encodings are:
//!
//! - Gzip
//! - Zlib
//! - Raw Deflate
//! - Snap Frame Encoding
//!
//! # References
//!
//! - [ParallelGzip](https://github.com/shevek/parallelgzip/blob/master/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java)
//! - [pigz](https://zlib.net/pigz/)
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{deflate::Gzip, parz::ParZ};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Gzip> = ParZ::builder(writer).build();
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::fmt::Debug;
use std::io;
use std::process::exit;

use bytes::Bytes;
use flume::{unbounded, Receiver, Sender};
use thiserror::Error;

use crate::check::Check;
use crate::parz::Compression;

pub mod check;
#[cfg(feature = "deflate")]
pub mod deflate;
pub mod parz;
#[cfg(feature = "snappy")]
pub mod snap;

/// 128 KB default buffer size, same as pigz.
pub const BUFSIZE: usize = 64 * (1 << 10) * 2;

/// 32 KB default dictionary size, same as pigz.
pub const DICT_SIZE: usize = 32768;

/// Small helper type to encapsulate that the channel that sends to the writer is sending
/// a receiver that will receive a result that is a tuple of the check value and the compressed bytes.
pub type CompressResult<C> = Result<(C, Vec<u8>), GzpError>;

#[derive(Error, Debug)]
pub enum GzpError {
    #[error("Failed to send over channel.")]
    ChannelSend,
    #[error(transparent)]
    ChannelReceive(#[from] flume::RecvError),
    #[error(transparent)]
    DeflateCompress(#[from] flate2::CompressError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    ThreadPool(#[from] rayon::ThreadPoolBuildError),
    #[error("Unknown")]
    Unknown,
}

/// A message sent from the [`ParZ`] writer to the compressor.
///
/// This message holds both the bytes to be compressed and written, as well as the oneshot channel
/// to send the compressed bytes to the writer.
#[derive(Debug)]
pub(crate) struct Message<C>
where
    C: Check + Send,
{
    buffer: Bytes,
    oneshot: Sender<CompressResult<C>>,
    dictionary: Option<Bytes>,
    is_last: bool,
}

impl<C> Message<C>
where
    C: Check + Send,
{
    /// Create a [`Message`] along with its oneshot channel.
    pub(crate) fn new_parts(
        buffer: Bytes,
        dictionary: Option<Bytes>,
    ) -> (Self, Receiver<CompressResult<C>>) {
        let (tx, rx) = unbounded();
        (
            Message {
                buffer,
                oneshot: tx,
                dictionary,
                is_last: false,
            },
            rx,
        )
    }
}

/// A [`Pair`] is used to represent header or footer information.
#[derive(Debug)]
pub struct Pair {
    /// Number of bytes to write, if negative the bytes will be written in big-endian
    num_bytes: isize,
    /// Unsigned int value to write
    value: usize,
}

/// Defines how to write the header and footer for each format.
pub trait FormatSpec: Clone + Copy + Debug + Send + Sync + 'static {
    /// The Check type for this format.
    type C: Check + Send + 'static;

    /// Create a new instance of this format spec
    fn new() -> Self;

    /// Create a check value for this format that implements [`Check`]
    #[inline]
    fn create_check() -> Self::C {
        Self::C::new()
    }

    /// Whether or not this format should try to use a dictionary.
    fn needs_dict(&self) -> bool;

    /// How to deflate bytes for this format. Returns deflated bytes.
    fn encode(
        &self,
        input: &[u8],
        compression_level: Compression,
        dict: Option<Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError>;

    /// Generate a generic header for the given format.
    fn header(&self, compression_leval: Compression) -> Vec<u8>;

    /// Generate a genric footer for the format.
    fn footer(&self, check: Self::C) -> Vec<u8>;

    /// Convert a list of [`Pair`] into bytes.
    fn to_bytes(&self, pairs: &[Pair]) -> Vec<u8> {
        // See the `put` function in pigz, which this is based on.
        let bytes_to_write = pairs
            .iter()
            .map(|p| isize::abs(p.num_bytes) as usize)
            .sum::<usize>();
        let mut buffer = Vec::with_capacity(bytes_to_write);
        for Pair { num_bytes, value } in pairs {
            let mut n = *num_bytes;
            if n < 0 {
                // big endian
                n = -n << 3;
                loop {
                    n -= 8;
                    buffer.push((value >> n) as u8);
                    // buffer.push(value.checked_shr(n as u32).unwrap_or(0) as u8);
                    if n < 0 {
                        exit(1)
                    }
                    if n == 0 {
                        break;
                    }
                }
            } else {
                // little endian
                let mut counter = 0;
                loop {
                    buffer.push((value >> counter) as u8);
                    counter += 8;
                    if counter == num_bytes * 8 {
                        break;
                    }
                }
            }
        }
        buffer
    }
}
