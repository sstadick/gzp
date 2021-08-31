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
//! A typical parallel compression task:
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{deflate::Gzip, parz::{ParZ, ParZBuilder}, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Gzip> = ParZBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
//!
//! A typical single_threaded task:
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{deflate::Gzip, syncz::SyncZBuilder, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz = SyncZBuilder::<Gzip, _>::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
//!
//! If the number of threads might be 0, the following provides a uniform
//! api:
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{deflate::Gzip, ZBuilder, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz = ZBuilder::<Gzip, _>::new()
//!     .num_threads(0)
//!     .from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::fmt::Debug;
use std::io::{self, Write};
use std::marker::PhantomData;

use bytes::Bytes;
use flume::{unbounded, Receiver, Sender};
use thiserror::Error;

use crate::check::Check;
use crate::parz::{Compression, ParZBuilder};
use crate::syncz::{SyncZ, SyncZBuilder};

pub mod check;
#[cfg(feature = "deflate")]
pub mod deflate;
mod mgzip;
pub mod parz;
#[cfg(feature = "snappy")]
pub mod snap;
pub mod syncz;

/// 128 KB default buffer size, same as pigz.
pub const BUFSIZE: usize = 64 * (1 << 10) * 2;

/// 32 KB default dictionary size, same as pigz.
pub const DICT_SIZE: usize = 32768;

/// Small helper type to encapsulate that the channel that sends to the writer is sending
/// a receiver that will receive a result that is a tuple of the check value and the compressed bytes.
pub type CompressResult<C> = Result<(C, Vec<u8>), GzpError>;

#[derive(Error, Debug)]
pub enum GzpError {
    #[error("Invalid buffer size ({0}), must be >= {1}")]
    BufferSize(usize, usize),

    #[error("Failed to send over channel.")]
    ChannelSend,

    #[error(transparent)]
    ChannelReceive(#[from] flume::RecvError),

    #[error(transparent)]
    DeflateCompress(#[from] flate2::CompressError),

    #[error("Invalid block size: {0}")]
    InvalidBlockSize(&'static str),

    #[error("Invalid block header: {0}")]
    InvalidHeader(&'static str),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("Invalid number of threads ({0}) selected.")]
    NumThreads(usize),

    #[error("Unknown")]
    Unknown,
}

/// Trait that unifies sync and async writer
pub trait ZWriter: Write {
    /// Cleans up resources, writes footers
    fn finish(&mut self) -> Result<(), GzpError>;
}

/// Create a synchronous writer wrapping the input `W` type.
pub trait SyncWriter<W: Write> {
    // type InputWriter: Write;
    type OutputWriter: Write;

    fn sync_writer(writer: W, compression_level: Compression) -> Self::OutputWriter;
}

/// Unified builder that returns a trait object
pub struct ZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write + Send + 'static,
{
    num_threads: usize,
    compression_level: Compression,
    writer: PhantomData<W>,
    format: PhantomData<F>,
}

impl<F, W> ZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            num_threads: num_cpus::get(),
            compression_level: Compression::new(3),
            writer: PhantomData,
            format: PhantomData,
        }
    }

    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Number of threads to use for compression
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Create a [`ZWriter`] trait object from a writer.
    #[allow(clippy::missing_panics_doc)]
    pub fn from_writer(self, writer: W) -> Box<dyn ZWriter>
    where
        SyncZ<<F as SyncWriter<W>>::OutputWriter>: ZWriter,
    {
        if self.num_threads > 1 {
            Box::new(
                ParZBuilder::<F>::new()
                    .compression_level(self.compression_level)
                    .num_threads(self.num_threads)
                    .unwrap()
                    .from_writer(writer),
            )
        } else {
            Box::new(
                SyncZBuilder::<F, W>::new()
                    .compression_level(self.compression_level)
                    .from_writer(writer),
            )
        }
    }
}

impl<F, W> Default for ZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
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

struct FooterValues {
    /// The check sum
    sum: u32,
    /// The number of bytes that went into the sum
    amount: u32,
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

    /// Check that the header is expected for this format
    #[inline]
    fn check_header(&self, _bytes: &[u8]) -> Result<(), GzpError> {
        Err(GzpError::InvalidHeader("Not Implemented?"))
    }

    /// Check that the header is expected for this format
    #[inline]
    fn get_block_size(&self, _bytes: &[u8]) -> Result<usize, GzpError> {
        Err(GzpError::InvalidBlockSize("Not Implemented?"))
    }

    /// Get the check value and check sum from the footer
    #[inline]
    fn get_footer_values(&self, input: &[u8]) -> FooterValues {
        let check_sum = LittleEndian::read_u32(&input[input.len() - 8..input.len() - 4]);
        let check_amount = LittleEndian::read_u32(&input[input.len() - 4..]);
        FooterValues {
            sum: check_sum,
            amount: check_amount,
        }
    }

    /// How to deflate bytes for this format. Returns deflated bytes.
    fn encode(
        &self,
        input: &[u8],
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError>;

    /// Generate a generic header for the given format.
    fn header(&self, compression_leval: Compression) -> Vec<u8>;

    /// Generate a genric footer for the format.
    fn footer(&self, check: &Self::C) -> Vec<u8>;

    /// Convert a list of [`Pair`] into bytes.
    fn to_bytes(&self, pairs: &[Pair]) -> Vec<u8> {
        // TODO: remove this in favor of byteorder
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
