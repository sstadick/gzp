//! Parallel compression.
//!
//! This module provides a implementations of [`Write`] that are backed by an async threadpool that
//! compresses blocks and writes to the underlying writer. This is very similar to how
//! [`pigz`](https://zlib.net/pigz/) works.
//!
//! The supported encodings are:
//!
//! - Gzip: [`pgz::pargz`]
//! - Snap: [`pgz::parsnap`]
//!
//! # References
//!
//! - [ParallelGzip](https://github.com/shevek/parallelgzip/blob/master/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java)
//! - [pigz](https://zlib.net/pigz/)
//!
//! # Known Differences from Pigz
//!
//! - Each block has an independent CRC value
//! - There is no continual dictionary for compression, compression is per-block only. On some data
//!   types this could lead to no compression for a given block if the block is small enough or the
//!   data is random enough.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "pargz")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::pargz::ParGz;
//!
//! let mut writer = vec![];
//! let mut par_gz = ParGz::builder(writer).build();
//! par_gz.write_all(b"This is a first test line\n").unwrap();
//! par_gz.write_all(b"This is a second test line\n").unwrap();
//! par_gz.finish().unwrap();
//! # }
//! ```
use std::ffi::CString;
use std::io::{self, Write};

use bytes::BytesMut;
use flate2::{Crc, GzBuilder};
use flume::{unbounded, Receiver, Sender};
use thiserror::Error;

use crate::pargz::Compression;

#[cfg(feature = "pargz")]
pub mod pargz;
#[cfg(feature = "parsnap")]
pub mod parsnap;

/// 128 KB default buffer size, same as pigz
pub(crate) const BUFSIZE: usize = 64 * (1 << 10) * 2;

#[derive(Error, Debug)]
pub enum GzpError {
    #[error("Failed to send over channel.")]
    ChannelSend,
    #[error(transparent)]
    ChannelReceive(#[from] flume::RecvError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Unknown")]
    Unknown,
}

/// A message sent from the Par writer to the compressor.
///
/// This message holds both the bytes to be compressed and written, as well as the oneshot channel
/// to send the compressed bytes to the writer.
#[derive(Debug)]
pub(crate) struct Message {
    buffer: BytesMut,
    oneshot: Sender<Result<(Crc, Vec<u8>), GzpError>>,
}

impl Message {
    /// Create a [`Message`] along with its oneshot channel.
    pub(crate) fn new_parts(
        buffer: BytesMut,
    ) -> (Self, Receiver<Result<(Crc, Vec<u8>), GzpError>>) {
        let (tx, rx) = unbounded();
        (
            Message {
                buffer,
                oneshot: tx,
            },
            rx,
        )
    }
}

pub static FHCRC: u8 = 1 << 1;
pub static FEXTRA: u8 = 1 << 2;
pub static FNAME: u8 = 1 << 3;
pub static FCOMMENT: u8 = 1 << 4;

// https://github.com/rust-lang/flate2-rs/blob/33f9f3d028848760207bb3f6618669bf5ef02c3d/src/gz/mod.rs#L197
fn generic_gzip_header(lvl: Compression) -> Vec<u8> {
    let extra = None::<Vec<u8>>;
    let filename = None::<CString>;
    let comment = None::<CString>;
    let operating_system = None::<u8>;
    let mtime = 0;
    let mut flg = 0;
    let mut header = vec![0u8; 10];
    match extra {
        Some(v) => {
            flg |= FEXTRA;
            header.push((v.len() >> 0) as u8);
            header.push((v.len() >> 8) as u8);
            header.extend(v);
        }
        None => {}
    }
    match filename {
        Some(filename) => {
            flg |= FNAME;
            header.extend(filename.as_bytes_with_nul().iter().map(|x| *x));
        }
        None => {}
    }
    match comment {
        Some(comment) => {
            flg |= FCOMMENT;
            header.extend(comment.as_bytes_with_nul().iter().map(|x| *x));
        }
        None => {}
    }
    header[0] = 0x1f;
    header[1] = 0x8b;
    header[2] = 8;
    header[3] = flg;
    header[4] = (mtime >> 0) as u8;
    header[5] = (mtime >> 8) as u8;
    header[6] = (mtime >> 16) as u8;
    header[7] = (mtime >> 24) as u8;
    header[8] = if lvl.level() >= Compression::best().level() {
        2
    } else if lvl.level() <= Compression::fast().level() {
        4
    } else {
        0
    };

    // Typically this byte indicates what OS the gz stream was created on,
    // but in an effort to have cross-platform reproducible streams just
    // default this value to 255. I'm not sure that if we "correctly" set
    // this it'd do anything anyway...
    header[9] = operating_system.unwrap_or(255);
    return header;
}

fn gzip_footer(crc: Crc, mut buffer: Vec<u8>) -> Vec<u8> {
    let mut crc_bytes_written = 0;
    while crc_bytes_written < 8 {
        let (sum, amt) = (crc.sum() as u32, crc.amount());
        let buf = [
            (sum >> 0) as u8,
            (sum >> 8) as u8,
            (sum >> 16) as u8,
            (sum >> 24) as u8,
            (amt >> 0) as u8,
            (amt >> 8) as u8,
            (amt >> 16) as u8,
            (amt >> 24) as u8,
        ];
        let n = buffer.write(&buf[crc_bytes_written..]).unwrap();
        crc_bytes_written += n;
    }

    buffer
}
