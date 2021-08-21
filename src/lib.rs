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
use std::process::exit;

use bytes::{Bytes, BytesMut};
use flate2::{Crc, GzBuilder};
use flume::{unbounded, Receiver, Sender};
use libz_sys::{uInt, uLong, z_off_t};
use thiserror::Error;

use crate::pargz::Compression;
use crate::Format::Zlib;

#[cfg(feature = "pargz")]
pub mod pargz;
#[cfg(feature = "parsnap")]
pub mod parsnap;

/// 128 KB default buffer size, same as pigz
pub(crate) const BUFSIZE: usize = 64 * (1 << 10) * 2;
pub(crate) const DICT_SIZE: usize = 32768;

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
    buffer: Bytes,
    oneshot: Sender<Result<(Crc, Vec<u8>), GzpError>>,
    dictionary: Option<Bytes>,
    is_last: bool,
}

impl Message {
    /// Create a [`Message`] along with its oneshot channel.
    pub(crate) fn new_parts(
        buffer: Bytes,
        dictionary: Option<Bytes>,
    ) -> (Self, Receiver<Result<(Crc, Vec<u8>), GzpError>>) {
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

#[derive(Debug, Copy, Clone)]
pub enum Format {
    Gzip,
    Zlib,
}

#[derive(Debug)]
struct Pair {
    /// Number of bytes to write
    num_bytes: isize,
    /// Unsigned int value to write
    value: usize,
}

/// Defines how to write the header and footer for each [`Format`].
trait FormatSpec {
    fn header(&self, compression_leval: Compression) -> Vec<u8>;
    fn footer(&self, check: Crc) -> Vec<u8>;
    fn to_bytes(&self, pairs: &[Pair]) -> Vec<u8> {
        let bytes_to_write = pairs
            .iter()
            .map(|p| isize::abs(p.num_bytes) as usize)
            .sum::<usize>();
        let mut buffer = Vec::with_capacity(bytes_to_write);
        for Pair { num_bytes, value } in pairs {
            let mut n = *num_bytes;
            if n < 0 {
                // big endian
                dbg!(n);
                n = dbg!(-n << 3);
                loop {
                    dbg!(n);
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

impl FormatSpec for Format {
    #[rustfmt::skip]
    fn header(&self, compression_level: Compression) -> Vec<u8> {
        let header = match self {
            Format::Gzip => {
                let comp_value = if compression_level.level() >= Compression::best().level() {
                    2
                } else if compression_level.level() <= Compression::fast().level() {
                    4
                } else {
                    0
                };
                vec![
                    Pair { num_bytes: 1, value: 31 }, // 0x1f in flate2
                    Pair { num_bytes: 1, value: 139 }, // 0x8b in flate2
                    Pair { num_bytes: 1, value: 8 }, // deflate
                    Pair { num_bytes: 1, value: 0 }, // name / comment
                    Pair { num_bytes: 4, value: 0 }, // mtime
                    Pair { num_bytes: 1, value: comp_value }, // Compression level
                    Pair { num_bytes: 1, value: 255 }, // OS
                ]
            }
            Format::Zlib => {
                let comp_level = compression_level.level();
                let comp_value = if comp_level >= 9 {
                    3 << 6
                } else if comp_level == 1 {
                    0 << 6
                } else if comp_level >= 6 {
                    1 << 6
                } else {
                    2 << 6
                };

                let mut head = (0x78 << 8) + // defalte, 32k window
                    comp_value; // compression level clue
                head += 31 - (head % 31); // make it multiple of 31
                vec![
                    Pair { num_bytes: -2, value: head } // zlib uses big-endian
                ]
            }
            _ => unimplemented!(),
        };
        self.to_bytes(&header)
    }

    #[rustfmt::skip]
    fn footer(&self, check: Crc) -> Vec<u8> {
        let footer = match self {
            Format::Gzip => {
                vec![
                    Pair { num_bytes: 4, value: check.sum() as usize },
                    Pair { num_bytes: 4, value: check.amount() as usize },
                ]
            }
            Format::Zlib => {
                vec![
                    Pair { num_bytes: -4, value: check.sum() as usize },
                ]
            }
        };
        self.to_bytes(&footer)
    }
}

// trait Check {
//     /// Current checksum
//     fn sum(&self) -> u32;
//     /// Amount input to the check
//     fn amount(&self) -> u32;
//     fn new() -> Self
//     where
//         Self: Sized;
//     fn update(&mut self, bytes: &[u8]);
//     fn combine(&mut self, other: &Self)
//     where
//         Self: Sized;
// }
//
// struct Adler32 {
//     sum: u32,
//     amount: u32,
// }
//
// impl Check for Adler32 {
//     fn sum(&self) -> u32 {
//         self.sum
//     }
//
//     fn amount(&self) -> u32 {
//         self.amount
//     }
//
//     fn new() -> Self {
//         Self { sum: 0, amount: 0 }
//     }
//
//     fn update(&mut self, bytes: &[u8]) {
//         // TODO: safer cast(s)?
//         self.amount += bytes.len() as u32;
//         self.sum = unsafe {
//             libz_sys::adler32(
//                 self.sum as uLong,
//                 bytes.as_ptr() as *mut _,
//                 bytes.len() as uInt,
//             )
//         } as u32;
//     }
//
//     fn combine(&mut self, other: &Self) {
//         self.sum = unsafe {
//             libz_sys::adler32_combine(
//                 self.sum as uLong,
//                 other.sum as uLong,
//                 other.amount as z_off_t,
//             )
//         } as u32;
//         self.amount += other.amount;
//     }
// }
//
// struct Crc32 {
//     crc: flate2::Crc,
// }
//
// impl Check for Crc32 {
//     fn sum(&self) -> u32 {
//         self.crc.sum()
//     }
//
//     fn amount(&self) -> u32 {
//         self.crc.amount()
//     }
//
//     fn new() -> Self {
//         let crc = flate2::Crc::new();
//         Self { crc }
//     }
//
//     fn update(&mut self, bytes: &[u8]) {
//         self.crc.update(bytes);
//     }
//
//     fn combine(&mut self, other: &Self) {
//         self.crc.combine(&other.crc);
//     }
// }

// pub static FHCRC: u8 = 1 << 1;
// pub static FEXTRA: u8 = 1 << 2;
// pub static FNAME: u8 = 1 << 3;
// pub static FCOMMENT: u8 = 1 << 4;
//
// // https://github.com/rust-lang/flate2-rs/blob/33f9f3d028848760207bb3f6618669bf5ef02c3d/src/gz/mod.rs#L197
// fn generic_gzip_header(lvl: Compression) -> Vec<u8> {
//     let extra = None::<Vec<u8>>;
//     let filename = None::<CString>;
//     let comment = None::<CString>;
//     let operating_system = None::<u8>;
//     let mtime = 0;
//     let mut flg = 0;
//     let mut header = vec![0u8; 10];
//     match extra {
//         Some(v) => {
//             flg |= FEXTRA;
//             header.push((v.len() >> 0) as u8);
//             header.push((v.len() >> 8) as u8);
//             header.extend(v);
//         }
//         None => {}
//     }
//     match filename {
//         Some(filename) => {
//             flg |= FNAME;
//             header.extend(filename.as_bytes_with_nul().iter().map(|x| *x));
//         }
//         None => {}
//     }
//     match comment {
//         Some(comment) => {
//             flg |= FCOMMENT;
//             header.extend(comment.as_bytes_with_nul().iter().map(|x| *x));
//         }
//         None => {}
//     }
//     header[0] = 0x1f;
//     header[1] = 0x8b;
//     header[2] = 8;
//     header[3] = flg;
//     header[4] = (mtime >> 0) as u8;
//     header[5] = (mtime >> 8) as u8;
//     header[6] = (mtime >> 16) as u8;
//     header[7] = (mtime >> 24) as u8;
//     header[8] = if lvl.level() >= Compression::best().level() {
//         2
//     } else if lvl.level() <= Compression::fast().level() {
//         4
//     } else {
//         0
//     };
//
//     // Typically this byte indicates what OS the gz stream was created on,
//     // but in an effort to have cross-platform reproducible streams just
//     // default this value to 255. I'm not sure that if we "correctly" set
//     // this it'd do anything anyway...
//     header[9] = operating_system.unwrap_or(255);
//     return header;
// }
//
// fn gzip_footer(crc: Crc, mut buffer: Vec<u8>) -> Vec<u8> {
//     let (sum, amt) = (crc.sum() as u32, crc.amount());
//     let buf = [
//         (sum >> 0) as u8,
//         (sum >> 8) as u8,
//         (sum >> 16) as u8,
//         (sum >> 24) as u8,
//         (amt >> 0) as u8,
//         (amt >> 8) as u8,
//         (amt >> 16) as u8,
//         (amt >> 24) as u8,
//     ];
//     buffer.write_all(&buf[..]).unwrap();
//     buffer
// }
