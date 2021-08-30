//! mgzip
//!

use crate::check::{Check, Crc32, PassThroughCheck};
use crate::{FormatSpec, GzpError, Pair};
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use flate2::{Compress, Compression, FlushCompress};
use std::fmt::{Debug, Formatter};

/// A synchronous implementation of Mgzip.
/// **NOTE** use [crate::deflate::Mgzip] for a parallel implementation.
#[derive(Debug, Copy, Clone)]
pub struct MgzipSync {}

impl MgzipSync {
    pub fn compress(bytes: &[u8], compression_level: Compression) -> Result<Vec<u8>, GzpError> {
        // The plus 64 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(input.len() + 64);
        let mut encoder = Compress::new(compression_level, false);

        encoder.compress_vec(input, &mut buffer, FlushCompress::Finish)?;

        let mut check = Crc32::new();
        check.update(input);

        // Add header with total byte sizes
        let mut header = header_inner(compression_level);
        let footer = footer_inner(&check);
        header.push(Pair {
            num_bytes: 4,
            value: buffer.len() + 28,
        });
        let mut header = self.to_bytes(&header);
        header.extend(buffer.into_iter().chain(footer));

        // Add byte footer
        Ok(header)
    }

    #[rustfmt::skip]
    fn header_inner(compression_level: Compression) -> Vec<Pair> {
        // Size = header + extra subfield size + filename with null terminator (if present) + datablock size (unknknown) + footer
        // const size: u32  = 16 + 4 + 0 + 0 + 8;

        let comp_value = if compression_level.level() >= Compression::best().level() {
            2
        } else if compression_level.level() <= Compression::fast().level() {
            4
        } else {
            0
        };

        let mut header = Vec::with_capacity(20);
        LittleEndian::write_u8(&mut header, 31); // magic byte
        LittleEndian::write_u8(&mut header, 139); // magic byte
        LittleEndian::write_u8(&mut header, 8); // compression method
        LittleEndian::write_u8(&mut header, 4); // name / comment / extraflag
        LittleEndian::write_u32(&mut header, 0); // mtime
        LittleEndian::write_u8(&mut headr, comp_value); // compression value
        LittleEndian::write_u8(&mut headr, 255); // OS
        LittleEndian::write_u8(&mut headr, 8); // Extra flag len
        LittleEndian::write_u8(&mut headr, b'I'); // mgzip subfield ID 1
        LittleEndian::write_u8(&mut headr, b'G'); // mgzip subfield ID2
        LittleEndian::write_u8(&mut headr, 4); // mgzip sufield len
        // The size bytes are append in the compressor function

        header
    }

    #[rustfmt::skip]
    fn footer_inner(check: &Crc32) -> Vec<u8> {
        let footer = vec![
            Pair { num_bytes: 4, value: check.sum() as usize },
            Pair { num_bytes: 4, value: check.amount() as usize },
        ];
        self.to_bytes(&footer)
    }
}
