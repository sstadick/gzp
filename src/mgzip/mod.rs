//! Mgzip format base implementation.
//!
//! Mgzip is a multi-gzip format that adds an extra field to the header indicating how large the
//! complete block (with header and footer) is.

use std::fmt::Debug;
use std::io::Write;

use byteorder::ByteOrder;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::buf::Limit;
use bytes::BytesMut;
use flate2::{Compress, Compression, FlushCompress};

use crate::check::{Check, Crc32};
use crate::{GzpError, BUFSIZE};

pub mod par_decompress;

/// A synchronous implementation of Mgzip.
///
/// **NOTE** use [crate::deflate::Mgzip] for a parallel implementation.
/// **NOTE** this uses an internal buffer already so the passed in writer almost certainly does not
/// need to be a BufferedWriter.
#[derive(Debug)]
pub struct MgzipSyncWriter<W>
where
    W: Write,
{
    /// The internal buffer to use
    buffer: BytesMut,
    /// The size of the blocks to create
    blocksize: usize,
    /// The compressio level to use
    compression_level: Compression,
    /// The inner writer
    writer: W,
}

impl<W> MgzipSyncWriter<W>
where
    W: Write,
{
    /// Create a new [`MgzipSyncWriter`]
    pub fn new(writer: W, compression_level: Compression) -> Self {
        Self::with_capacity(writer, compression_level, BUFSIZE)
    }

    pub fn with_capacity(writer: W, compression_level: Compression, blocksize: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(BUFSIZE),
            blocksize,
            compression_level,
            writer,
        }
    }
}

/// Compress a block of bytes, adding a header and footer.
#[inline]
pub fn compress(input: &[u8], compression_level: Compression) -> Result<Vec<u8>, GzpError> {
    // The plus 64 allows odd small sized blocks to extend up to a byte boundary
    let mut buffer = Vec::with_capacity(input.len() + 64);
    let mut encoder = Compress::new(compression_level, false);

    encoder.compress_vec(input, &mut buffer, FlushCompress::Finish)?;

    let mut check = Crc32::new();
    check.update(input);

    // Add header with total byte sizes
    let mut header = header_inner(compression_level, buffer.len() as u32);
    let footer = footer_inner(&check);
    header.extend(buffer.into_iter().chain(footer));

    // Add byte footer
    Ok(header)
}

/// Create an mgzip style header
#[inline]
fn header_inner(compression_level: Compression, compressed_size: u32) -> Vec<u8> {
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
    header.write_u8(31).unwrap(); // magic byte
    header.write_u8(139).unwrap(); // magic byte
    header.write_u8(8).unwrap(); // compression method
    header.write_u8(4).unwrap(); // name / comment / extraflag
    header.write_u32::<LittleEndian>(0).unwrap(); // mtime
    header.write_u8(comp_value).unwrap(); // compression value
    header.write_u8(255).unwrap(); // OS
    header.write_u16::<LittleEndian>(8).unwrap(); // Extra flag len
    header.write_u8(b'I').unwrap(); // mgzip subfield ID 1
    header.write_u8(b'G').unwrap(); // mgzip subfield ID2
    header.write_u16::<LittleEndian>(4).unwrap(); // mgzip sufield len
    header
        .write_u32::<LittleEndian>(compressed_size + 28)
        .unwrap(); // Size of block including header and footer

    header
}

/// Create an mgzip style footer
#[inline]
fn footer_inner(check: &Crc32) -> Vec<u8> {
    let mut footer = Vec::with_capacity(8);
    footer.write_u32::<LittleEndian>(check.sum()).unwrap();
    footer.write_u32::<LittleEndian>(check.amount()).unwrap();
    footer
}

impl<W> Write for MgzipSyncWriter<W>
where
    W: Write,
{
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() > self.blocksize {
            let b = self.buffer.split_to(self.blocksize).freeze();
            let compressed = compress(&b[..], self.compression_level).unwrap();
            self.writer.write_all(&compressed)?;
        }
        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        let b = self.buffer.split_to(self.buffer.len()).freeze();
        if !b.is_empty() {
            let compressed = compress(&b[..], self.compression_level).unwrap();
            self.writer.write_all(&compressed).unwrap();
        }
        self.writer.flush()
    }
}

impl<W> Drop for MgzipSyncWriter<W>
where
    W: Write,
{
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::process::exit;
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    use flate2::bufread::MultiGzDecoder;
    use proptest::prelude::*;
    use tempfile::tempdir;

    use crate::{BUFSIZE, DICT_SIZE};

    use super::*;

    #[test]
    fn test_simple_mgzipsync() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes
        let input = b"
        This is a longer test than normal to come up with a bunch of text.
        We'll read just a few lines at a time.
        ";

        // Compress input to output
        let mut mgzip = MgzipSyncWriter::new(out_writer, Compression::new(3));
        mgzip.write_all(input).unwrap();
        mgzip.flush().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = MultiGzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }
}
