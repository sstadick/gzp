//! mgzip

use crate::check::{Check, Crc32, PassThroughCheck};
use crate::deflate::Mgzip;
use crate::{FormatSpec, GzpError, Pair, BUFSIZE};
use byteorder::ByteOrder;
use byteorder::{LittleEndian, WriteBytesExt};
use bytes::{Bytes, BytesMut};
use flate2::{Compress, Compression, FlushCompress};
use std::fmt::{Debug, Formatter};
use std::io::Write;

/// A synchronous implementation of Mgzip.
/// **NOTE** use [crate::deflate::Mgzip] for a parallel implementation.
#[derive(Debug)]
pub struct MgzipSync<W> {
    buffer: BytesMut,
    blocksize: usize,
    compression_level: Compression,
    writer: W,
}

impl<W> MgzipSync<W>
where
    W: Write,
{
    pub fn new(writer: W, blocksize: usize, compression_level: Compression) -> Self {
        Self {
            buffer: BytesMut::with_capacity(BUFSIZE),
            blocksize,
            compression_level,
            writer,
        }
    }
    pub fn compress(
        &self,
        bytes: &[u8],
        compression_level: Compression,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 64 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(input.len() + 64);
        let mut encoder = Compress::new(compression_level, false);

        encoder.compress_vec(input, &mut buffer, FlushCompress::Finish)?;

        let mut check = Crc32::new();
        check.update(input);

        // Add header with total byte sizes
        let mut header = self.header_inner(compression_level, buffer.len() as u32);
        let footer = self.footer_inner(&check);
        header.extend(buffer.into_iter().chain(footer));

        // Add byte footer
        Ok(header)
    }

    #[inline]
    fn header_inner(&self, compression_level: Compression, compressed_size: u32) -> Vec<u8> {
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
        header.write_u8(31); // magic byte
        header.write_u8(139); // magic byte
        LittleEndian::write_u8(&mut header, 8); // compression method
        LittleEndian::write_u8(&mut header, 4); // name / comment / extraflag
        LittleEndian::write_u32(&mut header, 0); // mtime
        LittleEndian::write_u8(&mut headr, comp_value); // compression value
        LittleEndian::write_u8(&mut headr, 255); // OS
        LittleEndian::write_u8(&mut headr, 8); // Extra flag len
        LittleEndian::write_u8(&mut headr, b'I'); // mgzip subfield ID 1
        LittleEndian::write_u8(&mut headr, b'G'); // mgzip subfield ID2
        LittleEndian::write_u8(&mut headr, 4); // mgzip sufield len
        LittleEndian::write_u32(&mut header, compressed_size + 28); // Size of block including header and footer

        header
    }

    #[rustfmt::skip]
    #[inline]
    fn footer_inner(&self, check: &Crc32) -> Vec<u8> {
        let mut footer = Vec::with_capacity(8);
        LittleEndian::write_u32(&mut footer, check.sum());
        LittleEndian::write_u32(&mut footer, check.amount());
        footer
    }
}

impl<W> Write for MgzipSync<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() > self.blocksize {
            let b = self.buffer.split_to(self.blocksize).freeze();
            let compressed = self.compress(&b[..], self.compression_level).unwrap();
            self.writer.write_all(&compressed)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let b = self.buffer.split_to(self.buffer.len()).freeze();
        let compressed = self.compress(&b[..], self.compression_level).unwrap();
        self.writer.write_all(&compressed);
        self.writer.flush()
    }
}

impl<W> Drop for MgzipSync<W>
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
        let mut mgzip = MgzipSync::new(out_writer, BUFSIZE, Compression::new(3));
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
