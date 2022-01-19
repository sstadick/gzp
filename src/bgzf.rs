//! Bgzf format base implementation.
//!
//! Bgzf is a multi-gzip format that adds an extra field to the header indicating how large the
//! complete block (with header and footer) is.

use std::io::Write;
use std::io::{self, Read};

use byteorder::{LittleEndian, WriteBytesExt};
use bytes::{Buf, BytesMut};
use flate2::Compression;
#[cfg(not(feature = "libdeflate"))]
use flate2::{Compress, Decompress, FlushCompress};

#[cfg(not(feature = "libdeflate"))]
use crate::check::Check;
use crate::deflate::Bgzf;
use crate::{BlockFormatSpec, FooterValues, GzpError, BUFSIZE};

pub(crate) const BGZF_BLOCK_SIZE: usize = 65280;
// default from bgzf, compress(BGZF_BLOCK_SIZE) < BGZF_MAX_BLOCK_SIZE
pub(crate) const MAX_BGZF_BLOCK_SIZE: usize = 64 * 1024;
// 65536 which is u16::MAX + 1
pub(crate) static BGZF_EOF: &[u8] = &[
    0x1f, 0x8b, // ID1, ID2
    0x08, // CM = DEFLATE
    0x04, // FLG = FEXTRA
    0x00, 0x00, 0x00, 0x00, // MTIME = 0
    0x00, // XFL = 0
    0xff, // OS = 255 (unknown)
    0x06, 0x00, // XLEN = 6
    0x42, 0x43, // SI1, SI2
    0x02, 0x00, // SLEN = 2
    0x1b, 0x00, // BSIZE = 27
    0x03, 0x00, // CDATA
    0x00, 0x00, 0x00, 0x00, // CRC32 = 0x00000000
    0x00, 0x00, 0x00, 0x00, // ISIZE = 0
];
#[cfg(feature = "libdeflate")]
pub(crate) const BGZF_HEADER_SIZE: usize = 18;
#[cfg(feature = "libdeflate")]
pub(crate) const BGZF_FOOTER_SIZE: usize = 8;

const EXTRA: f64 = 0.1;

#[inline]
fn extra_amount(input_len: usize) -> usize {
    std::cmp::max(128, (input_len as f64 * EXTRA) as usize)
}

/// A sync implementation of a Bgzf reader
pub struct BgzfSyncReader<R>
where
    R: Read,
{
    buffer: BytesMut,
    compressed_buffer: BytesMut,
    #[cfg(feature = "libdeflate")]
    decompressor: libdeflater::Decompressor,
    #[cfg(not(feature = "libdeflate"))]
    decompressor: Decompress,
    reader: R,
    format: Bgzf,
}

impl<R> BgzfSyncReader<R>
where
    R: Read,
{
    pub fn new(reader: R) -> Self {
        #[cfg(feature = "libdeflate")]
        let decompressor = libdeflater::Decompressor::new();

        #[cfg(not(feature = "libdeflate"))]
        let decompressor = Decompress::new(false);

        Self {
            buffer: BytesMut::with_capacity(BUFSIZE),
            compressed_buffer: BytesMut::with_capacity(BGZF_BLOCK_SIZE),
            decompressor,
            reader,
            format: Bgzf {},
        }
    }
}

/// A synchronous implementation of Bgzf.
///
/// **NOTE** use [crate::deflate::Bgzf] for a parallel implementation.
/// **NOTE** this uses an internal buffer already so the passed in writer almost certainly does not
/// need to be a BufferedWriter.
pub struct BgzfSyncWriter<W>
where
    W: Write,
{
    /// The internal buffer to use
    buffer: BytesMut,
    /// The size of the blocks to create
    blocksize: usize,
    /// The compressio level to use
    compression_level: Compression,
    /// The compressor to reuse
    #[cfg(feature = "libdeflate")]
    compressor: libdeflater::Compressor,
    #[cfg(not(feature = "libdeflate"))]
    compressor: Compress,
    /// The inner writer
    writer: W,
}

impl<W> BgzfSyncWriter<W>
where
    W: Write,
{
    /// Create a new [`BgzfSyncWriter`]
    pub fn new(writer: W, compression_level: Compression) -> Self {
        Self::with_capacity(writer, compression_level, BGZF_BLOCK_SIZE)
    }

    pub fn with_capacity(writer: W, compression_level: Compression, blocksize: usize) -> Self {
        assert!(blocksize <= BGZF_BLOCK_SIZE);
        #[cfg(feature = "libdeflate")]
        let compressor = libdeflater::Compressor::new(
            libdeflater::CompressionLvl::new(compression_level.level() as i32).unwrap(),
        );
        #[cfg(not(feature = "libdeflate"))]
        let compressor = Compress::new(compression_level, false);
        Self {
            buffer: BytesMut::with_capacity(BUFSIZE),
            blocksize,
            compression_level,
            compressor,
            writer,
        }
    }
}

/// Decompress a block of bytes
#[cfg(feature = "libdeflate")]
#[inline]
pub fn decompress(
    input: &[u8],
    decoder: &mut libdeflater::Decompressor,
    output: &mut [u8],
    footer_vals: FooterValues,
) -> Result<(), GzpError> {
    if footer_vals.amount != 0 {
        let _bytes_decompressed = decoder.deflate_decompress(&input[..input.len() - 8], output)?;
    }
    let mut new_check = libdeflater::Crc::new();
    new_check.update(output);

    if footer_vals.sum != new_check.sum() {
        return Err(GzpError::InvalidCheck {
            found: new_check.sum(),
            expected: footer_vals.sum,
        });
    }
    Ok(())
}

/// Decompress a block of bytes
#[cfg(not(feature = "libdeflate"))]
#[inline]
pub fn decompress(
    input: &[u8],
    decoder: &mut Decompress,
    output: &mut [u8],
    footer_vals: FooterValues,
) -> Result<(), GzpError> {
    use flate2::Crc;

    if footer_vals.amount != 0 {
        let _bytes_decompressed = decoder.decompress(
            &input[..input.len() - 8],
            output,
            flate2::FlushDecompress::Finish,
        )?;
        decoder.reset(false);
    }
    let mut new_check = flate2::Crc::new();
    new_check.update(output);

    if footer_vals.sum != new_check.sum() {
        return Err(GzpError::InvalidCheck {
            found: new_check.sum(),
            expected: footer_vals.sum,
        });
    }
    Ok(())
}

/// Compress a block of bytes, adding a header and footer.
#[cfg(feature = "libdeflate")]
#[inline]
pub fn compress(
    input: &[u8],
    encoder: &mut libdeflater::Compressor,
    compression_level: Compression,
) -> Result<Vec<u8>, GzpError> {
    // The plus 64 allows odd small sized blocks to extend up to a byte boundary
    // let mut buffer = Vec::with_capacity(input.len() + 64);
    let mut buffer =
        vec![0; BGZF_HEADER_SIZE + input.len() + extra_amount(input.len()) + BGZF_FOOTER_SIZE];

    let bytes_written = encoder
        .deflate_compress(input, &mut buffer[BGZF_HEADER_SIZE..])
        .map_err(GzpError::LibDeflaterCompress)?;
    // Make sure that compressed buffer is smaller than
    if bytes_written >= MAX_BGZF_BLOCK_SIZE {
        return Err(GzpError::BlockSizeExceeded(
            bytes_written,
            MAX_BGZF_BLOCK_SIZE,
        ));
    }
    let mut check = libdeflater::Crc::new();
    check.update(input);

    // Add header with total byte sizes
    let header = header_inner(compression_level, bytes_written as u16);
    buffer[0..BGZF_HEADER_SIZE].copy_from_slice(&header);
    buffer.truncate(BGZF_HEADER_SIZE + bytes_written);

    // let mut footer = Vec::with_capacity(8);
    buffer.write_u32::<LittleEndian>(check.sum())?;
    buffer.write_u32::<LittleEndian>(input.len() as u32)?;

    Ok(buffer)
}

#[cfg(not(feature = "libdeflate"))]
/// Compress a block of bytes, adding a header and footer.
#[inline]
pub fn compress(
    input: &[u8],
    encoder: &mut Compress,
    compression_level: Compression,
) -> Result<Vec<u8>, GzpError> {
    {
        // The plus 64 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(input.len() + extra_amount(input.len()));
        // let mut encoder = Compress::new(compression_level, false);
        encoder.compress_vec(input, &mut buffer, FlushCompress::Finish)?;

        // Make sure that compressed buffer is smaller than
        if !(buffer.len() < MAX_BGZF_BLOCK_SIZE) {
            return Err(GzpError::BlockSizeExceeded(
                buffer.len(),
                MAX_BGZF_BLOCK_SIZE,
            ));
        }
        let mut check = crate::check::Crc32::new();
        check.update(input);

        // Add header with total byte sizes
        let mut header = header_inner(compression_level, buffer.len() as u16);
        let footer = footer_inner(&check);
        header.extend(buffer.into_iter().chain(footer));
        encoder.reset();
        Ok(header)
    }
}

/// Create an Bgzf style header
#[inline]
fn header_inner(compression_level: Compression, compressed_size: u16) -> Vec<u8> {
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
    header.write_u16::<LittleEndian>(6).unwrap(); // Extra flag len
    header.write_u8(b'B').unwrap(); // Bgzf subfield ID 1
    header.write_u8(b'C').unwrap(); // Bgzf subfield ID2
    header.write_u16::<LittleEndian>(2).unwrap(); // Bgzf sufield len
    header
        .write_u16::<LittleEndian>(compressed_size + 26 - 1)
        .unwrap(); // Size of block including header and footer - 1 BLEN

    header
}

/// Create an Bgzf style foote
#[cfg(not(feature = "libdeflate"))]
#[inline]
fn footer_inner(check: &crate::check::Crc32) -> Vec<u8> {
    let mut footer = Vec::with_capacity(8);
    footer.write_u32::<LittleEndian>(check.sum()).unwrap();
    footer.write_u32::<LittleEndian>(check.amount()).unwrap();
    footer
}

impl<W> Write for BgzfSyncWriter<W>
where
    W: Write,
{
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= self.blocksize {
            let b = self.buffer.split_to(self.blocksize).freeze();
            let compressed = compress(&b[..], &mut self.compressor, self.compression_level)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.writer.write_all(&compressed)?;
        }
        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        while !self.buffer.is_empty() {
            let b = self
                .buffer
                .split_to(std::cmp::min(self.buffer.len(), BGZF_BLOCK_SIZE))
                .freeze();
            let compressed = compress(&b[..], &mut self.compressor, self.compression_level)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.writer.write_all(&compressed)?;
            self.writer.write_all(BGZF_EOF)?; // this is an empty block
        }
        self.writer.flush()
    }
}

impl<W> Drop for BgzfSyncWriter<W>
where
    W: Write,
{
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

impl<R> Read for BgzfSyncReader<R>
where
    R: Read,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut total_read = 0;
        loop {
            let before = self.buffer.remaining();
            if before > buf.len() - total_read {
                self.buffer.copy_to_slice(&mut buf[total_read..]);
            } else if !self.buffer.is_empty() {
                self.buffer
                    .copy_to_slice(&mut buf[total_read..total_read + before]);
            }
            let after = self.buffer.remaining();
            total_read += before - after;

            if total_read == buf.len() {
                break;
            } else if total_read <= buf.len() {
                let mut header_buf = vec![0; Bgzf::HEADER_SIZE];
                if let Ok(()) = self.reader.read_exact(&mut header_buf) {
                    self.format.check_header(&header_buf).unwrap();
                    let size = self.format.get_block_size(&header_buf).unwrap();

                    self.compressed_buffer.clear();
                    self.compressed_buffer.resize(size - Bgzf::HEADER_SIZE, 0);
                    self.reader.read_exact(&mut self.compressed_buffer)?;

                    let check = self.format.get_footer_values(&self.compressed_buffer);
                    self.buffer.clear();
                    self.buffer.resize(check.amount as usize, 0);

                    decompress(
                        &self.compressed_buffer,
                        &mut self.decompressor,
                        &mut self.buffer,
                        check,
                    )
                    .unwrap();
                } else {
                    break;
                }
            }
        }

        Ok(total_read)
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_simple_bgzfsync() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes
        let input = b"
        This is a longer test than normal to come up with a bunch of text.
        We'll read just a few lines at a time.
        What if this is a longer string, does that then make
        things fail?
        ";

        let orig_file = dir.path().join("orig.output.txt");
        let mut orig_writer = BufWriter::new(File::create(&orig_file).unwrap());
        orig_writer.write_all(input).unwrap();
        drop(orig_writer);

        // Compress input to output
        let mut bgzf = BgzfSyncWriter::new(out_writer, Compression::new(3));
        bgzf.write_all(input).unwrap();
        bgzf.flush().unwrap();
        drop(bgzf);
        // dbg!(output_file);
        // dbg!(orig_file);
        // std::process::exit(1);

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut decoder = BgzfSyncReader::new(&result[..]);
        // let mut gz = MultiGzDecoder::new(&result[..]);
        let mut bytes = vec![];
        decoder.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }
}
