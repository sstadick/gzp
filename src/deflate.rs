//! DEFLATE based compression formats.
//!
//! This includes:
//!
//! - Gzip
//! - Zlib
//! - Raw Deflate
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "any_zlib")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{deflate::Zlib, par::compress::{ParCompress, ParCompressBuilder}, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParCompress<Zlib> = ParCompressBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```

use std::io::Write;

use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
#[cfg(feature = "any_zlib")]
use flate2::write::ZlibEncoder;
use flate2::{
    write::{DeflateEncoder, GzEncoder},
    Compress, Compression, FlushCompress,
};
#[cfg(not(feature = "libdeflate"))]
use flate2::{Decompress, FlushDecompress};

use crate::bgzf::{BgzfSyncWriter, BGZF_BLOCK_SIZE};
#[cfg(feature = "any_zlib")]
use crate::check::Adler32;
use crate::check::{Check, Crc32, PassThroughCheck};
use crate::mgzip::MgzipSyncWriter;
use crate::syncz::SyncZ;
use crate::{bgzf, check, mgzip, BlockFormatSpec, FormatSpec, GzpError, Pair, SyncWriter, ZWriter};

/// The extra amount of space to add to the compressed vec to allow for EOF and other possible extra characters
// const EXTRA: usize = 256;
const EXTRA: f64 = 0.1;

#[inline]
fn output_buffer_size(input_len: usize) -> usize {
    input_len + std::cmp::max(128, (input_len as f64 * EXTRA) as usize)
}

//////////////////////////////////////////////////////////
// GZIP
//////////////////////////////////////////////////////////

/// Gzip deflate stream with gzip header and footer.
#[derive(Copy, Clone, Debug)]
pub struct Gzip {}

impl FormatSpec for Gzip {
    type C = Crc32;
    type Compressor = Compress;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        Ok(Compress::new(compression_level, false))
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        cfg!(feature = "any_zlib")
    }

    #[inline]
    #[allow(unused)]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 128 allows odd small sized blocks to extend up to a byte boundary and end stream
        let mut buffer = Vec::with_capacity(output_buffer_size(input.len()));
        let flush = if is_last {
            FlushCompress::Finish
        } else {
            FlushCompress::Sync
        };
        // let mut encoder = Compress::new(compression_level, false);
        #[cfg(feature = "any_zlib")]
        if let Some(dict) = dict {
            encoder.set_dictionary(&dict[..])?;
        }
        encoder.compress_vec(input, &mut buffer, flush)?;

        encoder.reset();
        Ok(buffer)
    }

    #[rustfmt::skip]
    fn header(&self, compression_level: Compression) -> Vec<u8> {
        let comp_value = if compression_level.level() >= Compression::best().level() {
            2
        } else if compression_level.level() <= Compression::fast().level() {
            4
        } else {
            0
        };

        let header = vec![
            Pair { num_bytes: 1, value: 31 }, // 0x1f in flate2
            Pair { num_bytes: 1, value: 139 }, // 0x8b in flate2
            Pair { num_bytes: 1, value: 8 }, // deflate
            Pair { num_bytes: 1, value: 0 }, // name / comment
            Pair { num_bytes: 4, value: 0 }, // mtime
            Pair { num_bytes: 1, value: comp_value }, // Compression level
            Pair { num_bytes: 1, value: 255 }, // OS
        ];

        self.to_bytes(&header)
    }

    #[rustfmt::skip]
    fn footer(&self, check: &Self::C) -> Vec<u8> {
        let footer = vec![
            Pair { num_bytes: 4, value: check.sum() as usize },
            Pair { num_bytes: 4, value: check.amount() as usize },
        ];
        self.to_bytes(&footer)
    }
}

impl<W> SyncWriter<W> for Gzip
where
    W: Write,
{
    type OutputWriter = GzEncoder<W>;

    fn sync_writer(writer: W, compression_level: Compression) -> GzEncoder<W> {
        GzEncoder::new(writer, compression_level)
    }
}

impl<W: Write> ZWriter for SyncZ<GzEncoder<W>> {
    fn finish(&mut self) -> Result<(), GzpError> {
        self.inner.take().unwrap().finish()?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////
// ZLIB
//////////////////////////////////////////////////////////

/// Zlib deflate stream with zlib header and footer.
#[cfg(feature = "any_zlib")]
#[derive(Copy, Clone, Debug)]
pub struct Zlib {}

#[cfg(feature = "any_zlib")]
impl FormatSpec for Zlib {
    type C = Adler32;
    type Compressor = Compress;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        Ok(Compress::new(compression_level, false))
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        cfg!(feature = "any_zlib")
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        _compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 16 allows odd small sized blocks to extend up to a byte boundary and end stream
        let mut buffer = Vec::with_capacity(output_buffer_size(input.len()));
        #[cfg(feature = "any_zlib")]
        if let Some(dict) = dict {
            encoder.set_dictionary(&dict[..])?;
        }
        encoder.compress_vec(
            input,
            &mut buffer,
            if is_last {
                FlushCompress::Finish
            } else {
                FlushCompress::Sync
            },
        )?;
        encoder.reset();
        Ok(buffer)
    }

    fn header(&self, compression_level: Compression) -> Vec<u8> {
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

        let mut head = (0x78 << 8) + // deflate, 32k window
            comp_value; // compression level clue
        head += 31 - (head % 31); // make it multiple of 31
        let header = vec![
            Pair {
                num_bytes: -2,
                value: head,
            }, // zlib uses big-endian
        ];
        self.to_bytes(&header)
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
        let footer = vec![Pair {
            num_bytes: -4,
            value: check.sum() as usize,
        }];
        self.to_bytes(&footer)
    }
}

#[cfg(feature = "any_zlib")]
impl<W> SyncWriter<W> for Zlib
where
    W: Write,
{
    type OutputWriter = ZlibEncoder<W>;

    fn sync_writer(writer: W, compression_level: Compression) -> ZlibEncoder<W> {
        ZlibEncoder::new(writer, compression_level)
    }
}

#[cfg(feature = "any_zlib")]
impl<W: Write> ZWriter for SyncZ<ZlibEncoder<W>> {
    fn finish(&mut self) -> Result<(), GzpError> {
        self.inner.take().unwrap().finish()?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////
// DEFLATE
//////////////////////////////////////////////////////////

/// Produce a contiguous raw deflate
#[derive(Copy, Clone, Debug)]
pub struct RawDeflate {}

#[allow(unused)]
impl FormatSpec for RawDeflate {
    type C = PassThroughCheck;
    type Compressor = Compress;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        Ok(Compress::new(compression_level, false))
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        cfg!(feature = "any_zlib")
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 8 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(output_buffer_size(input.len()));
        // let mut encoder = Compress::new(compression_level, false);
        #[cfg(feature = "any_zlib")]
        if let Some(dict) = dict {
            encoder.set_dictionary(&dict[..])?;
        }
        // TODO: finish? on last block?
        encoder.compress_vec(input, &mut buffer, FlushCompress::Sync)?;
        encoder.reset();
        Ok(buffer)
    }

    fn header(&self, compression_level: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
        vec![]
    }
}

impl<W> SyncWriter<W> for RawDeflate
where
    W: Write,
{
    type OutputWriter = DeflateEncoder<W>;

    fn sync_writer(writer: W, compression_level: Compression) -> DeflateEncoder<W> {
        DeflateEncoder::new(writer, compression_level)
    }
}

impl<W: Write> ZWriter for SyncZ<DeflateEncoder<W>> {
    fn finish(&mut self) -> Result<(), GzpError> {
        self.inner.take().unwrap().finish()?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////
// MGZIP
//////////////////////////////////////////////////////////

/// Produce an Mgzip encoder
#[derive(Copy, Clone, Debug)]
pub struct Mgzip {}

impl BlockFormatSpec for Mgzip {
    #[cfg(feature = "libdeflate")]
    type B = check::LibDeflateCrc;
    #[cfg(not(feature = "libdeflate"))]
    type B = check::Crc32;

    #[cfg(feature = "libdeflate")]
    type Decompressor = libdeflater::Decompressor;
    #[cfg(not(feature = "libdeflate"))]
    type Decompressor = Decompress;

    const HEADER_SIZE: usize = 20;

    fn create_decompressor(&self) -> Self::Decompressor {
        #[cfg(feature = "libdeflate")]
        {
            libdeflater::Decompressor::new()
        }
        #[cfg(not(feature = "libdeflate"))]
        {
            Decompress::new(false)
        }
    }

    #[inline]
    fn decode_block(
        &self,
        decoder: &mut Self::Decompressor,
        input: &[u8],
        orig_size: usize,
    ) -> Result<Vec<u8>, GzpError> {
        #[cfg(feature = "libdeflate")]
        {
            let mut result = vec![0; orig_size];
            let _bytes_decompressed = decoder.deflate_decompress(input, &mut result)?;
            Ok(result)
        }

        #[cfg(not(feature = "libdeflate"))]
        {
            let mut result = Vec::with_capacity(orig_size);
            decoder.decompress_vec(&input, &mut result, FlushDecompress::Finish)?;
            decoder.reset(false);
            Ok(result)
        }
    }

    #[inline]
    fn check_header(&self, bytes: &[u8]) -> Result<(), GzpError> {
        // Check that the extra field flag is set
        if bytes[3] & 4 != 4 {
            Err(GzpError::InvalidHeader("Extra field flag not set"))
        } else if bytes[12] != b'I' || bytes[13] != b'G' {
            // Check for IG in SID
            Err(GzpError::InvalidHeader("Bad SID"))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn get_block_size(&self, bytes: &[u8]) -> Result<usize, GzpError> {
        Ok(LittleEndian::read_u32(&bytes[16..]) as usize)
    }
}

#[allow(unused)]
impl FormatSpec for Mgzip {
    type C = PassThroughCheck;

    #[cfg(feature = "libdeflate")]
    type Compressor = libdeflater::Compressor;

    #[cfg(not(feature = "libdeflate"))]
    type Compressor = Compress;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        #[cfg(feature = "libdeflate")]
        {
            Ok(libdeflater::Compressor::new(
                libdeflater::CompressionLvl::new(compression_level.level() as i32)
                    .map_err(GzpError::LibDeflaterCompressionLvl)?,
            ))
        }
        #[cfg(not(feature = "libdeflate"))]
        {
            Ok(Compress::new(compression_level, false))
        }
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        false
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        mgzip::compress(input, encoder, compression_level)
    }

    fn header(&self, compression_level: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
        vec![]
    }
}

impl<W> SyncWriter<W> for Mgzip
where
    W: Write,
{
    type OutputWriter = MgzipSyncWriter<W>;

    fn sync_writer(writer: W, compression_level: Compression) -> Self::OutputWriter {
        MgzipSyncWriter::new(writer, compression_level)
    }
}

impl<W: Write> ZWriter for SyncZ<MgzipSyncWriter<W>> {
    fn finish(&mut self) -> Result<(), GzpError> {
        self.inner.take().unwrap().flush()?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////
// BGZF
//////////////////////////////////////////////////////////

/// Produce an Bgzf encoder
#[derive(Copy, Clone, Debug)]
pub struct Bgzf {}

impl BlockFormatSpec for Bgzf {
    #[cfg(feature = "libdeflate")]
    type B = check::LibDeflateCrc;
    #[cfg(not(feature = "libdeflate"))]
    type B = check::Crc32;

    #[cfg(feature = "libdeflate")]
    type Decompressor = libdeflater::Decompressor;
    #[cfg(not(feature = "libdeflate"))]
    type Decompressor = Decompress;

    const HEADER_SIZE: usize = 18;

    fn create_decompressor(&self) -> Self::Decompressor {
        #[cfg(feature = "libdeflate")]
        {
            libdeflater::Decompressor::new()
        }
        #[cfg(not(feature = "libdeflate"))]
        {
            Decompress::new(false)
        }
    }
    #[inline]
    fn decode_block(
        &self,
        decoder: &mut Self::Decompressor,
        input: &[u8],
        orig_size: usize,
    ) -> Result<Vec<u8>, GzpError> {
        #[cfg(feature = "libdeflate")]
        {
            let mut result = vec![0; orig_size];
            let _bytes_decompressed = decoder.deflate_decompress(input, &mut result)?;
            Ok(result)
        }

        #[cfg(not(feature = "libdeflate"))]
        {
            let mut result = Vec::with_capacity(orig_size);
            decoder.decompress_vec(&input, &mut result, FlushDecompress::Finish)?;
            decoder.reset(false);
            Ok(result)
        }
    }

    #[inline]
    fn check_header(&self, bytes: &[u8]) -> Result<(), GzpError> {
        // Check that the extra field flag is set
        if bytes[3] & 4 != 4 {
            Err(GzpError::InvalidHeader("Extra field flag not set"))
        } else if bytes[12] != b'B' || bytes[13] != b'C' {
            // Check for BC in SID
            Err(GzpError::InvalidHeader("Bad SID"))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn get_block_size(&self, bytes: &[u8]) -> Result<usize, GzpError> {
        Ok(LittleEndian::read_u16(&bytes[16..]) as usize + 1)
    }
}

#[allow(unused)]
impl FormatSpec for Bgzf {
    type C = PassThroughCheck;

    #[cfg(feature = "libdeflate")]
    type Compressor = libdeflater::Compressor;

    #[cfg(not(feature = "libdeflate"))]
    type Compressor = Compress;

    const DEFAULT_BUFSIZE: usize = BGZF_BLOCK_SIZE;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        #[cfg(feature = "libdeflate")]
        {
            Ok(libdeflater::Compressor::new(
                libdeflater::CompressionLvl::new(compression_level.level() as i32)
                    .map_err(GzpError::LibDeflaterCompressionLvl)?,
            ))
        }
        #[cfg(not(feature = "libdeflate"))]
        {
            Ok(Compress::new(compression_level, false))
        }
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        false
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        let mut bytes = bgzf::compress(input, encoder, compression_level)?;
        if is_last {
            bytes.extend(bgzf::BGZF_EOF);
        }
        Ok(bytes)
    }

    fn header(&self, compression_level: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
        vec![]
    }
}

impl<W> SyncWriter<W> for Bgzf
where
    W: Write,
{
    // type OutputWriter = GzEncoder<W>;
    type OutputWriter = BgzfSyncWriter<W>;

    fn sync_writer(writer: W, compression_level: Compression) -> Self::OutputWriter {
        BgzfSyncWriter::new(writer, compression_level)
    }
}

impl<W: Write> ZWriter for SyncZ<BgzfSyncWriter<W>> {
    fn finish(&mut self) -> Result<(), GzpError> {
        self.inner.take().unwrap().flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    #[cfg(feature = "any_zlib")]
    use flate2::bufread::ZlibDecoder;
    use flate2::bufread::{GzDecoder, MultiGzDecoder};
    use proptest::prelude::*;
    use tempfile::tempdir;

    use crate::bgzf::{BgzfSyncReader, BGZF_BLOCK_SIZE};
    use crate::mgzip::MgzipSyncReader;
    use crate::par::compress::{ParCompress, ParCompressBuilder};
    use crate::par::decompress::ParDecompressBuilder;
    use crate::syncz::SyncZBuilder;
    use crate::{ZBuilder, ZWriter, BUFSIZE, DICT_SIZE};

    use super::*;

    #[test]
    fn test_simple_mgzip() {
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
        let mut par_gz: ParCompress<Mgzip> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

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

    #[test]
    fn test_simple() {
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
        let mut par_gz: ParCompress<Gzip> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = GzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    fn test_simple_drop() {
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
        let mut par_gz: ParCompress<Gzip> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        drop(par_gz);

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = GzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    fn test_simple_sync() {
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
        let mut z = SyncZBuilder::<Gzip, _>::new().from_writer(out_writer);
        z.write_all(input).unwrap();
        z.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = GzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    fn test_simple_sync_drop() {
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
        let mut z = SyncZBuilder::<Gzip, _>::new().from_writer(out_writer);
        z.write_all(input).unwrap();
        drop(z);

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = GzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    #[cfg(feature = "any_zlib")]
    fn test_simple_zlib() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes
        let input = b"\
        This is a longer test than normal to come up with a bunch of text.\n\
        We'll read just a few lines at a time.\n\
        ";

        // Compress input to output
        let mut par_gz: ParCompress<Zlib> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = ZlibDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    #[cfg(feature = "any_zlib")]
    fn test_simple_zlib_sync() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes
        let input = b"\
        This is a longer test than normal to come up with a bunch of text.\n\
        We'll read just a few lines at a time.\n\
        ";

        // Compress input to output
        let mut z = SyncZBuilder::<Zlib, _>::new().from_writer(out_writer);
        z.write_all(input).unwrap();
        z.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = ZlibDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    fn test_regression() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes that is 206 bytes long
        // let input = b"The quick brown fox jumped over the moon\n";
        let input = [
            132, 19, 107, 159, 69, 217, 180, 131, 224, 49, 143, 41, 194, 30, 151, 22, 55, 30, 42,
            139, 219, 62, 123, 44, 148, 144, 88, 233, 199, 126, 110, 65, 6, 87, 51, 215, 17, 253,
            22, 63, 110, 1, 100, 202, 44, 138, 187, 226, 50, 50, 218, 24, 193, 218, 43, 172, 69,
            71, 8, 164, 5, 186, 189, 215, 151, 170, 243, 235, 219, 103, 1, 0, 102, 80, 179, 95,
            247, 26, 168, 147, 139, 245, 177, 253, 94, 82, 146, 133, 103, 223, 96, 34, 128, 237,
            143, 182, 48, 201, 201, 92, 29, 172, 137, 70, 227, 98, 181, 246, 80, 21, 106, 175, 246,
            41, 229, 187, 87, 65, 79, 63, 115, 66, 143, 251, 41, 251, 214, 7, 64, 196, 27, 180, 42,
            132, 116, 211, 148, 44, 177, 137, 91, 119, 245, 156, 78, 24, 253, 69, 38, 52, 152, 115,
            123, 94, 162, 72, 186, 239, 136, 179, 11, 180, 78, 54, 217, 120, 173, 141, 114, 174,
            220, 160, 223, 184, 114, 73, 148, 120, 43, 25, 21, 62, 62, 244, 85, 87, 19, 174, 182,
            227, 228, 70, 153, 5, 92, 51, 161, 9, 140, 199, 244, 241, 151, 236, 81, 211,
        ];

        // Compress input to output
        let mut par_gz: ParCompress<Gzip> = ParCompressBuilder::new()
            .buffer_size(DICT_SIZE)
            .unwrap()
            .from_writer(out_writer);
        par_gz.write_all(&input[..]).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = GzDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    #[test]
    fn test_simple_mgzip_etoe_decompress() {
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
        let mut par_gz: ParCompress<Mgzip> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let reader = BufReader::new(File::open(output_file).unwrap());
        let mut par_d = ParDecompressBuilder::<Mgzip>::new().from_reader(reader);
        let mut result = vec![];
        par_d.read_to_end(&mut result).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), result);
    }

    #[test]
    fn test_simple_bgzf_etoe_decompress() {
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
        let mut par_gz: ParCompress<Bgzf> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let reader = BufReader::new(File::open(output_file).unwrap());
        let mut par_d = ParDecompressBuilder::<Bgzf>::new().from_reader(reader);
        let mut result = vec![];
        par_d.read_to_end(&mut result).unwrap();
        par_d.finish().unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), result);
    }

    proptest! {
        #[test]
        #[ignore]
        fn test_all_gzip_comp(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000_usize,
            comp_level in 0..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            // let input_file = dir.path().join("input.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());
            // let mut in_writer = BufWriter::new(File::create(&input_file).unwrap());
            // in_writer.write_all(&input).unwrap();
            // in_writer.flush().unwrap();


            // Compress input to output
            let mut par_gz: Box<dyn ZWriter> = if num_threads > 0 {
                Box::new(ParCompressBuilder::<Gzip>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer))
            } else {
                Box::new(SyncZBuilder::<Gzip, _>::new().compression_level(Compression::new(comp_level)).from_writer(out_writer))
            };
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // std::process::exit(1);
            // Read output back in
            let mut reader = BufReader::new(File::open(output_file.clone()).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = GzDecoder::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }

        #[test]
        #[ignore]
        fn test_all_mgzip(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
            comp_level in 1..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz: Box<dyn ZWriter> = if num_threads > 0 {
                Box::new(ParCompressBuilder::<Mgzip>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer))
            } else {
                Box::new(SyncZBuilder::<Mgzip, _>::new().compression_level(Compression::new(comp_level)).from_writer(out_writer))
            };
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = MgzipSyncReader::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }

        #[test]
        #[ignore]
        fn test_all_mgzip_decompress(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)), // (DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 0..num_cpus::get(),
            num_threads_decomp in 0..num_cpus::get(),
            write_size in 1000..1001usize,
            comp_level in 1..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz = ZBuilder::<Mgzip, _>::new()
                    .buffer_size(buf_size)
                    .num_threads(num_threads)
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer);

            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // Read output back in
            let reader = BufReader::new(File::open(output_file).unwrap());
            let mut reader: Box<dyn Read> = if num_threads_decomp > 0 {
                Box::new(ParDecompressBuilder::<Mgzip>::new().num_threads(num_threads_decomp).unwrap().from_reader(reader))
            } else {
                Box::new(MgzipSyncReader::with_capacity(reader, buf_size))
            };
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();


            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), result);
        }

        #[test]
        #[ignore]
        fn test_all_bgzf_compress(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BGZF_BLOCK_SIZE,
            num_threads in 0..num_cpus::get(),
            write_size in 1..(4*BGZF_BLOCK_SIZE),
            comp_level in 1..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz: Box<dyn ZWriter> = if num_threads > 0 {
                Box::new(ParCompressBuilder::<Bgzf>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer))
            } else {
                Box::new(SyncZBuilder::<Bgzf, _>::new().compression_level(Compression::new(comp_level)).from_writer(out_writer))
            };
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = BgzfSyncReader::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            prop_assert_eq!(input.to_vec(), bytes, "BGZF fail");
        }

        #[test]
        #[ignore]
        fn test_all_bgzf_decompress(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BGZF_BLOCK_SIZE,
            num_threads in 0..num_cpus::get(),
            num_threads_decomp in 0..num_cpus::get(),
            write_size in 1000..1001_usize,
            comp_level in 1..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz = ZBuilder::<Bgzf, _>::new()
                    .buffer_size(buf_size)
                    .num_threads(num_threads)
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer);

            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // Read output back in
            let reader = BufReader::new(File::open(output_file).unwrap());
            let mut reader: Box<dyn Read> = if num_threads_decomp > 0 {
                Box::new(ParDecompressBuilder::<Bgzf>::new().num_threads(num_threads_decomp).unwrap().from_reader(reader))
            } else {
                Box::new(BgzfSyncReader::new(reader))
            };
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();


            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), result);
        }

        #[test]
        #[ignore]
        fn test_all_gzip_zbuilder(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz = ZBuilder::<Gzip, _>::new().num_threads(num_threads).from_writer(out_writer);
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // std::process::exit(1);
            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = GzDecoder::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }

        #[test]
        #[ignore]
        #[cfg(feature = "any_zlib")]
        fn test_all_zlib(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 1..num_cpus::get(),
            write_size in 1..10_000_usize,
            comp_level in 0..9_u32
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz: Box<dyn ZWriter> = if num_threads > 0 {
                Box::new(ParCompressBuilder::<Zlib>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .compression_level(Compression::new(comp_level))
                    .from_writer(out_writer))
            } else {
                Box::new(SyncZBuilder::<Zlib, _>::new().compression_level(Compression::new(comp_level)).from_writer(out_writer))
            };
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();


            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = ZlibDecoder::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }
    }
}
