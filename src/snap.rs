//! Snap compression format.
//!
//! This uses the `FrameEncoder` format so each block is a frame.
//!
//! # References
//!
//! - [snap-rs](https://docs.rs/snap)
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "snappy")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{snap::Snap, par::compress::{ParCompressBuilder, ParCompress}, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParCompress<Snap> = ParCompressBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::io::{Read, Write};

use bytes::Bytes;
use snap::read::FrameEncoder;

use crate::check::PassThroughCheck;
use crate::syncz::SyncZ;
use crate::{Compression, FormatSpec, GzpError, SyncWriter, ZWriter};

/// Produce snappy deflate stream
#[derive(Copy, Clone, Debug)]
pub struct Snap {}

#[allow(unused)]
impl FormatSpec for Snap {
    type C = PassThroughCheck;
    // TODO: use the raw Encoder and apply same optimizations ad DEFLATE formats
    type Compressor = ();

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        false
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        Ok(())
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        compressor: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 8 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(input.len());
        let mut encoder = FrameEncoder::new(input);
        encoder.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    fn header(&self, compression_leval: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
        vec![]
    }
}

impl<W> SyncWriter<W> for Snap
where
    W: Write,
{
    type OutputWriter = snap::write::FrameEncoder<W>;

    /// Compression level is ignored for snap
    fn sync_writer(writer: W, _compression_level: Compression) -> snap::write::FrameEncoder<W> {
        snap::write::FrameEncoder::new(writer)
    }
}

impl<W: Write> ZWriter for SyncZ<snap::write::FrameEncoder<W>> {
    /// This is a no-op for snappy and does nothing
    fn finish(&mut self) -> Result<(), GzpError> {
        drop(self.inner.take());
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

    use proptest::prelude::*;
    use snap::read::FrameDecoder;
    use tempfile::tempdir;

    use crate::par::compress::{ParCompress, ParCompressBuilder};
    use crate::syncz::SyncZBuilder;
    use crate::{ZBuilder, ZWriter, BUFSIZE, DICT_SIZE};

    use super::*;

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
        let mut par_gz: ParCompress<Snap> = ParCompressBuilder::new().from_writer(out_writer);
        par_gz.write_all(input).unwrap();
        par_gz.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = FrameDecoder::new(&result[..]);
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    proptest! {
        #[test]
        #[ignore]
        fn test_all_snap(
            input in prop::collection::vec(0..u8::MAX, 1..DICT_SIZE * 10),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz: Box<dyn ZWriter> = if num_threads > 0 {
                Box::new(ParCompressBuilder::<Snap>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .from_writer(out_writer))
            } else {
                Box::new(SyncZBuilder::<Snap, _>::new().from_writer(out_writer))
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
            let mut gz = FrameDecoder::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }

        #[test]
        #[ignore]
        fn test_all_snap_zbuilder(
            input in prop::collection::vec(0..u8::MAX, 1..DICT_SIZE * 10),
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz = ZBuilder::<Snap, _>::new().num_threads(num_threads).from_writer(out_writer);
            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut gz = FrameDecoder::new(&result[..]);
            let mut bytes = vec![];
            gz.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }

    }
}
