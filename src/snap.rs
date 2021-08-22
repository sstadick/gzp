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
//! use gzp::{snap::Snap, parz::ParZ};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Snap> = ParZ::builder(writer).build();
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::io::Read;

use bytes::Bytes;
use snap::read::FrameEncoder;

use crate::check::PassThroughCheck;
use crate::parz::Compression;
use crate::{FormatSpec, GzpError};

/// Produce snappy deflate stream
#[derive(Copy, Clone, Debug)]
pub struct Snap {}

#[allow(unused)]
impl FormatSpec for Snap {
    type C = PassThroughCheck;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        false
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        compression_level: Compression,
        dict: Option<Bytes>,
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

    fn footer(&self, check: Self::C) -> Vec<u8> {
        vec![]
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

    use crate::parz::ParZ;
    use crate::{BUFSIZE, DICT_SIZE};

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
        let mut par_gz: ParZ<Snap> = ParZ::builder(out_writer).build();
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
            num_threads in 4..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz: ParZ<Snap> = ParZ::builder(out_writer)
                .buffer_size(buf_size)
                .num_threads(num_threads)
                .build();
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
