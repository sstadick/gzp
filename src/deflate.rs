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
//! use gzp::{deflate::Zlib, parz::ParZ};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Zlib> = ParZ::builder(writer).build();
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use bytes::Bytes;
use flate2::{Compress, FlushCompress};

#[cfg(feature = "any_zlib")]
use crate::check::Adler32;
use crate::check::{Check, Crc32, PassThroughCheck};
use crate::parz::Compression;
use crate::{FormatSpec, GzpError, Pair};

/// Gzip deflate stream with gzip header and footer.
#[derive(Copy, Clone, Debug)]
pub struct Gzip {}

impl FormatSpec for Gzip {
    type C = Crc32;

    fn new() -> Self {
        Self {}
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
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 16 allows odd small sized blocks to extend up to a byte boundary and end stream
        let mut buffer = Vec::with_capacity(input.len() + 64);
        let mut encoder = Compress::new(compression_level, false);
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

/// Zlib deflate stream with zlib header and footer.
#[cfg(feature = "any_zlib")]
#[derive(Copy, Clone, Debug)]
pub struct Zlib {}

#[cfg(feature = "any_zlib")]
impl FormatSpec for Zlib {
    type C = Adler32;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        cfg!(feature = "any_zlib")
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 16 allows odd small sized blocks to extend up to a byte boundary and end stream
        let mut buffer = Vec::with_capacity(input.len() + 64);
        let mut encoder = Compress::new(compression_level, false);
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
        Ok(buffer)
    }

    fn header(&self, compression_leval: Compression) -> Vec<u8> {
        let comp_level = compression_leval.level();
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

/// Produce a contiguous raw deflat
#[derive(Copy, Clone, Debug)]
pub struct RawDeflate {}

#[allow(unused)]
impl FormatSpec for RawDeflate {
    type C = PassThroughCheck;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        cfg!(feature = "any_zlib")
    }

    #[inline]
    fn encode(
        &self,
        input: &[u8],
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        // The plus 8 allows odd small sized blocks to extend up to a byte boundary
        let mut buffer = Vec::with_capacity(input.len() + 64);
        let mut encoder = Compress::new(compression_level, false);
        #[cfg(feature = "any_zlib")]
        if let Some(dict) = dict {
            encoder.set_dictionary(&dict[..])?;
        }
        encoder.compress_vec(input, &mut buffer, FlushCompress::Sync)?;

        Ok(buffer)
    }

    fn header(&self, compression_leval: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, check: &Self::C) -> Vec<u8> {
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

    use flate2::bufread::GzDecoder;
    #[cfg(feature = "any_zlib")]
    use flate2::bufread::ZlibDecoder;
    use proptest::prelude::*;
    use tempfile::tempdir;

    use crate::parz::ParZ;
    use crate::z::Z;
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
        let mut par_gz: ParZ<Gzip> = ParZ::builder(out_writer).build();
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
    fn test_simple_z() {
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
        let mut par_gz: Z<Gzip, _> = Z::builder(out_writer).build().unwrap();
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
        let mut par_gz: ParZ<Zlib> = ParZ::builder(out_writer).build();
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
        // let mut par_gz = ZBuilder::<Gzip, _>::new(out_writer)
        //     .buffer_size(DICT_SIZE)
        //     .unwrap()
        //     .build();
        let mut par_gz: ParZ<Gzip> = ParZ::builder(out_writer)
            .buffer_size(DICT_SIZE)
            .unwrap()
            .build();
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

    proptest! {
        #[test]
        #[ignore]
        fn test_all_gzip(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            if num_threads > 0 {
                let mut par_gz: ParZ<Gzip> = ParZ::builder(out_writer)
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .build();
                for chunk in input.chunks(write_size) {
                    par_gz.write_all(chunk).unwrap();
                }
                par_gz.finish().unwrap();
            } else {
                let mut z = Z::<Gzip, _>::builder(out_writer).buffer_size(buf_size).unwrap().build().unwrap();
                for chunk in input.chunks(write_size) {
                    z.write_all(chunk).unwrap();
                }
                z.finish().unwrap();
            }

            dbg!(&output_file);
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
            num_threads in 0..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            if num_threads > 0 {
                let mut par_gz: ParZ<Zlib> = ParZ::builder(out_writer)
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .build();
                for chunk in input.chunks(write_size) {
                    par_gz.write_all(chunk).unwrap();
                }
                par_gz.finish().unwrap();
            } else {
                let mut z = Z::<Zlib, _>::builder(out_writer).buffer_size(buf_size).unwrap().build().unwrap();
                for chunk in input.chunks(write_size) {
                    z.write_all(chunk).unwrap();
                }
                z.finish().unwrap();
            }

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
