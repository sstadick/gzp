//! Parallel decompression.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{parz::{ParZ, ParZBuilder}, deflate::Gzip, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Gzip> = ParZBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::{
    io::{self, Read, Write},
    process::exit,
    thread::JoinHandle,
};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};
pub use flate2::Compression;
use flate2::{bufread::GzDecoder, Decompress, FlushDecompress};
use flume::{bounded, unbounded, Receiver, Sender};

use crate::{
    check::Crc32, Check, CompressResult, FormatSpec, GzpError, Message, ZWriter, BUFSIZE, DICT_SIZE,
};

#[derive(Debug)]
pub struct ParDecompressBuilder<F>
where
    F: FormatSpec,
{
    buffer_size: usize,
    num_threads: usize,
    format: F,
}

impl<F> ParDecompressBuilder<F>
where
    F: FormatSpec,
{
    pub fn new() -> Self {
        Self {
            buffer_size: BUFSIZE,
            num_threads: num_cpus::get(),
            format: F::new(),
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Result<Self, GzpError> {
        if buffer_size < DICT_SIZE {
            return Err(GzpError::BufferSize(buffer_size, DICT_SIZE));
        }
        self.buffer_size = buffer_size;
        Ok(self)
    }

    pub fn num_threads(mut self, num_threads: usize) -> Result<Self, GzpError> {
        if num_threads == 0 {
            return Err(GzpError::NumThreads(num_threads));
        }
        self.num_threads = num_threads;
        Ok(self)
    }

    pub fn from_reader<R: Read + Send + 'static>(self, reader: R) -> ParDecompress<F> {
        let (tx_reader, rx_reader) = bounded(self.num_threads * 2);
        let buffer_size = self.buffer_size;
        let format = self.format;
        let handle = std::thread::spawn(move || {
            ParDecompress::run(&tx_reader, reader, self.num_threads, format)
        });
        ParDecompress {
            handle: Some(handle),
            rx_reader: Some(rx_reader),
            dictionary: None,
            buffer: None,
            buffer_size,
            format,
        }
    }
}

impl<F> Default for ParDecompressBuilder<F>
where
    F: FormatSpec,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
pub struct ParDecompress<F>
where
    F: FormatSpec,
{
    handle: Option<std::thread::JoinHandle<Result<(), GzpError>>>,
    rx_reader: Option<Receiver<Receiver<BytesMut>>>,
    buffer: Option<BytesMut>,
    dictionary: Option<Bytes>,
    buffer_size: usize,
    format: F,
}

impl<F> ParDecompress<F>
where
    F: FormatSpec,
{
    pub fn builder() -> ParDecompressBuilder<F> {
        ParDecompressBuilder::new()
    }

    #[allow(clippy::needless_collect)]
    fn run<R>(
        tx_reader: &Sender<Receiver<BytesMut>>,
        mut reader: R,
        num_threads: usize,
        format: F,
    ) -> Result<(), GzpError>
    where
        R: Read + Send + 'static,
    {
        let (tx, rx): (Sender<DMessage>, Receiver<DMessage>) = bounded(num_threads * 2);
        eprintln!("Got {} threads", num_threads);

        let handles: Vec<JoinHandle<Result<(), GzpError>>> = (0..num_threads)
            .map(|_| {
                let rx = rx.clone();
                std::thread::spawn(move || -> Result<(), GzpError> {
                    while let Ok(m) = rx.recv() {
                        let check_value = LittleEndian::read_u32(
                            &m.buffer[m.buffer.len() - 8..m.buffer.len() - 4],
                        );
                        let orig_size = LittleEndian::read_u32(&m.buffer[m.buffer.len() - 4..]);
                        let mut result = Vec::with_capacity(orig_size as usize);

                        let mut decoder = Decompress::new(false);
                        decoder
                            .decompress_vec(
                                &m.buffer[..m.buffer.len() - 8],
                                &mut result,
                                FlushDecompress::Finish,
                            )
                            .unwrap();
                        let mut check = Crc32::new();
                        check.update(&result);
                        assert!(check.sum() == check_value);
                        // TODO:  Add result type
                        m.oneshot.send(BytesMut::from(&result[..])).unwrap();
                    }
                    Ok(())
                })
            })
            // This collect is needed to force the evaluation, otherwise this thread will block on writes waiting
            // for data to show up that will never come since the iterator is lazy.
            .collect();

        // Reader
        loop {
            // TODO: check sid
            // TODO: probably make this a buffered reader
            // Read the first 28 bytes
            let mut buf = vec![0; 20];
            if let Ok(()) = reader.read_exact(&mut buf) {
                let size = LittleEndian::read_u32(&buf[16..]) as usize;
                let mut remainder = vec![0; size - 20];
                if let Ok(()) = reader.read_exact(&mut remainder) {
                    // let mut bytes = BytesMut::with_capacity(size);
                    // bytes.extend_from_slice(&buf);
                    // bytes.extend_from_slice(&remainder);
                    let (m, r) = DMessage::new_parts(Bytes::from(remainder), None);

                    tx_reader.send(r).unwrap();
                    tx.send(m).unwrap();
                    // Put a placeholder oneshot channel in the "to user" queue
                } else {
                    panic!("ahhhh")
                }
            } else {
                break; // EOF or malformed
            }
        }
        drop(tx);

        // Gracefully shutdown the compression threads
        handles
            .into_iter()
            .try_for_each(|handle| match handle.join() {
                Ok(result) => result,
                Err(e) => std::panic::resume_unwind(e),
            })
    }
}

#[derive(Debug)]
pub(crate) struct DMessage {
    buffer: Bytes,
    oneshot: Sender<BytesMut>,
    dictionary: Option<Bytes>,
    is_last: bool,
}

impl DMessage {
    pub(crate) fn new_parts(
        buffer: Bytes,
        dictionary: Option<Bytes>,
    ) -> (Self, Receiver<BytesMut>) {
        let (tx, rx) = unbounded();
        (
            DMessage {
                buffer,
                oneshot: tx,
                dictionary,
                is_last: false,
            },
            rx,
        )
    }
}

impl<F> Read for ParDecompress<F>
where
    F: FormatSpec,
{
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_copied = 0;
        let asked_for_bytes = buf.len();
        loop {
            if bytes_copied == asked_for_bytes {
                break;
            }

            if self.buffer.is_some() && self.buffer.as_ref().unwrap().len() > 0 {
                let curr_len = self.buffer.as_ref().unwrap().len();
                let to_copy = &self
                    .buffer
                    .as_mut()
                    .unwrap()
                    .split_to(std::cmp::min(buf.remaining_mut(), curr_len));

                buf.put(&to_copy[..]);
                bytes_copied += to_copy.len();
            } else if let Ok(new_buffer_chan) = self.rx_reader.as_mut().unwrap().recv() {
                if let Ok(new_buffer) = new_buffer_chan.recv() {
                    self.buffer = Some(new_buffer);
                    // std::mem::replace(&mut self.buffer, Some(new_buffer));
                } else {
                    panic!("failed read from placeholder chan")
                }
            } else if self.rx_reader.as_ref().unwrap().is_disconnected()
                && self.rx_reader.as_ref().unwrap().is_empty()
            {
                break;
            } else {
                panic!("chan chan")
            }
        }

        Ok(bytes_copied)
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
    use tempfile::tempdir;

    use crate::deflate::Mgzip;
    use crate::parz::{ParZ, ParZBuilder};
    use crate::ZWriter;

    use super::*;

    #[test]
    fn test_simple_mgzip_etoe() {
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
        let mut par_gz: ParZ<Mgzip> = ParZBuilder::new().from_writer(out_writer);
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

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_all_mgzip(
            input in prop::collection::vec(0..u8::MAX, 1..10000), // (DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 2..num_cpus::get(),
            write_size in 1000..1001usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_gz = ParZBuilder::<Mgzip>::new()
                    .buffer_size(buf_size).unwrap()
                    .num_threads(num_threads).unwrap()
                    .from_writer(out_writer);

            for chunk in input.chunks(write_size) {
                par_gz.write_all(chunk).unwrap();
            }
            par_gz.finish().unwrap();

            // std::process::exit(1);
            // Read output back in
            let reader = BufReader::new(File::open(output_file).unwrap());
            let mut reader = ParDecompressBuilder::<Mgzip>::new().num_threads(num_threads).unwrap().from_reader(reader);
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();


            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), result);
        }
    }
}
