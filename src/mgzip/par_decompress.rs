//! Parallel decompression for block type gzip formats (mgzip, bgzf)
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{parz::{ParZ, ParZBuilder}, deflate::Mgzip, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParZ<Mgzip> = ParZBuilder::new().from_writer(writer);
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
    check::Crc32, BlockFormatSpec, Check, CompressResult, FormatSpec, GzpError, Message, ZWriter,
    BUFSIZE, DICT_SIZE,
};

#[derive(Debug)]
pub struct ParDecompressBuilder<F>
where
    F: BlockFormatSpec,
{
    buffer_size: usize,
    num_threads: usize,
    format: F,
}

impl<F> ParDecompressBuilder<F>
where
    F: BlockFormatSpec,
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
            buffer: BytesMut::new(),
            buffer_size,
            format,
        }
    }
}

impl<F> Default for ParDecompressBuilder<F>
where
    F: BlockFormatSpec,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
pub struct ParDecompress<F>
where
    F: BlockFormatSpec,
{
    handle: Option<std::thread::JoinHandle<Result<(), GzpError>>>,
    rx_reader: Option<Receiver<Receiver<BytesMut>>>,
    buffer: BytesMut,
    buffer_size: usize,
    format: F,
}

impl<F> ParDecompress<F>
where
    F: BlockFormatSpec,
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

        let handles: Vec<JoinHandle<Result<(), GzpError>>> = (0..num_threads)
            .map(|_| {
                let rx = rx.clone();
                std::thread::spawn(move || -> Result<(), GzpError> {
                    while let Ok(m) = rx.recv() {
                        let check_values = format.get_footer_values(&m.buffer[..]);
                        let result = format.decode_block(
                            &m.buffer[..m.buffer.len() - 8],
                            check_values.amount as usize,
                        )?;

                        let mut check = F::B::new();
                        check.update(&result);

                        if check.sum() != check_values.sum {
                            return Err(GzpError::InvalidCheck(check.sum(), check_values.sum));
                        }
                        m.oneshot
                            .send(BytesMut::from(&result[..]))
                            .map_err(|_e| GzpError::ChannelSend)?;
                    }
                    Ok(())
                })
            })
            // This collect is needed to force the evaluation, otherwise this thread will block on writes waiting
            // for data to show up that will never come since the iterator is lazy.
            .collect();

        // Reader
        loop {
            // TODO: probably make this a buffered reader??
            // Read gzip header
            let mut buf = vec![0; 20];
            if let Ok(()) = reader.read_exact(&mut buf) {
                format.check_header(&buf);
                let size = format.get_block_size(&buf)?;
                let mut remainder = vec![0; size - 20];
                reader.read_exact(&mut remainder)?;
                let (m, r) = DMessage::new_parts(Bytes::from(remainder));

                tx_reader.send(r).unwrap();
                tx.send(m).unwrap();
            } else {
                break; // EOF
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

    /// Close things in such a way as to get errors
    fn finish(&mut self) -> Result<(), GzpError> {
        drop(self.rx_reader.take());
        match self.handle.take().unwrap().join() {
            Ok(result) => result,
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}

#[derive(Debug)]
pub(crate) struct DMessage {
    buffer: Bytes,
    oneshot: Sender<BytesMut>,
    is_last: bool,
}

impl DMessage {
    pub(crate) fn new_parts(buffer: Bytes) -> (Self, Receiver<BytesMut>) {
        let (tx, rx) = unbounded();
        (
            DMessage {
                buffer,
                oneshot: tx,
                is_last: false,
            },
            rx,
        )
    }
}

impl<F> Read for ParDecompress<F>
where
    F: BlockFormatSpec,
{
    // Ok(0) means done
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_copied = 0;
        let asked_for_bytes = buf.len();
        loop {
            if bytes_copied == asked_for_bytes {
                break;
            }

            // First try to use up anything in current buffer
            if self.buffer.len() > 0 {
                let curr_len = self.buffer.len();
                let to_copy = &self
                    .buffer
                    .split_to(std::cmp::min(buf.remaining_mut(), curr_len));

                buf.put(&to_copy[..]);
                bytes_copied += to_copy.len();
            } else {
                // Then pull from channel of buffers
                match self.rx_reader.as_mut().unwrap().recv() {
                    Ok(new_buffer_chan) => {
                        self.buffer = new_buffer_chan
                            .recv()
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    }
                    Err(e) => {
                        if self.rx_reader.as_ref().unwrap().is_disconnected()
                            && self.rx_reader.as_ref().unwrap().is_empty()
                        {
                            break;
                        } else {
                            return Err(io::Error::new(io::ErrorKind::Other, e));
                        }
                    }
                }
            }
        }

        Ok(bytes_copied)
    }
}

impl<F> Drop for ParDecompress<F>
where
    F: BlockFormatSpec,
{
    fn drop(&mut self) {
        self.finish().unwrap();
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

    proptest! {
        #[test]
        #[ignore]
        fn test_all_mgzip(
            input in prop::collection::vec(0..u8::MAX, 1..(DICT_SIZE * 10)), // (DICT_SIZE * 10)),
            buf_size in DICT_SIZE..BUFSIZE,
            num_threads in 1..num_cpus::get(),
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
