//! Parallel Snap compression.
//!
//! # Examples
//!
//! ```
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::parsnap::ParSnap;
//!
//! let mut writer = vec![];
//! let mut par_snap = ParSnap::builder(writer).build();
//! par_snap.write_all(b"This is a first test line\n").unwrap();
//! par_snap.write_all(b"This is a second test line\n").unwrap();
//! par_snap.finish().unwrap();
//! ```
use std::io::{self, Read, Write};

use bytes::BytesMut;
use futures::executor::block_on;
use snap::read::FrameEncoder;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{GzpError, BUFSIZE};

/// The [`ParSnap`] builder.
#[derive(Debug)]
pub struct ParSnapBuilder<W> {
    /// The buffersize accumulate before trying to compress it. Defaults to [`BUFSIZE`].
    buffer_size: usize,
    /// The underlying writer to write to.
    writer: W,
    /// The number of threads to use for compression. Defaults to all available threads.
    num_threads: usize,
}

impl<W> ParSnapBuilder<W>
where
    W: Send + Write + 'static,
{
    /// Create a new [`ParSnapBuilder`] object.
    pub fn new(writer: W) -> Self {
        Self {
            buffer_size: BUFSIZE,
            writer,
            num_threads: num_cpus::get(),
        }
    }

    /// Set the [`buffer_size`](ParSnapBuilder.buffer_size).
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        assert!(buffer_size > 0);
        self.buffer_size = buffer_size;
        self
    }

    /// Set the [`num_threads`](ParSnapBuilder.num_threads).
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        assert!(num_threads <= num_cpus::get() && num_threads > 0);
        self.num_threads = num_threads;
        self
    }

    /// Create a configured [`ParSnap`] object.
    pub fn build(self) -> ParSnap {
        let (tx, rx) = mpsc::channel(self.num_threads);
        let buffer_size = self.buffer_size;
        let handle = std::thread::spawn(move || ParSnap::run(rx, self.writer, self.num_threads));
        ParSnap {
            handle,
            tx,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
        }
    }
}

pub struct ParSnap {
    handle: std::thread::JoinHandle<Result<(), GzpError>>,
    tx: Sender<BytesMut>,
    buffer: BytesMut,
    buffer_size: usize,
}

impl ParSnap {
    /// Create a builder to configure the [`ParSnap`] runtime.
    pub fn builder<W>(writer: W) -> ParSnapBuilder<W>
    where
        W: Write + Send + 'static,
    {
        ParSnapBuilder::new(writer)
    }

    /// Launch the tokio runtime that coordinates the threadpool that does the following:
    ///
    /// 1. Receives chunks of bytes from from the [`ParSnap::write`] method.
    /// 2. Spawn a task compressing the chunk of bytes.
    /// 3. Send the future for that task to the writer.
    /// 4. Write the bytes to the underlying writer.
    fn run<W>(mut rx: Receiver<BytesMut>, mut writer: W, num_threads: usize) -> Result<(), GzpError>
    where
        W: Write + Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .build()?;

        // Spawn the main task
        rt.block_on(async {
            let (out_sender, mut out_receiver) = mpsc::channel(num_threads);
            let compressor = tokio::task::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    let task = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, GzpError> {
                        let mut buffer = Vec::with_capacity(chunk.len());
                        let mut encoder = FrameEncoder::new(&chunk[..]);
                        encoder.read_to_end(&mut buffer)?;

                        Ok(buffer)
                    });
                    out_sender
                        .send(task)
                        .await
                        .map_err(|_e| GzpError::ChannelSend)?;
                }
                Ok::<(), GzpError>(())
            });

            let writer_task = tokio::task::spawn_blocking(move || -> Result<(), GzpError> {
                while let Some(chunk) = block_on(out_receiver.recv()) {
                    let chunk = block_on(chunk)??;
                    writer.write_all(&chunk)?;
                }
                writer.flush()?;
                Ok(())
            });

            compressor.await??;
            writer_task.await??;
            Ok::<(), GzpError>(())
        })
    }

    /// Flush the buffers and wait on all threads to finish working.
    ///
    /// This *MUST* be called before the [`ParGz`] object goes out of scope.
    pub fn finish(mut self) -> Result<(), GzpError> {
        self.flush()?;
        drop(self.tx);
        self.handle.join().unwrap()
    }
}

impl Write for ParSnap {
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() > self.buffer_size {
            let b = self.buffer.split_to(self.buffer_size);
            block_on(self.tx.send(b)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.buffer
                .reserve(self.buffer_size.saturating_sub(self.buffer.len()))
        }

        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        block_on(self.tx.send(self.buffer.split()))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    use proptest::prelude::*;
    use snap::read::FrameDecoder;
    use tempfile::tempdir;

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
        let mut par_snap = ParSnap::builder(out_writer).build();
        par_snap.write_all(input).unwrap();
        par_snap.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut snap = FrameDecoder::new(&result[..]);
        let mut bytes = vec![];
        snap.read_to_end(&mut bytes).unwrap();

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
        let mut par_snap = ParSnap::builder(out_writer)
            .buffer_size(205)
            // .compression_level(Compression::new(2))
            .build();
        par_snap.write_all(&input).unwrap();
        par_snap.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut snap = FrameDecoder::new(&result[..]);
        let mut bytes = vec![];
        snap.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }

    proptest! {
        #[test]
        fn test_all(
            input in prop::collection::vec(0..u8::MAX, 1..10_000),
            buf_size in 1..10_000usize,
            num_threads in 1..num_cpus::get(),
            write_size in 1..10_000usize,
        ) {
            let dir = tempdir().unwrap();

            // Create output file
            let output_file = dir.path().join("output.txt");
            let out_writer = BufWriter::new(File::create(&output_file).unwrap());


            // Compress input to output
            let mut par_snap = ParSnap::builder(out_writer)
                .buffer_size(buf_size)
                .num_threads(num_threads)
                .build();
            for chunk in input.chunks(write_size) {
                par_snap.write_all(chunk).unwrap();
            }
            par_snap.finish().unwrap();

            // Read output back in
            let mut reader = BufReader::new(File::open(output_file).unwrap());
            let mut result = vec![];
            reader.read_to_end(&mut result).unwrap();

            // Decompress it
            let mut snap = FrameDecoder::new(&result[..]);
            let mut bytes = vec![];
            snap.read_to_end(&mut bytes).unwrap();

            // Assert decompressed output is equal to input
            assert_eq!(input.to_vec(), bytes);
        }
    }
}
