use bytes::BytesMut;
use flate2::bufread::GzEncoder;
use futures::executor::block_on;
use std::io::{self, Read, Write};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub use flate2::Compression;

// Known differences from pigz
// - each block has an independent CRC value
// - no continual dictionary for compression is kept, it's per block only

// TODOs:
// - [X] Propagate errors
// - [X] Make buffer size, compression level all configurable
// - [X] Make number of threads to use configurable
// - [ ] Add tests (proptest?)
// - [ ] Move from close method to drop method?
// - [ ] Try not using `Bytes`?

// Refereneces:
// - https://github.com/shevek/parallelgzip/blob/master/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java
// - pbgzip
// - pigz

/// 128 KB default buffer size, same as pigz
const BUFSIZE: usize = 64 * (1 << 10) * 2;

/// The [`ParGz`] builder.
#[derive(Debug)]
pub struct ParGzBuilder<W> {
    /// The level to compress the output. Defaults to `3`.
    compression_level: Compression,
    /// The buffersize accumulate before trying to compress it. Defaults to [`BUFSIZE`].
    buffer_size: usize,
    /// The underlying writer to write to.
    writer: W,
    /// The number of threads to use for compression. Defaults to all available threads.
    num_threads: usize,
}

impl<W> ParGzBuilder<W>
where
    W: Send + Write + 'static,
{
    /// Create a new [`ParGzBuilder`] object.
    pub fn new(writer: W) -> Self {
        Self {
            compression_level: Compression::new(3),
            buffer_size: BUFSIZE,
            writer,
            num_threads: num_cpus::get(),
        }
    }

    /// Set the [`buffer_size`](ParGzBuilder.buffer_size).
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the [`compression_level`](ParGzBuilder.compression_level).
    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Set the [`num_threads`](ParGzBuilder.num_threads).
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Create a configured [`ParGz`] object.
    pub fn build(self) -> ParGz {
        let (tx, rx) = mpsc::channel(32);
        let buffer_size = self.buffer_size;
        let handle = std::thread::spawn(move || {
            ParGz::run(rx, self.writer, self.num_threads, self.compression_level)
        });
        ParGz {
            handle,
            tx,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
        }
    }
}

#[derive(Error, Debug)]
pub enum ParGzError {
    #[error("Failed to send over channel.")]
    ChannelSend,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("Unknown")]
    Unknown,
}
pub struct ParGz {
    handle: std::thread::JoinHandle<Result<(), ParGzError>>,
    tx: Sender<BytesMut>,
    buffer: BytesMut,
    buffer_size: usize,
}

impl ParGz {
    /// Create a builder to configure the [`ParGz`] runtime.
    pub fn builder<W>(writer: W) -> ParGzBuilder<W>
    where
        W: Write + Send + 'static,
    {
        ParGzBuilder::new(writer)
    }

    /// Launch the tokio runtime that coordinates the threadpool that does the following:
    ///
    /// 1. Receives chunks of bytes from from the [`ParGz::write`] method.
    /// 2. Spawn a task compressing the chunk of bytes.
    /// 3. Send the future for that task to the writer.
    /// 4. Write the bytes to the underlying writer.
    fn run<W>(
        mut rx: Receiver<BytesMut>,
        mut writer: W,
        num_threads: usize,
        compression_level: Compression,
    ) -> Result<(), ParGzError>
    where
        W: Write + Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .build()?;

        // Spawn the main task
        rt.block_on(async {
            let (out_sender, mut out_receiver) = mpsc::channel(32);
            let compressor = tokio::task::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    let task =
                        tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ParGzError> {
                            let mut buffer = Vec::with_capacity(chunk.len());
                            let mut gz: GzEncoder<&[u8]> =
                                GzEncoder::new(&chunk[..], compression_level);
                            gz.read_to_end(&mut buffer)?;

                            Ok(buffer)
                        });
                    out_sender
                        .send(task)
                        .await
                        .map_err(|_e| ParGzError::ChannelSend)?;
                }
                Ok::<(), ParGzError>(())
            });

            let writer_task = tokio::task::spawn_blocking(move || -> Result<(), ParGzError> {
                while let Some(chunk) = block_on(out_receiver.recv()) {
                    let chunk = block_on(chunk)??;
                    writer.write_all(&chunk)?;
                }

                Ok(())
            });

            compressor.await??;
            writer_task.await??;
            Ok::<(), ParGzError>(())
        })
    }

    /// Flush the buffers and wait on all threads to finish working.
    ///
    /// This *MUST* be called before the [`ParGz`] object goes out of scope.
    pub fn finish(mut self) -> Result<(), ParGzError> {
        self.flush().unwrap();
        drop(self.tx);
        self.handle.join().unwrap()
    }
}

impl Write for ParGz {
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= self.buffer_size {
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
