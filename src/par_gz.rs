use bytes::{buf, BytesMut};
use flate2::bufread::GzEncoder;
use futures::executor::block_on;
use std::io::{self, Read, Write};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub use flate2::Compression;

// TODOs:
// - [X] Propagate errors
// - [X] Make buffer size, compression level all configurable
// - [X] Make number of threads to use configurable
// - [ ] Add tests (proptest?)
// - [ ] Move from close method to drop method?
// - [ ] Try not using `Bytes`?

// TODOs:
// - [ ] Add support for BGZIP: http://samtools.github.io/hts-specs/SAMv1.pdf
//  basically just track lines or something and flush when we would cross 64kb
//  Also include a bit more header info

// Refereneces:
// - https://github.com/shevek/parallelgzip/blob/master/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java
// - pbgzip
// - pigz

// 128 KB I think, same as pigz
const BUFSIZE: usize = 64 * (1 << 10) * 2;

#[derive(Debug)]
pub struct ParGzBuilder<W> {
    compression_level: Compression,
    buffer_size: usize,
    writer: W,
    num_threads: usize,
}

impl<W> ParGzBuilder<W>
where
    W: Send + Write + 'static,
{
    pub fn new(writer: W) -> Self {
        Self {
            compression_level: Compression::new(3),
            buffer_size: BUFSIZE,
            writer,
            num_threads: num_cpus::get(),
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    pub fn build(self) -> ParGz {
        let (tx, rx) = mpsc::channel(32);
        let comp_level = self.compression_level;
        let buffer_size = self.buffer_size;
        let handle = std::thread::spawn(move || ParGz::run(rx, self.writer, self.num_threads));
        ParGz {
            handle,
            tx,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            compression_level: comp_level,
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
    // Do I need to join this handle via close or something?
    handle: std::thread::JoinHandle<Result<(), ParGzError>>,
    tx: Sender<BytesMut>,
    buffer: BytesMut,
    buffer_size: usize,
    compression_level: Compression,
}

impl ParGz {
    pub fn builder<W>(writer: W) -> ParGzBuilder<W>
    where
        W: Write + Send + 'static,
    {
        ParGzBuilder::new(writer)
    }

    fn run<W>(
        mut rx: Receiver<BytesMut>,
        mut writer: W,
        num_threads: usize,
    ) -> Result<(), ParGzError>
    where
        W: Write + Send + 'static,
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_threads)
            .build()?;

        // Spawn the main task
        rt.block_on(async {
            // eprintln!("In tokio runtime");
            let (out_sender, mut out_receiver) = mpsc::channel(32);
            let compressor = tokio::task::spawn(async move {
                // eprintln!("In compressor");
                while let Some(chunk) = rx.recv().await {
                    // eprintln!("Received a chunk: {}", chunk.len());
                    let task =
                        tokio::task::spawn_blocking(move || -> Result<Vec<u8>, ParGzError> {
                            // TODO:
                            // - This seems inefficient to create a reader each time
                            // - keep track of if this is the first block or not so we can write a header
                            // - I'm assuming this does all the right things under the hood as described here:
                            //  https://linux.die.net/man/1/pigz
                            // - Add a "trailer"?
                            //   - https://github.com/shevek/parallelgzip/blob/af5f5c297e735f3f2df7aa4eb0e19a5810b8aff6/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java#L297
                            // eprintln!("Spawned a task to compress chunk");

                            let mut buffer = Vec::with_capacity(chunk.len());
                            let mut gz: GzEncoder<&[u8]> =
                                GzEncoder::new(&chunk[..], Compression::new(3));
                            gz.read_to_end(&mut buffer)?;
                            Ok(buffer)
                        });
                    // eprintln!("Sent task to writer");
                    out_sender
                        .send(task)
                        .await
                        .map_err(|_e| ParGzError::ChannelSend)?;
                }
                Ok::<(), ParGzError>(())
            });

            let writer_task = tokio::task::spawn_blocking(move || -> Result<(), ParGzError> {
                // eprintln!("In writer");
                while let Some(chunk) = block_on(out_receiver.recv()) {
                    // eprintln!("Writer received a chunk to write");
                    writer.write_all(&block_on(chunk)??)?;
                    // eprintln!("Writer wrote a chunk ");
                }
                Ok(())
            });

            compressor.await??;
            writer_task.await??;
            Ok::<(), ParGzError>(())
            // eprintln!("Done waiting");
        })
    }

    // TODO:
    // - make this impl drop?
    // - maybe name this `finish` like flate2?
    pub fn close(mut self) -> Result<(), ParGzError> {
        self.flush().unwrap();
        drop(self.tx);
        self.handle.join().unwrap()
    }
}

impl Write for ParGz {
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

    fn flush(&mut self) -> std::io::Result<()> {
        block_on(self.tx.send(self.buffer.split()))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // TODO: more cleanup?
        Ok(())
    }
}
