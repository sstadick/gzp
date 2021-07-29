use bytes::BytesMut;
use flate2::{bufread::GzEncoder, Compression};
use futures::executor::block_on;
use std::io::{self, Read, Write};
use thiserror::Error;
use tokio::{
    io::AsyncWrite,
    sync::mpsc::{self, Receiver, Sender},
};

// TODOs:
// - [ ] Propagate errors
// - [ ] Make buffer size, compression level and compression type all configurable
// - [ ] Make number of threads to use configurable
// - [ ] Add tests (proptest?)
// - [ ] Move from close method to drop method?

// Refereneces:
// - https://github.com/shevek/parallelgzip/blob/master/src/main/java/org/anarres/parallelgzip/ParallelGZIPOutputStream.java
// - pbgzip
// - pigz

// 128 KB I think, same as pigz
const BUFSIZE: usize = 64 * (1 << 10) * 2;

#[derive(Error, Debug)]
pub enum ParGzError {
    #[error("Unknown")]
    Unknown,
}
pub struct ParGz {
    // Do I need to join this handle via close or something?
    handle: std::thread::JoinHandle<Result<(), ParGzError>>,
    tx: Sender<BytesMut>,
    buffer: BytesMut,
}

impl ParGz {
    pub fn new<W>(writer: W) -> Self
    where
        W: Write + Send + 'static,
    {
        // Start a new background thread with a runtime and a channel to send writes over
        let (tx, rx) = mpsc::channel(32);
        // let mut inner = ParGzInner { writer, rx };
        let handle = std::thread::spawn(move || ParGz::run(rx, writer));
        Self {
            handle,
            tx,
            buffer: BytesMut::with_capacity(BUFSIZE),
        }
    }

    fn run<W>(mut rx: Receiver<BytesMut>, mut writer: W) -> Result<(), ParGzError>
    where
        W: Write + Send + 'static,
    {
        // TODO: unify errors
        let rt = tokio::runtime::Runtime::new().unwrap();
        // println!("{:?}", rt);

        // Spawn the main task
        rt.block_on(async {
            // eprintln!("In tokio runtime");
            let (out_sender, mut out_receiver) = mpsc::channel(32);
            let compressor = tokio::task::spawn(async move {
                // eprintln!("In compressor");
                while let Some(chunk) = rx.recv().await {
                    // eprintln!("Received a chunk: {}", chunk.len());
                    let task = tokio::task::spawn_blocking(move || {
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
                        gz.read_to_end(&mut buffer).unwrap();
                        buffer
                    });
                    // eprintln!("Sent task to writer");
                    out_sender.send(task).await.unwrap();
                }
            });

            let writer_task = tokio::task::spawn_blocking(move || {
                // eprintln!("In writer");
                while let Some(chunk) = block_on(out_receiver.recv()) {
                    // eprintln!("Writer received a chunk to write");
                    writer.write_all(&block_on(chunk).unwrap()).unwrap();
                    // eprintln!("Writer wrote a chunk ");
                }
            });

            compressor.await.unwrap();
            writer_task.await.unwrap();
            // eprintln!("Done waiting");
        });
        Ok(())
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
        if self.buffer.len() >= BUFSIZE {
            let b = self.buffer.split_to(BUFSIZE);
            block_on(self.tx.send(b)).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.buffer
                .reserve(BUFSIZE.saturating_sub(self.buffer.len()))
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
