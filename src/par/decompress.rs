//! Parallel decompression for block type gzip formats (mgzip, bgzf)

use std::{
    io::{self, Read},
    thread::JoinHandle,
};

use bytes::{BufMut, Bytes, BytesMut};
use flate2::read::MultiGzDecoder;
pub use flate2::Compression;
use flume::{bounded, unbounded, Receiver, Sender};

use crate::{BlockFormatSpec, Check, GzpError, BUFSIZE, DICT_SIZE};

#[derive(Debug)]
pub struct ParDecompressBuilder<F>
where
    F: BlockFormatSpec,
{
    buffer_size: usize,
    num_threads: usize,
    format: F,
    pin_threads: Option<usize>,
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
            pin_threads: None,
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Result<Self, GzpError> {
        if buffer_size < DICT_SIZE {
            return Err(GzpError::BufferSize(buffer_size, DICT_SIZE));
        }
        self.buffer_size = buffer_size;
        Ok(self)
    }

    /// Set the number of threads and verify that that they are > 0 ensuring the mulit-threaded decompression will be attempted.
    pub fn num_threads(mut self, num_threads: usize) -> Result<Self, GzpError> {
        if num_threads == 0 {
            return Err(GzpError::NumThreads(num_threads));
        }
        self.num_threads = num_threads;
        Ok(self)
    }

    /// Set the [`pin_threads`](ParDecompressBuilder.pin_threads).
    pub fn pin_threads(mut self, pin_threads: Option<usize>) -> Self {
        self.pin_threads = pin_threads;
        self
    }

    /// Build a guaranteed multi-threaded decompressor
    pub fn from_reader<R: Read + Send + 'static>(self, reader: R) -> ParDecompress<F> {
        let (tx_reader, rx_reader) = bounded(self.num_threads * 2);
        let buffer_size = self.buffer_size;
        let format = self.format;
        let pin_threads = self.pin_threads;
        let handle = std::thread::spawn(move || {
            ParDecompress::run(&tx_reader, reader, self.num_threads, format, pin_threads)
        });
        ParDecompress {
            handle: Some(handle),
            rx_reader: Some(rx_reader),
            buffer: BytesMut::new(),
            buffer_size,
            format,
        }
    }

    /// Set the number of threads and allow 0 threads.
    pub fn maybe_num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// If `num_threads` is 0, this returns a single-threaded decompressor
    pub fn maybe_par_from_reader<R: Read + Send + 'static>(self, reader: R) -> Box<dyn Read> {
        if self.num_threads == 0 {
            Box::new(MultiGzDecoder::new(reader))
        } else {
            Box::new(self.from_reader(reader))
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
        pin_threads: Option<usize>,
    ) -> Result<(), GzpError>
    where
        R: Read + Send + 'static,
    {
        let (tx, rx): (Sender<DMessage>, Receiver<DMessage>) = bounded(num_threads * 2);

        let core_ids = core_affinity::get_core_ids().unwrap();
        let handles: Vec<JoinHandle<Result<(), GzpError>>> = (0..num_threads)
            .map(|i| {
                let rx = rx.clone();
                let core_ids = core_ids.clone();
                std::thread::spawn(move || -> Result<(), GzpError> {
                    if let Some(pin_at) = pin_threads {
                        if let Some(id) = core_ids.get(pin_at + i) {
                            core_affinity::set_for_current(*id);
                        }
                    }
                    let mut decompressor = format.create_decompressor();
                    while let Ok(m) = rx.recv() {
                        let check_values = format.get_footer_values(&m.buffer[..]);
                        let result = if check_values.amount != 0 {
                            format.decode_block(
                                &mut decompressor,
                                &m.buffer[..m.buffer.len() - 8],
                                check_values.amount as usize,
                            )?
                        } else {
                            vec![]
                        };

                        let mut check = F::B::new();
                        check.update(&result);

                        if check.sum() != check_values.sum {
                            return Err(GzpError::InvalidCheck {
                                found: check.sum(),
                                expected: check_values.sum,
                            });
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
            // Read gzip header
            let mut buf = vec![0; F::HEADER_SIZE];
            if let Ok(()) = reader.read_exact(&mut buf) {
                format.check_header(&buf)?;
                let size = format.get_block_size(&buf)?;
                let mut remainder = vec![0; size - F::HEADER_SIZE];
                reader.read_exact(&mut remainder)?;
                let (m, r) = DMessage::new_parts(Bytes::from(remainder));

                tx_reader.send(r).map_err(|_e| GzpError::ChannelSend)?;
                tx.send(m).map_err(|_e| GzpError::ChannelSend)?;
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
    pub fn finish(&mut self) -> Result<(), GzpError> {
        if self.rx_reader.is_some() {
            drop(self.rx_reader.take());
        }
        if self.handle.is_some() {
            match self.handle.take().unwrap().join() {
                Ok(result) => result,
                Err(e) => std::panic::resume_unwind(e),
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
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
            if !self.buffer.is_empty() {
                let curr_len = self.buffer.len();
                let to_copy = &self
                    .buffer
                    .split_to(std::cmp::min(buf.remaining_mut(), curr_len));

                buf.put(&to_copy[..]);
                bytes_copied += to_copy.len();
            } else if self.rx_reader.is_some() {
                // Then pull from channel of buffers
                match self.rx_reader.as_mut().unwrap().recv() {
                    Ok(new_buffer_chan) => {
                        self.buffer = match new_buffer_chan.recv() {
                            Ok(b) => b,
                            Err(_recv_error) => {
                                // If an error occurred receiving, that means the senders have been dropped and the
                                // decompressor thread hit an error. Collect that error here, and if it was an Io
                                // error, preserve it.
                                let error = match self.handle.take().unwrap().join() {
                                    Ok(result) => result,
                                    Err(e) => std::panic::resume_unwind(e),
                                };

                                let err = match error {
                                    Ok(()) => {
                                        self.rx_reader.take();
                                        break;
                                    } // finished reading file
                                    Err(GzpError::Io(ioerr)) => ioerr,
                                    Err(err) => io::Error::new(io::ErrorKind::Other, err),
                                };
                                self.rx_reader.take();
                                return Err(err);
                            }
                        };
                    }
                    Err(_recv_error) => {
                        // If an error occurred receiving, that means the senders have been dropped and the
                        // decompressor thread hit an error. Collect that error here, and if it was an Io
                        // error, preserve it.
                        let error = match self.handle.take().unwrap().join() {
                            Ok(result) => result,
                            Err(e) => std::panic::resume_unwind(e),
                        };

                        let err = match error {
                            Ok(()) => {
                                self.rx_reader.take();
                                break;
                            } // finished reading file
                            Err(GzpError::Io(ioerr)) => ioerr,
                            Err(err) => io::Error::new(io::ErrorKind::Other, err),
                        };
                        self.rx_reader.take();
                        return Err(err);
                    }
                }
            } else {
                break;
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
        if self.rx_reader.is_some() {
            match self.finish() {
                // ChannelSend errors are acceptable since we just dropped the receiver to cause the shutdown
                Ok(()) | Err(GzpError::ChannelSend) => (),
                Err(err) => std::panic::resume_unwind(Box::new(err)),
            }
        }
    }
}
