//! Parallel compression.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{par::compress::{ParCompress, ParCompressBuilder}, deflate::Gzip, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParCompress<Gzip> = ParCompressBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::{
    io::{self, Write},
    thread::JoinHandle,
};

use bytes::{Bytes, BytesMut};
pub use flate2::Compression;
use flume::{bounded, Receiver, Sender};

use crate::check::Check;
use crate::{CompressResult, FormatSpec, GzpError, Message, ZWriter, DICT_SIZE};

/// The [`ParCompress`] builder.
#[derive(Debug)]
pub struct ParCompressBuilder<F>
where
    F: FormatSpec,
{
    /// The buffersize accumulate before trying to compress it. Defaults to `F::DEFAULT_BUFSIZE`.
    buffer_size: usize,
    /// The number of threads to use for compression. Defaults to all available threads.
    num_threads: usize,
    /// The compression level of the output, see [`Compression`].
    compression_level: Compression,
    /// The out file format to use.
    format: F,
    /// Whether or not to pin threads to specific cpus and what core to start pins at
    pin_threads: Option<usize>,
}

impl<F> ParCompressBuilder<F>
where
    F: FormatSpec,
{
    /// Create a new [`ParCompressBuilder`] object.
    pub fn new() -> Self {
        Self {
            buffer_size: F::DEFAULT_BUFSIZE,
            num_threads: num_cpus::get(),
            compression_level: Compression::new(3),
            format: F::new(),
            pin_threads: None,
        }
    }

    /// Set the [`buffer_size`](ParCompressBuilder.buffer_size). Must be >= [`DICT_SIZE`].
    ///
    /// # Errors
    /// - [`GzpError::BufferSize`] error if selected buffer size is less than [`DICT_SIZE`].
    pub fn buffer_size(mut self, buffer_size: usize) -> Result<Self, GzpError> {
        if buffer_size < DICT_SIZE {
            return Err(GzpError::BufferSize(buffer_size, DICT_SIZE));
        }
        self.buffer_size = buffer_size;
        Ok(self)
    }

    /// Set the [`num_threads`](ParCompressBuilder.num_threads) that will be used for compression.
    ///
    /// Note that one additional thread will be used for writing. Threads equal to `num_threads`
    /// will be spun up in the background and will remain blocking and waiting for blocks to compress
    /// until ['finish`](ParCompress.finish) is called.
    ///
    /// # Errors
    /// - [`GzpError::NumThreads`] error if 0 threads selected.
    pub fn num_threads(mut self, num_threads: usize) -> Result<Self, GzpError> {
        if num_threads == 0 {
            return Err(GzpError::NumThreads(num_threads));
        }
        self.num_threads = num_threads;
        Ok(self)
    }

    /// Set the [`compression_level`](ParCompressBuilder.compression_level).
    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Set the [`pin_threads`](ParCompressBuilder.pin_threads).
    pub fn pin_threads(mut self, pin_threads: Option<usize>) -> Self {
        self.pin_threads = pin_threads;
        self
    }

    /// Create a configured [`ParCompress`] object.
    pub fn from_writer<W: Write + Send + 'static>(self, writer: W) -> ParCompress<F> {
        let (tx_compressor, rx_compressor) = bounded(self.num_threads * 2);
        let (tx_writer, rx_writer) = bounded(self.num_threads * 2);
        let buffer_size = self.buffer_size;
        let comp_level = self.compression_level;
        let pin_threads = self.pin_threads;
        let format = self.format;
        let handle = std::thread::spawn(move || {
            ParCompress::run(
                &rx_compressor,
                &rx_writer,
                writer,
                self.num_threads,
                comp_level,
                format,
                pin_threads,
            )
        });
        ParCompress {
            handle: Some(handle),
            tx_compressor: Some(tx_compressor),
            tx_writer: Some(tx_writer),
            dictionary: None,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            format,
        }
    }
}

impl<F> Default for ParCompressBuilder<F>
where
    F: FormatSpec,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
pub struct ParCompress<F>
where
    F: FormatSpec,
{
    handle: Option<std::thread::JoinHandle<Result<(), GzpError>>>,
    tx_compressor: Option<Sender<Message<F::C>>>,
    tx_writer: Option<Sender<Receiver<CompressResult<F::C>>>>,
    buffer: BytesMut,
    dictionary: Option<Bytes>,
    buffer_size: usize,
    format: F,
}

impl<F> ParCompress<F>
where
    F: FormatSpec,
{
    /// Create a builder to configure the [`ParCompress`] runtime.
    pub fn builder() -> ParCompressBuilder<F> {
        ParCompressBuilder::new()
    }

    /// Launch threads to compress chunks and coordinate sending compressed results
    /// to the writer.
    #[allow(clippy::needless_collect)]
    fn run<W>(
        rx: &Receiver<Message<F::C>>,
        rx_writer: &Receiver<Receiver<CompressResult<F::C>>>,
        mut writer: W,
        num_threads: usize,
        compression_level: Compression,
        format: F,
        pin_threads: Option<usize>,
    ) -> Result<(), GzpError>
    where
        W: Write + Send + 'static,
    {
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

                    let mut compressor = format.create_compressor(compression_level)?;
                    while let Ok(m) = rx.recv() {
                        let chunk = &m.buffer;
                        let buffer = format.encode(
                            chunk,
                            &mut compressor,
                            compression_level,
                            m.dictionary.as_ref(),
                            m.is_last,
                        )?;
                        let mut check = F::create_check();
                        check.update(chunk);

                        m.oneshot
                            .send(Ok::<(F::C, Vec<u8>), GzpError>((check, buffer)))
                            .map_err(|_e| GzpError::ChannelSend)?;
                    }
                    Ok(())
                })
            })
            // This collect is needed to force the evaluation, otherwise this thread will block on writes waiting
            // for data to show up that will never come since the iterator is lazy.
            .collect();

        // Writer
        writer.write_all(&format.header(compression_level))?;
        let mut running_check = F::create_check();
        while let Ok(chunk_chan) = rx_writer.recv() {
            let chunk_chan: Receiver<CompressResult<F::C>> = chunk_chan;
            let (check, chunk) = chunk_chan.recv()??;
            running_check.combine(&check);
            writer.write_all(&chunk)?;
        }
        let footer = format.footer(&running_check);
        writer.write_all(&footer)?;
        writer.flush()?;

        // Gracefully shutdown the compression threads
        handles
            .into_iter()
            .try_for_each(|handle| match handle.join() {
                Ok(result) => result,
                Err(e) => std::panic::resume_unwind(e),
            })
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    ///
    /// If this is the last buffer to be sent, set `is_last` to false to trigger compression
    /// stream completion.
    ///
    /// # Panics
    /// - If called after `finish`
    fn flush_last(&mut self, is_last: bool) -> std::io::Result<()> {
        loop {
            let b = self
                .buffer
                .split_to(std::cmp::min(self.buffer.len(), self.buffer_size))
                .freeze();
            let (mut m, r) = Message::new_parts(b, std::mem::replace(&mut self.dictionary, None));
            if is_last && self.buffer.is_empty() {
                m.is_last = true;
            }

            if m.buffer.len() >= DICT_SIZE && !m.is_last && self.format.needs_dict() {
                self.dictionary = Some(m.buffer.slice(m.buffer.len() - DICT_SIZE..));
            }

            self.tx_writer
                .as_ref()
                .unwrap()
                .send(r)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.tx_compressor
                .as_ref()
                .unwrap()
                .send(m)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            if self.buffer.is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl<F> ZWriter for ParCompress<F>
where
    F: FormatSpec,
{
    /// Flush the buffers and wait on all threads to finish working.
    ///
    /// This *MUST* be called before the [`ParCompress`] object goes out of scope.
    ///
    /// # Errors
    /// - [`GzpError`] if there is an issue flushing the last blocks or an issue joining on the writer thread
    ///
    /// # Panics
    /// - If called twice
    fn finish(&mut self) -> Result<(), GzpError> {
        self.flush_last(true)?;

        // while !self.tx_compressor.as_ref().unwrap().is_empty() {}
        // while !self.tx_writer.as_ref().unwrap().is_empty() {}
        drop(self.tx_compressor.take());
        drop(self.tx_writer.take());
        match self.handle.take().unwrap().join() {
            Ok(result) => result,
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}

impl<F> Drop for ParCompress<F>
where
    F: FormatSpec,
{
    fn drop(&mut self) {
        if self.tx_compressor.is_some() && self.tx_writer.is_some() && self.handle.is_some() {
            self.finish().unwrap();
        }
        // Resources already cleaned up if channels and handle are None
    }
}

impl<F> Write for ParCompress<F>
where
    F: FormatSpec,
{
    /// Write a buffer into this writer, returning how many bytes were written.
    ///
    /// # Panics
    /// - If called after calling `finish`
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while self.buffer.len() > self.buffer_size {
            let b = self.buffer.split_to(self.buffer_size).freeze();
            let (m, r) = Message::new_parts(b, std::mem::replace(&mut self.dictionary, None));
            // Bytes uses and ARC, this is O(1) to get the last 32k bytes from teh previous chunk
            self.dictionary = if self.format.needs_dict() {
                Some(m.buffer.slice(m.buffer.len() - DICT_SIZE..))
            } else {
                None
            };
            self.tx_writer
                .as_ref()
                .unwrap()
                .send(r)
                .map_err(|_send_error| {
                    // If an error occured sending, that means the recievers have dropped an the compressor thread hit an error
                    // Collect that error here, and if it was an Io error, preserve it
                    let error = match self.handle.take().unwrap().join() {
                        Ok(result) => result,
                        Err(e) => std::panic::resume_unwind(e),
                    };
                    match error {
                        Ok(()) => std::panic::resume_unwind(Box::new(error)), // something weird happened
                        Err(GzpError::Io(ioerr)) => ioerr,
                        Err(err) => io::Error::new(io::ErrorKind::Other, err),
                    }
                })?;
            self.tx_compressor
                .as_ref()
                .unwrap()
                .send(m)
                .map_err(|_send_error| {
                    // If an error occured sending, that means the recievers have dropped an the compressor thread hit an error
                    // Collect that error here, and if it was an Io error, preserve it
                    let error = match self.handle.take().unwrap().join() {
                        Ok(result) => result,
                        Err(e) => std::panic::resume_unwind(e),
                    };
                    match error {
                        Ok(()) => std::panic::resume_unwind(Box::new(error)), // something weird happened
                        Err(GzpError::Io(ioerr)) => ioerr,
                        Err(err) => io::Error::new(io::ErrorKind::Other, err),
                    }
                })?;
            self.buffer
                .reserve(self.buffer_size.saturating_sub(self.buffer.len()));
        }

        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_last(false)
    }
}
