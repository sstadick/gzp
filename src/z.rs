//! Single thread compression.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate_default")] {
//! use gzp::{deflate::Gzip, parz::ParZ};
//!
//! let mut writer = vec![];
//! let mut z = Z::<Gzip, _>::builder(writer).num_threads(0).build();
//! z.write_all(b"This is a first test line\n").unwrap();
//! z.write_all(b"This is a second test line\n").unwrap();
//! z.finish().unwrap();
//! # }
//! ```

use std::fmt::Debug;
use std::io;
use std::io::Write;

use bytes::{Bytes, BytesMut};

use crate::check::Check;
use crate::parz::Compression;
use crate::{FormatSpec, GzpError, BUFSIZE, DICT_SIZE};

/// The [`Z`] builder
#[derive(Debug)]
pub struct ZBuilder<F, W>
where
    F: FormatSpec,
{
    /// The buffersize accumulate before trying to compress it. Defaults to [`BUFSIZE`].
    buffer_size: usize,
    /// The underlying writer to write to.
    writer: W,
    /// The compression level of the output, see [`Compression`].
    compression_level: Compression,
    /// The out file format to use.
    format: F,
}

impl<F, W> ZBuilder<F, W>
where
    F: FormatSpec,
    W: Send + Write + 'static,
{
    /// Create a new [`ZBuilder`] object.
    pub fn new(writer: W) -> Self {
        Self {
            buffer_size: BUFSIZE,
            writer,
            compression_level: Compression::new(3),
            format: F::new(),
        }
    }

    /// Set the [`buffer_size`](ZBuilder.buffer_size). Must be >= [`DICT_SIZE`].
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

    /// Set the [`compression_level`](ZBuilder.compression_level).
    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Create a configured [`ParZ`] object.
    ///
    /// Note, this writes the header to the writer.
    pub fn build(mut self) -> io::Result<Z<F, W>> {
        self.writer
            .write_all(&self.format.header(self.compression_level))?;
        Ok(Z {
            buffer: BytesMut::with_capacity(self.buffer_size),
            buffer_size: self.buffer_size,
            check: F::C::new(),
            compression_level: self.compression_level,
            dictionary: None,
            is_last: false,
            format: self.format,
            writer: self.writer,
        })
    }
}

#[allow(unused)]
pub struct Z<F, W>
where
    F: FormatSpec,
    W: Write + Send + 'static,
{
    buffer: BytesMut,
    buffer_size: usize,
    check: F::C,
    compression_level: Compression,
    dictionary: Option<Bytes>,
    is_last: bool,
    format: F,
    writer: W,
}

impl<F, W> Z<F, W>
where
    F: FormatSpec,
    W: Write + Send + 'static,
{
    pub fn builder(writer: W) -> ZBuilder<F, W>
    where
        W: Write + Send + 'static,
    {
        ZBuilder::new(writer)
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are written NOW.
    ///
    /// If this is the last buffer to be written, set `is_last` to false to trigger compression
    /// stream completion.
    pub fn flush_last(&mut self, is_last: bool) -> std::io::Result<()> {
        // Get all the bytes in the buffer
        let b = self.buffer.split().freeze();
        // Set the dictionary in case this isn't the last chunk
        if b.len() >= DICT_SIZE && !is_last && self.format.needs_dict() {
            self.dictionary = Some(b.slice(b.len() - DICT_SIZE..))
        }

        // Compress
        let buffer = self
            .format
            .encode(
                &b,
                self.compression_level,
                self.dictionary.as_ref(),
                is_last,
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.check.update(&b);

        // Write
        self.writer.write_all(&buffer)?;
        let footer = self.format.footer(&self.check);
        self.writer.write_all(&footer)
    }
    /// Flush the buffers and write the compression footer.
    ///
    /// This *MUST* be called before the [`Z`] object goes out of scope.
    ///
    /// # Errors
    /// - [`GzpError`] if there is an issue flushing the last blocks or an issue joining on the writer thread
    pub fn finish(mut self) -> Result<(), GzpError> {
        self.flush_last(true).map_err(|e| GzpError::Io(e))
    }
}

impl<F, W> Write for Z<F, W>
where
    F: FormatSpec,
    W: Write + Send + 'static,
{
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() > self.buffer_size {
            // Set up buffers
            let b = self.buffer.split_to(self.buffer_size).freeze();
            self.dictionary = if self.format.needs_dict() {
                Some(b.slice(b.len() - DICT_SIZE..))
            } else {
                None
            };

            // Compress
            let buffer = self
                .format
                .encode(&b, self.compression_level, self.dictionary.as_ref(), false)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.check.update(&b);

            // Write
            self.writer.write_all(&buffer)?;
        }
        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_last(false)
    }
}
