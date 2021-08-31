//! Single threaded compression that mimics the [`crate::par::compress::ParCompress`] API and implements
//! [`crate::ZWriter`].
use std::{
    io::{self, Write},
    marker::PhantomData,
};

use flate2::Compression;

use crate::{FormatSpec, SyncWriter};

/// Builder for [`SyncZ`] synchronous compressor.
pub struct SyncZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write,
{
    compression_level: Compression,
    format: PhantomData<F>,
    phantom: PhantomData<W>,
}

impl<F, W> SyncZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            compression_level: Compression::new(3),
            format: PhantomData,
            phantom: PhantomData,
        }
    }

    /// Set the compression level.
    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Create from a writer.
    pub fn from_writer(self, writer: W) -> SyncZ<F::OutputWriter> {
        SyncZ {
            inner: Some(F::sync_writer(writer, self.compression_level)),
        }
    }
}

impl<F, W> Default for SyncZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write,
{
    fn default() -> Self {
        Self::new()
    }
}

/// The single threaded writer.
pub struct SyncZ<W: Write> {
    pub(crate) inner: Option<W>,
}

impl<W> SyncZ<W>
where
    W: Write,
{
    /// Create a [`SyncZBuilder`].
    pub fn builder<InnerW: Write, F: FormatSpec + SyncWriter<InnerW>>() -> SyncZBuilder<F, InnerW> {
        SyncZBuilder::new()
    }
}

impl<W> Write for SyncZ<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.as_mut().unwrap().flush()
    }
}
