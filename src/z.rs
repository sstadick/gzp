use std::{
    io::{self, Write},
    marker::PhantomData,
};

use flate2::Compression;

use crate::{FormatSpec, SyncWriter};

/// Builder for [`Z`] synchronous compressor.
pub struct ZBuilder<F, W>
where
    F: FormatSpec + SyncWriter<W>,
    W: Write,
{
    compression_level: Compression,
    format: PhantomData<F>,
    phantom: PhantomData<W>,
}

impl<F, W> ZBuilder<F, W>
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
    pub fn compression_leval(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Create from a writer.
    pub fn from_writer(self, writer: W) -> Z<F::OutputWriter> {
        Z {
            inner: Some(F::sync_writer(writer, self.compression_level)),
        }
    }
}

pub struct Z<W: Write> {
    pub(crate) inner: Option<W>,
}

impl<W> Z<W>
where
    W: Write,
{
    pub fn builder<InnerW: Write, F: FormatSpec + SyncWriter<InnerW>>() -> ZBuilder<F, InnerW> {
        ZBuilder::new()
    }
}

impl<W> Write for Z<W>
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
