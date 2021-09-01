//! Wrappers around different check types.
//!
//! A `Check` simply computes the checksum for a block and keeps track of the number of
//! bytes that went into that checksum.
//!
//! The [`flate2::Crc`] implementation is used for Crc32 with a thin wrapper, and for
//! the Adler Check the `ader32*` family of functions from zlib is wrapped.
//!
//! Additionally, there is a passthrough check to allow for compressions types that
//! can bypass this check.
#[cfg(feature = "deflate")]
use flate2::Crc;
#[cfg(feature = "any_zlib")]
use libz_sys::{uInt, uLong, z_off_t};

pub trait Check {
    /// Current checksum
    fn sum(&self) -> u32;

    /// Amount input to the check
    fn amount(&self) -> u32;

    /// Create a new [`Check`] Object
    fn new() -> Self
    where
        Self: Sized;

    /// Update the [`Check`] object with more bytes.
    fn update(&mut self, bytes: &[u8]);

    /// Combine two like [`Check`] objects.
    fn combine(&mut self, other: &Self)
    where
        Self: Sized;
}

/// LibDeflates impl of CRC, this does not implement `combine`
#[cfg(feature = "libdeflate")]
pub struct LibDeflateCrc {
    crc: libdeflater::Crc,
    amount: u32,
}

#[cfg(feature = "libdeflate")]
impl Check for LibDeflateCrc {
    #[inline]
    fn sum(&self) -> u32 {
        self.crc.sum()
    }

    #[inline]
    fn amount(&self) -> u32 {
        self.amount
    }

    #[inline]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            crc: libdeflater::Crc::new(),
            amount: 0,
        }
    }

    #[inline]
    fn update(&mut self, bytes: &[u8]) {
        self.amount += bytes.len() as u32;
        self.crc.update(bytes);
    }

    /// Not needed for or implemented for libdeflate.
    ///
    /// Calling this is an error.
    fn combine(&mut self, _other: &Self)
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

/// The adler32 check implementation for zlib
#[cfg(feature = "any_zlib")]
pub struct Adler32 {
    sum: u32,
    amount: u32,
}

#[cfg(feature = "any_zlib")]
impl Check for Adler32 {
    #[inline]
    fn sum(&self) -> u32 {
        self.sum
    }

    #[inline]
    fn amount(&self) -> u32 {
        self.amount
    }

    #[inline]
    fn new() -> Self {
        let start = unsafe { libz_sys::adler32(0, std::ptr::null_mut(), 0) } as u32;

        Self {
            sum: start,
            amount: 0,
        }
    }

    #[inline]
    fn update(&mut self, bytes: &[u8]) {
        // TODO: safer cast(s)?
        self.amount += bytes.len() as u32;
        self.sum = unsafe {
            libz_sys::adler32(
                self.sum as uLong,
                bytes.as_ptr() as *mut _,
                bytes.len() as uInt,
            )
        } as u32;
    }

    #[inline]
    fn combine(&mut self, other: &Self) {
        self.sum = unsafe {
            libz_sys::adler32_combine(
                self.sum as uLong,
                other.sum as uLong,
                other.amount as z_off_t,
            )
        } as u32;
        self.amount += other.amount;
    }
}

/// The crc32 check implementation for Gzip
#[cfg(feature = "deflate")]
pub struct Crc32 {
    crc: Crc,
}

#[cfg(feature = "deflate")]
impl Check for Crc32 {
    #[inline]
    fn sum(&self) -> u32 {
        self.crc.sum()
    }

    #[inline]
    fn amount(&self) -> u32 {
        self.crc.amount()
    }

    #[inline]
    fn new() -> Self {
        let crc = flate2::Crc::new();
        Self { crc }
    }

    #[inline]
    fn update(&mut self, bytes: &[u8]) {
        self.crc.update(bytes);
    }

    #[inline]
    fn combine(&mut self, other: &Self) {
        self.crc.combine(&other.crc);
    }
}

/// A passthrough check object that performs no calculations and no-ops all calls.
pub struct PassThroughCheck {}

#[allow(unused)]
impl Check for PassThroughCheck {
    #[inline]
    fn sum(&self) -> u32 {
        0
    }

    #[inline]
    fn amount(&self) -> u32 {
        0
    }

    #[inline]
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {}
    }

    #[inline]
    fn update(&mut self, bytes: &[u8]) {}

    #[inline]
    fn combine(&mut self, other: &Self)
    where
        Self: Sized,
    {
    }
}
