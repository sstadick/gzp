# ⛓️gzp

<p align="center">
  <a href="https://github.com/sstadick/gzp/actions?query=workflow%3Aci"><img src="https://github.com/sstadick/gzp/workflows/ci/badge.svg" alt="Build Status"></a>
  <img src="https://img.shields.io/crates/l/gzp.svg" alt="license">
  <a href="https://crates.io/crates/gzp"><img src="https://img.shields.io/crates/v/gzp.svg?colorB=319e8c" alt="Version info"></a><br>
</p>

Multi-threaded encoding and decoding.

## Why?

This crate provides a near drop in replacement for `Write` that has will compress chunks of data in parallel and write
to an underlying writer in the same order that the bytes were handed to the writer. This allows for much faster
compression of data.

Additionally, this provides multi-threaded decompressors for Mgzip and BGZF formats.

### Supported Encodings:

- Gzip via [flate2](https://docs.rs/flate2/)
- Zlib via [flate2](https://docs.rs/flate2/)
- Raw Deflate via [flate2](https://docs.rs/flate2/)
- Snappy via [rust-snappy](https://docs.rs/snap)
- [BGZF](https://samtools.github.io/hts-specs/SAMv1.pdf) block compression format limited to 64 Kb blocks
- [Mgzip](https://pypi.org/project/mgzip/) block compression format with no block size limit

## Usage / Features

By default `gzp` has the `deflate_default` and `libdeflate` features enabled which brings in the best performing `zlib`
implementation as the backend for `flate2` as well as `libdeflater` for the block gzip formats.

### Examples

- Deflate default

```toml
[dependencies]
gzp = { version = "*" }
```

- Rust backend, this means that the `Zlib` format will not be available.

```toml
[dependencies]
gzp = { version = "*", default-features = false, features = ["deflate_rust"] }
```

- Snap only

```toml
[dependencies]
gzp = { version = "*", default-features = false, features = ["snap_default"] }
```

## Examples

Simple example

```rust
use std::{env, fs::File, io::Write};

use gzp::{deflate::Gzip, ZBuilder, ZWriter};

fn main() {
    let mut writer = vec![];
    // ZBuilder will return a trait object that transparent over `ParZ` or `SyncZ`
    let mut parz = ZBuilder::<Gzip, _>::new()
        .num_threads(0)
        .from_writer(writer);
    parz.write_all(b"This is a first test line\n").unwrap();
    parz.write_all(b"This is a second test line\n").unwrap();
    parz.finish().unwrap();
}
```

An updated version of [pgz](https://github.com/vorner/pgz).

```rust
use gzp::{
    ZWriter,
    deflate::Mgzip,
    par::{compress::{ParCompress, ParCompressBuilder}}
};
use std::io::{Read, Write};

fn main() {
    let chunksize = 64 * (1 << 10) * 2;

    let stdout = std::io::stdout();
    let mut writer: ParCompress<Mgzip> = ParCompressBuilder::new().from_writer(stdout);

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    let mut buffer = Vec::with_capacity(chunksize);
    loop {
        let mut limit = (&mut stdin).take(chunksize as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear();
    }
    writer.finish().unwrap();
}
```

Same thing but using Snappy instead.

```rust
use gzp::{parz::{ParZ, ParZBuilder}, snap::Snap};
use std::io::{Read, Write};

fn main() {
    let chunksize = 64 * (1 << 10) * 2;

    let stdout = std::io::stdout();
    let mut writer: ParZ<Snap> = ParZBuilder::new().from_writer(stdout);

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    let mut buffer = Vec::with_capacity(chunksize);
    loop {
        let mut limit = (&mut stdin).take(chunksize as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear();
    }
    writer.finish().unwrap();
}
```

## Acknowledgements

- Many of the ideas for this crate were directly inspired by [`pigz`](https://github.com/madler/pigz), including
  implementation details for some functions.

## Contributing

PRs are very welcome! Please run tests locally and ensure they are passing. May tests are ignored in CI because the CI
instances don't have enough threads to test them / are too slow.

```bash
cargo test --all-features && cargo test --all-features -- --ignored
```

Note that tests will take 30-60s.

## Future todos

- Pull in an adler crate to replace zlib impl (need one that can combine values, probably implement COMB from pigz).
- Add more metadata to the headers
- Return an auto-generated index for BGZF / Mgzip formats
- Try with https://docs.rs/lzzzz/0.8.0/lzzzz/lz4_hc/fn.compress.html

## Benchmarks

All benchmarks were run on the file in `./bench-data/shakespeare.txt` catted together 100 times which creates a rough
550Mb file.

The primary benchmark takeaway is that compression time decreases proportionately to the number of threads used.

![benchmarks](./violin.svg)
