# ⛓️gzp

<p align="center">
  <a href="https://github.com/sstadick/gzp/actions?query=workflow%3Aci"><img src="https://github.com/sstadick/gzp/workflows/ci/badge.svg" alt="Build Status"></a>
  <img src="https://img.shields.io/crates/l/gzp.svg" alt="license">
  <a href="https://crates.io/crates/gzp"><img src="https://img.shields.io/crates/v/gzp.svg?colorB=319e8c" alt="Version info"></a><br>
</p>

Multithreaded encoding.

## Why?

This crate provides a near drop in replacement for `Write` that has will compress chunks of data in parallel and write
to an underlying writer in the same order that the bytes were handed to the writer. This allows for much faster
compression of data.

### Supported Encodings:

- Gzip via [flate2](https://docs.rs/flate2/)
- Snappy via [rust-snappy](https://docs.rs/snap)

## Usage / Features

There are no features enabled by default. `pargz_default` enables `pargz` and `flate2_default`. `flate2_default` in turn
uses the default backed for `flate2`, which, as of this writing is the pure rust backend. `parsnap_default` is just a
wrapper over `parsnap`, which pulls in the `snap` dependency.

The following demonstrate common ways to override the features:

Simple way to enable both `pargz` and `parsnap`:

```toml
[dependencies]
gzp = { version = "*", features = ["pargz_default", "parsnap_default"] }
```

Use `pargz` to pull in `flate2` and `zlib-ng-compat` to use the `zlib-ng-compat` backend for `flate2` (most performant).

```toml
[dependencies]
gzp = { version = "*", features = ["pargz", "zlib-ng-compat"] }
```

To use Snap (could also use `parsnap_default`):

```toml
[dependencies]
gzp = { version = "*", no_default_features = true, features = ["parsnap"] }
```

To use both Snap and Gzip with specific backend:

```toml
[dependencies]
gzp = { version = "*", no_default_features = true, features = ["parsnap_default", "pargz", "zlib-ng-compat"] }
```

## Examples

Simple example

```rust
use std::{env, fs::File, io::Write};

use gzp::pargz::ParGz;

fn main() {
    let file = env::args().skip(1).next().unwrap();
    let writer = File::create(file).unwrap();
    let mut par_gz = ParGz::builder(writer).build();
    par_gz.write_all(b"This is a first test line\n").unwrap();
    par_gz.write_all(b"This is a second test line\n").unwrap();
    par_gz.finish().unwrap();
}
```

An updated version of [pgz](https://github.com/vorner/pgz).

```rust
use gzp::pargz::ParGz;
use std::io::{Read, Write};

fn main() {
    let chunksize = 64 * (1 << 10) * 2;

    let stdout = std::io::stdout();
    let mut writer = ParGz::builder(stdout).build();

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
use gzp::parsnap::ParSnap;
use std::io::{Read, Write};

fn main() {
    let chunksize = 64 * (1 << 10) * 2;

    let stdout = std::io::stdout();
    let mut writer = ParSnap::builder(stdout).build();

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

## Notes

- Files written with this are just Gzipped blocks catted together and must be read
  with `flate2::bufread::MultiGzDecoder`.

## Future todos

- Explore removing `Bytes` in favor of raw vec
- Check that block is actually smaller than when it started
- Update the CRC value with each block written
- Add a BGZF mode + tabix index generation (or create that as its own crate)
- Try with https://docs.rs/lzzzz/0.8.0/lzzzz/lz4_hc/fn.compress.html

## Benchmarks

All benchmarks were run on the file in `./bench-data/shakespeare.txt` catted together 100 times which creats a rough
550Mb file.

| Name | Num Threads | Compression Level | Buffer Size | Time | File Size | | --- | - | ----------------- | -----------
| ---- | --------- | | Gzip Only | NA | 3 | 128 Kb | 6.6s | 218 Mb | | Gzip | 1 | 3 | 128 Kb | 2.4s | 223 Mb | | Gzip |
4 | 3 | 128 Kb | 1.2s | 223 Mb | | Gzip | 8 | 3 | 128 Kb | 0.8s | 223 Mb | | Gzip | 16 | 3 | 128 Kb | 0.6s | 223 Mb | |
Gzip | 30 | 3 | 128 Kb | 0.6s | 223 Mb | | Snap Only | NA | NA | 128 Kb | 1.6s | 333 Mb | | Snap | 1 | NA | 128 Kb |
0.7s | 333 Mb | | Snap | 4 | NA | 128 Kb | 0.5s | 333 Mb | | Snap | 8 | NA | 128 Kb | 0.4s | 333 Mb | | Snap | 16 | NA |
128 Kb | 0.4s | 333 Mb | | Snap | 30 | NA | 128 Kb | 0.4s | 333 Mb |

![benchmarks](./violin.svg)
