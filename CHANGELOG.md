# v0.10.1

- This release makes bgzf, mgzip, and the `FooterValues.sum` value public

# v0.10.0

- Drop support for cloudflare zlib via flate2 to resolve conflicting symbols

# v0.9.6

- Update deps, specifically flate2

# v0.9.5

- Fix regression in v0.9.3 that failed to add EOF block for BGZF files. EOF is now properly added.

# v0.9.4

- Yanked - intended to included changes from v0.9.5 but was actually just v0.9.3 again.

# v0.9.3

- Fix [gzp issue 30](https://github.com/sstadick/gzp/issues/30) where compression will fail for BGZF on a final call to `flush` when the remaining bytes are > the the max allowed bytes by that format. Additionally this will now keep the `Writer` buffer at less than `buffer_size` in the even that very large calls to write are happening. Previously this would just accumulate a very large buffer until flush was called, potentially leading to out-of-memory errors.

# v0.9.2

- Fix bug with BGZF format flush that would send blocks of size > max block size and return an unknown error.

# v0.9.1

- mgzip and bgzf formats were relying on libdeflater features even when under the `not(libdeflate)` config, this is now fixed

# v0.9.0

- Fix [crabz issue 9](https://github.com/sstadick/crabz/issues/11)             
- Add ability to pin compression and decompression threads to specific cpus
- Added Single threaded decompression for Bgzf and Mgzip formats backed by libdeflate

# v0.8.2

- Fix [crabz issue 9](https://github.com/sstadick/crabz/issues/9) which is that when 0 or 1 threads are used to compress block formats a `GzEncoder` was used instead of the correspoding single-threaded block encoder.

# v0.8.1

- Allow buffer size to be configured on `ZBuilder`
- Add `maybe_*` methods to `DecompressBuilder` to transparently support single threaded decompression
- Fix panic on drop for decompressor when an error occurs elsewhere in program
- Fix panic on drop for compressor when an error occurs elsewhere in program

See [crabz issue](https://github.com/sstadick/crabz/issues/7)
and [PR](https://github.com/sstadick/gzp/pull/16)

# v0.8.0

- Adds support for Mgzip and BGZF compression and decompression
- Large reorg of internal structure
- Modest performance improvements reusing decompressors / compressors

See [PR15](https://github.com/sstadick/gzp/pull/15)

# v0.7.2

- Fix snap feature flags

# v0.7.1

- Handle errors coming from internal writer transparently so that the correct error type is returned to the caller.
  - Specifically broken pipes can now be handled the same way they are for anything that implements `Write`
- Added tests so make sure dropping the writer correctly shuts things down

# v0.7.0

This release adds the `SyncZ` type as well as many API changes.
The cumulative result is that with this release `ZBuilder` can be used to return a `Box<dyn ZWriter>` trait object that will use `ParZ` if `num_threads > 1`, otherwise it will fall back to using `SyncZ`.
This allows calling code to use `gzp` regardless of the number of threads which could likely be 0 in some cases.

See [PR13](https://github.com/sstadick/gzp/pull/13).

# v0.6.0

This release brings performance improvements across the board, but especially for for resource restricted systems.

Driven by [PR12](https://github.com/sstadick/gzp/pull/12) / [Issue11](https://github.com/sstadick/gzp/issues/11)

- Change backend to use thread-per-core compressors, dropping rayon
- Change meaning of `num_threads` to mean "number of compression threads", which allows for oversubscribing the writer thread since it spends most of its time idle. 
- Added errors for misconfigurations of number of threads / buffer size
- Bugfix to correctly set the compression dictionary in the event of an early call to flush
- Improved docs
