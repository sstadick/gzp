# v0.6.0

This release brings performance improvements across the board, but especially for for resource restricted systems.

Driven by [PR12](https://github.com/sstadick/gzp/pull/12) / [Issue11](https://github.com/sstadick/gzp/issues/11)

- Change backend to use thread-per-core compressors, dropping rayon
- Change meaning of `num_threads` to mean "number of compression threads", which allows for oversubscribing the writer thread since it spends most of its time idle. 
- Added errors for misconfigurations of number of threads / buffer size
- Bugfix to correctly set the compression dictionary in the event of an early call to flush
- Improved docs