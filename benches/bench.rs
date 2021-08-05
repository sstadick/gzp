use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::write::GzEncoder;
use gzp::{Compression, Encoder, ParGz};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use tempfile::tempdir;

fn compress_with_gzip(num_threads: usize, buffer_size: usize, compression_level: u32) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_gzip.txt.gz")).unwrap();
    let mut writer = ParGz::builder(output_file)
        .num_threads(num_threads)
        .encoder(Encoder::Gzip {
            compression_level: Compression::new(compression_level),
        })
        .build();
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.finish().unwrap();
}

fn compress_with_zlib(num_threads: usize, buffer_size: usize, compression_level: u32) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_gzip.txt.gz")).unwrap();
    let mut writer = ParGz::builder(output_file)
        .num_threads(num_threads)
        .encoder(Encoder::Zlib {
            compression_level: Compression::new(compression_level),
        })
        .build();
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.finish().unwrap();
}

fn compress_with_deflate(num_threads: usize, buffer_size: usize, compression_level: u32) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_gzip.txt.gz")).unwrap();
    let mut writer = ParGz::builder(output_file)
        .num_threads(num_threads)
        .encoder(Encoder::Deflate {
            compression_level: Compression::new(compression_level),
        })
        .build();
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.finish().unwrap();
}

fn compress_with_snap(num_threads: usize, buffer_size: usize) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_gzip.txt.gz")).unwrap();
    let mut writer = ParGz::builder(output_file)
        .num_threads(num_threads)
        .encoder(Encoder::Snap)
        .build();
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.finish().unwrap();
}

fn compress_with_gzip_only(buffer_size: usize, compression_level: u32) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_flate2.txt.gz")).unwrap();
    let mut writer = GzEncoder::new(output_file, Compression::new(compression_level));
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.finish().unwrap();
}

fn compress_with_snap_only(buffer_size: usize) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_snap.txt.gz")).unwrap();
    let mut writer = snap::write::FrameEncoder::new(output_file);
    let mut reader = BufReader::new(File::open("./bench-data/shakespeare.txt").unwrap());

    let mut buffer = Vec::with_capacity(buffer_size);
    loop {
        let mut limit = (&mut reader).take(buffer_size as u64);
        limit.read_to_end(&mut buffer).unwrap();
        if buffer.is_empty() {
            break;
        }
        writer.write_all(&buffer).unwrap();
        buffer.clear()
    }

    writer.flush().unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let buffersize = 64 * (1 << 10);
    let compression_level = 3;
    let mut group = c.benchmark_group("Compression");
    for num_cpus in [1usize, 4, 8, 12] {
        group.bench_with_input(
            BenchmarkId::new("Gzip", num_cpus),
            &num_cpus,
            |b, &num_cpus| {
                b.iter(|| compress_with_gzip(num_cpus, buffersize, compression_level));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("Zlib", num_cpus),
            &num_cpus,
            |b, &num_cpus| {
                b.iter(|| compress_with_zlib(num_cpus, buffersize, compression_level));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("Deflate", num_cpus),
            &num_cpus,
            |b, &num_cpus| {
                b.iter(|| compress_with_deflate(num_cpus, buffersize, compression_level));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("Snap", num_cpus),
            &num_cpus,
            |b, &num_cpus| {
                b.iter(|| compress_with_snap(num_cpus, buffersize));
            },
        );
    }

    group.bench_function("Gzip Only", |b| {
        b.iter(|| compress_with_gzip_only(buffersize, compression_level))
    });

    group.bench_function("Snap Only", |b| {
        b.iter(|| compress_with_snap_only(buffersize))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
