use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flate2::write::GzEncoder;
use gzp::{Compression, ParGz};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use tempfile::tempdir;

fn compress_with_gzp(num_threads: usize, buffer_size: usize, compression_level: u32) {
    let dir = tempdir().unwrap();
    let output_file = File::create(dir.path().join("shakespeare_gzp.txt.gz")).unwrap();
    let mut writer = ParGz::builder(output_file)
        .num_threads(num_threads)
        .compression_level(Compression::new(compression_level))
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

fn compress_with_flate2(buffer_size: usize, compression_level: u32) {
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

fn compress_with_snap(buffer_size: usize) {
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
            BenchmarkId::from_parameter(num_cpus),
            &num_cpus,
            |b, &num_cpus| {
                b.iter(|| compress_with_gzp(num_cpus, buffersize, compression_level));
            },
        );
    }

    group.bench_function("Flate2", |b| {
        b.iter(|| compress_with_flate2(buffersize, compression_level))
    });

    group.bench_function("Snap", |b| {
        b.iter(|| compress_with_flate2(buffersize, compression_level))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
