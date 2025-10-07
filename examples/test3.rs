//! This example demonstrates how to use scoped threads with ParCompress.
#[cfg(feature = "deflate")]
mod example {
    use gzp::deflate::Gzip;
    use gzp::par::compress::ParCompressBuilder;
    use gzp::ZWriter;
    use std::io::Write;

    pub fn main() {
        let mut output = Vec::new();

        std::thread::scope(|scope| {
            let mut compressor =
                ParCompressBuilder::<Gzip>::new().from_borrowed_writer(&mut output, scope);
            compressor.write_all(b"Data to compress").unwrap();
            compressor.finish().unwrap()
        });
        println!("Compressed size: {}", output.len());
    }
}

#[cfg(not(feature = "deflate"))]
mod example {
    pub fn main() {}
}

fn main() {
    example::main()
}
