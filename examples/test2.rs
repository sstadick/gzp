#[cfg(feature = "deflate")]
mod example {
    use std::io::{Read, Write};

    use gzp::{
        deflate::Gzip,
        par::compress::{ParCompress, ParCompressBuilder},
        Compression, ZWriter,
    };

    pub fn main() {
        let chunksize = 64 * (1 << 10) * 2;

        let stdout = std::io::stdout();
        let mut writer: ParCompress<Gzip> = ParCompressBuilder::new()
            .compression_level(Compression::new(6))
            .from_writer(stdout);

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
}

#[cfg(not(feature = "deflate"))]
mod example {
    pub fn main() {}
}

fn main() {
    example::main()
}
