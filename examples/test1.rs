#[cfg(feature = "deflate")]
mod example {
    use gzp::deflate::Gzip;
    use gzp::par::compress::{ParCompress, ParCompressBuilder};
    use gzp::ZWriter;
    use std::{env, fs::File, io::Write};

    pub fn main() {
        let file = env::args().skip(1).next().unwrap();
        let writer = File::create(file).unwrap();
        let mut parz: ParCompress<Gzip> = ParCompressBuilder::new().from_writer(writer);
        parz.write_all(b"This is a first test line\n").unwrap();
        parz.write_all(b"This is a second test line\n").unwrap();
        parz.finish().unwrap();
    }
}

#[cfg(not(feature = "deflate"))]
mod example {
    pub fn main() {}
}

fn main() {
    example::main()
}
