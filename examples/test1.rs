#[cfg(feature = "deflate")]
mod example {
    use std::{env, fs::File, io::Write};

    use gzp::deflate::Gzip;
    use gzp::parz::{ParZ, ParZBuilder};
    use gzp::ZWriter;

    pub fn main() {
        let file = env::args().skip(1).next().unwrap();
        let writer = File::create(file).unwrap();
        let mut parz: ParZ<Gzip> = ParZBuilder::new().from_writer(writer);
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
