#[cfg(feature = "pargz")]
mod example {
    use std::{env, fs::File, io::Write};

    use gzp::pargz::ParGz;

    pub fn main() {
        let file = env::args().skip(1).next().unwrap();
        let writer = File::create(file).unwrap();
        let mut par_gz = ParGz::builder(writer).build();
        par_gz.write_all(b"This is a first test line\n").unwrap();
        par_gz.write_all(b"This is a second test line\n").unwrap();
        par_gz.finish().unwrap();
    }
}

#[cfg(not(feature = "pargz"))]
mod example {
    pub fn main() {}
}

fn main() {
    example::main()
}
