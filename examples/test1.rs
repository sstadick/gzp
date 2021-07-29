use std::{env, fs::File, io::Write};

use par_gz::ParGz;
fn main() {
    let file = env::args().skip(1).next().unwrap();
    let writer = File::create(file).unwrap();
    let mut par_gz = ParGz::new(writer);
    par_gz.write_all(b"This is a first test line\n").unwrap();
    par_gz.write_all(b"This is a second test line\n").unwrap();
    par_gz.close().unwrap();
}
