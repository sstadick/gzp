use par_gz::ParGz;
use std::io::{Read, Write};

fn main() {
    let chunksize = 64 * (1 << 10) * 2;

    let stdout = std::io::stdout();
    let mut writer = ParGz::builder(stdout).build();

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
    writer.close().unwrap();
}
