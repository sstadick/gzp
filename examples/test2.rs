#[cfg(feature = "pargz")]
mod example_pargz {
    use std::io::{Read, Write};

    use gzp::pargz::ParGz;

    pub fn main() {
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
        writer.finish().unwrap();
    }
}

#[cfg(feature = "parsnap")]
mod example_parsnap {
    use std::io::{Read, Write};

    use gzp::parsnap::ParSnap;

    pub fn main() {
        let chunksize = 64 * (1 << 10) * 2;

        let stdout = std::io::stdout();
        let mut writer = ParSnap::builder(stdout).build();

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

fn main() {
    if cfg!(feature = "parsnap") {
        example_parsnap::main()
    } else if cfg!(feature = "pargz") {
        example_pargz::main()
    }
}
