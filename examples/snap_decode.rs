#[cfg(feature = "parsnap")]
mod example {
    use std::io;

    pub fn main() {
        let stdin = io::stdin();
        let stdout = io::stdout();

        let mut rdr = snap::read::FrameDecoder::new(stdin.lock());
        let mut wtr = stdout.lock();
        io::copy(&mut rdr, &mut wtr).expect("I/O operation failed");
    }
}

#[cfg(not(feature = "parsnap"))]
mod example {
    pub fn main() {}
}

fn main() {
    example::main()
}
