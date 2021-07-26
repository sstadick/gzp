use std::{
    io::Read,
    thread::{self, JoinHandle},
};

use bytes::Bytes;
use flate2::{bufread::GzEncoder, Compression};
use tokio::sync::mpsc::{self, Receiver};

pub struct Connection {
    handle: JoinHandle<()>,
}

impl Connection {
    pub fn new_conn() {
        let (tx, rx) = mpsc::channel(32);
        let runner = Runner::new(rx);
        // Spawn on another thread, give back a channel to send things
        let handle = thread::spawn(move || runner.run());
    }

    // TODO: close it down
}

struct Runner {
    rx: Receiver<&'static [u8]>,
}

impl Runner {
    fn new(rx: Receiver<&'static [u8]>) -> Self {
        Runner { rx }
    }

    fn compress(i: &[u8], level: Compression) -> Vec<u8> {
        // Pre-allocate space for all the data, compression is likely to make it smaller.
        let mut result = Vec::with_capacity(i.len());
        let mut gz = GzEncoder::new(&i[..], level);
        gz.read_to_end(&mut result).unwrap();
        result
    }

    async fn run(&mut self) {
        while let Some(chunk) = self.rx.recv().await {
            let task =
                tokio::task::spawn(async move { Self::compress(chunk, Compression::new(3)) });
        }
    }
}
