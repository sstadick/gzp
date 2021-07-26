use bytes::BytesMut;
use failure::Error;
use flate2::bufread::GzEncoder;
use flate2::Compression;
use futures::executor::block_on;
use std::io::Read;
use structopt::StructOpt;
use tokio::io::AsyncReadExt;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{self},
};

#[derive(StructOpt)]
struct Options {
    #[structopt(
        short = "j",
        long = "jobs",
        help = "How many parallel jobs should be run"
    )]
    jobs: Option<usize>,
    #[structopt(
        short = "c",
        long = "chunk-size",
        help = "The size of one chunk, before it gets processed",
        default_value = "33554432"
    )]
    chunk: usize,
    #[structopt(
        short = "q",
        long = "queue-size",
        help = "Maximum number of waiting chunks",
        default_value = "2"
    )]
    queue: usize,
    #[structopt(
        short = "O",
        long = "compression",
        help = "The compression level, 0-9",
        default_value = "3"
    )]
    compression: u32,
}

// fn read_input(mut sink: Sender<Vec<u8>>, chunk_size: usize) -> JoinHandle<Result<(), Error>> {
//     thread::spawn(move || -> Result<(), Error> {
//         let stdin_unlocked = io::stdin();
//         let mut stdin = stdin_unlocked.lock();
//         loop {
//             let mut limit = (&mut stdin).take(chunk_size as u64);
//             let mut buffer = Vec::with_capacity(chunk_size);
//             limit.read_to_end(&mut buffer)?;
//             if buffer.is_empty() {
//                 return Ok(()); // A real EOF
//             }
//             block_on(sink.send(buffer))?;
//         }
//     })
// }

// fn write_output(stream: Receiver<Vec<u8>>) -> JoinHandle<Result<(), Error>> {
//     thread::spawn(move || -> Result<(), Error> {
//         block_on(
//             stream
//                 // .map_err(|()| failure::err_msg("Error on channel"))
//                 .for_each(|chunk| {
//                     let stdout_unlocked = io::stdout();
//                     let mut stdout = stdout_unlocked.lock();
//                     stdout.write_all(&chunk).expect("bad"); //.map_err(Error::from)
//                     future::ready(())
//                 }),
//         );
//         Ok(())
//     })
// }

// fn compress(i: &[u8], level: Compression) -> Vec<u8> {
//     // Pre-allocate space for all the data, compression is likely to make it smaller.
//     let mut result = Vec::with_capacity(i.len());
//     let mut gz = GzEncoder::new(&i[..], level);
//     gz.read_to_end(&mut result).unwrap();
//     result
// }

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (in_sender, mut in_receiver) = mpsc::channel(2);
    let (out_sender, mut out_reciever) = mpsc::channel(32);

    // Notes:
    // - when using async reads, it doesn't fill the whole buffer like expected
    // - Need to make the compress block multithreaded / spawn a task - per
    // - Try going back to async reader, how to validate the output order

    // Reader Task
    let reader = tokio::task::spawn_blocking(move || {
        let chunksize = 33554432;
        let stdin = std::io::stdin();
        let mut stdin = stdin.lock();

        loop {
            let mut limit = (&mut stdin).take(chunksize as u64);
            let mut buffer = Vec::with_capacity(chunksize);
            limit.read_to_end(&mut buffer).unwrap();
            if buffer.is_empty() {
                break;
            }
            block_on(in_sender.send(buffer)).unwrap();
        }
    });

    // Compressor Task
    let compressor = tokio::task::spawn(async move {
        while let Some(chunk) = in_receiver.recv().await {
            let task = tokio::task::spawn_blocking(move || {
                let mut buffer = Vec::with_capacity(chunk.len());
                let mut gz: GzEncoder<&[u8]> = GzEncoder::new(&chunk, Compression::new(3));
                gz.read_to_end(&mut buffer).unwrap();
                buffer
            });
            out_sender.send(task).await.unwrap();
        }
    });

    // Writer Task
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(chunk) = out_reciever.recv().await {
            stdout.write_all(&chunk.await.unwrap()).await.unwrap();
        }
    });

    reader.await.unwrap();
    compressor.await.unwrap();
    writer.await.unwrap();
    Ok(())
}

// fn main() -> Result<(), Error> {
//     let options = Options::from_args();
//     let (in_sender, mut in_receiver) = mpsc::channel(options.queue);
//     let (out_sender, out_receiver) = mpsc::channel(32);
//     let in_thread = read_input(in_sender, options.chunk);
//     let out_thread = write_output(out_receiver);

//     let jobs = options.jobs;

//     let rt = runtime::Runtime::new()?;
//     let compression = options.compression;

//     rt.block_on(async {
//         for chunk in in_receiver.recv() {
//             out_sender.send(
//                 tokio::task::spawn(async { compress(&chunk, Compression::new(compression)) }).await,
//             );
//         }
//         // in_receiver.map(|chunk| {}).buffered(32).forward(out_sender)
//     });

//     // in_receiver
//     //     // .map_err(|()| failure::err_msg("Error on channel"))
//     //     .map(|chunk| threaded_rt.spawn(async { compress(&chunk, Compression::new(compression)) }))
//     //     .buffered(32)
//     //     .forward(out_sender);

//     out_thread.join().unwrap()?;
//     in_thread.join().unwrap()?;
//     Ok(())
// }
