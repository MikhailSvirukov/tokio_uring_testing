use std::io::Read;
use std::net::TcpListener;
use std::thread::JoinHandle;
use std::time::Instant;

use uring_bench::{BLOCK_SIZE, SOCKET_AMOUNT, create_writers};

fn main() {
    let reader = std::thread::spawn(move || {
        let listener = TcpListener::bind("0.0.0.0:12456").unwrap();
        let streams = create_readers(listener);
        spawn_threads(streams)
    });

    std::thread::spawn(create_writers).join().unwrap();

    let (handles, start): (Vec<JoinHandle<()>>, Instant) = reader.join().unwrap();

    for h in handles {
        h.join().unwrap();
    }
    let time = start.elapsed();
    println!(
        "Threads read blocks {} each from {} sockets in {:?}",
        BLOCK_SIZE, SOCKET_AMOUNT, time
    );
}

fn create_readers(listener: TcpListener) -> Vec<std::net::TcpStream> {
    (0..SOCKET_AMOUNT)
        .map(|_| {
            let (sock, _) = listener.accept().unwrap();
            sock
        })
        .collect::<Vec<std::net::TcpStream>>()
}

fn spawn_threads(streams: Vec<std::net::TcpStream>) -> (Vec<JoinHandle<()>>, Instant) {
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(SOCKET_AMOUNT);
    let blocks = vec![vec![0u8; BLOCK_SIZE]; SOCKET_AMOUNT];
    let start = Instant::now();
    for (mut block, mut stream) in blocks.into_iter().zip(streams) {
        let handle = std::thread::spawn(move || {
            stream.read_exact(block.as_mut_slice()).unwrap();
        });
        handles.push(handle);
    }
    (handles, start)
}
