use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uring_bench::{BLOCK_SIZE, SOCKET_AMOUNT, create_writers};

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:12456")
        .await
        .unwrap();

    let writer = std::thread::spawn(|| create_writers());
    let readers = tokio::spawn(async move {
        let sockets = prepare_readers(listener).await;
        let (tasks, start): (Vec<JoinHandle<()>>, Instant) = spawn_tasks(sockets);
        for task in tasks {
            task.await.unwrap();
        }
        start.elapsed()
    });
    writer.join().unwrap();
    let time = readers.await.unwrap();
    println!(
        "Tokio tasks read blocks {} each from {} sockets in {:?}",
        BLOCK_SIZE, SOCKET_AMOUNT, time
    );
}

async fn prepare_readers(listener: tokio::net::TcpListener) -> Vec<tokio::net::TcpStream> {
    let mut sockets = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        let (stream, _) = listener.accept().await.unwrap();
        sockets.push(stream);
    }
    sockets
}

fn spawn_tasks(streams: Vec<tokio::net::TcpStream>) -> (Vec<JoinHandle<()>>, Instant) {
    let blocks = vec![vec![0u8; BLOCK_SIZE]; SOCKET_AMOUNT];
    let mut futures = Vec::with_capacity(SOCKET_AMOUNT);
    let start = Instant::now();
    for (mut block, mut stream) in blocks.into_iter().zip(streams) {
        futures.push(tokio::spawn(async move {
            let r = stream.read_exact(block.as_mut_slice()).await.unwrap();
            assert_eq!(r, BLOCK_SIZE);
        }));
    }
    (futures, start)
}
