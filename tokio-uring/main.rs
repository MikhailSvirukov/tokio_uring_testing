use std::net::TcpListener;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

use tokio::task::JoinHandle;
use tokio::time::Instant;
use uring_bench::{BLOCK_SIZE, SOCKET_AMOUNT, create_writers};

fn main() {
    let listener = TcpListener::bind("0.0.0.0:12456").unwrap();
    std::thread::spawn(create_writers);
    let _ = std::thread::spawn(move || {
        let fds = prepare_sockets(listener);
        let start = tokio_uring::start(async move {
            let (handles, start) = create_read_futures(fds);
            for h in handles {
                h.await.unwrap();
            }
            start.elapsed()
        });
        println!(
            "Uring read blocks {} each from {} sockets in {:?}",
            BLOCK_SIZE, SOCKET_AMOUNT, start
        );
    })
    .join();
}

fn prepare_sockets(listener: TcpListener) -> Vec<RawFd> {
    let mut vec = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        let (sock, _) = listener.accept().unwrap();
        vec.push(sock.into_raw_fd());
    }
    vec
}

fn create_read_futures(vec: Vec<RawFd>) -> (Vec<JoinHandle<()>>, Instant) {
    let mut futures = Vec::with_capacity(SOCKET_AMOUNT);
    let mut res = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        res.push(vec![0u8; BLOCK_SIZE]);
    }
    let start = Instant::now();
    for (buf, fd) in res.into_iter().zip(vec) {
        futures.push(tokio_uring::spawn(async move {
            let stream = unsafe { tokio_uring::net::TcpStream::from_raw_fd(fd) };
            let mut pos = 0;
            let mut current_buf = buf;
            loop {
                let (n, new_buf) = stream.read(current_buf).await;
                pos += n.unwrap();
                current_buf = new_buf;
                if pos == BLOCK_SIZE {
                    break;
                }
            }
            assert_eq!(pos, BLOCK_SIZE);
        }));
    }
    (futures, start)
}
