use std::net::TcpListener;
use std::os::fd::{FromRawFd, IntoRawFd};
use std::time::Instant;

use uring_bench::{BLOCK_SIZE, SOCKET_AMOUNT, create_writers};

fn main() {
    let listener = TcpListener::bind("0.0.0.0:12456").unwrap();
    std::thread::spawn(|| create_writers());
    let reader_thread = std::thread::spawn(move || {
        let fur = create_read_futures(listener);
        let time = tokio_uring::start(async move {
            let start = Instant::now();
            futures::future::join_all(fur).await;
            start.elapsed()
        });
        time
    });

    let duration = reader_thread.join().unwrap();
    println!(
        "Uring read blocks {} each from {} sockets in {:?}",
        BLOCK_SIZE, SOCKET_AMOUNT, duration
    );
}

fn create_read_futures(listener: TcpListener) -> Vec<impl Future<Output = ()> + Sized> {
    let mut futures = Vec::with_capacity(SOCKET_AMOUNT);
    let mut res = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        res.push(vec![0u8; BLOCK_SIZE]);
    }
    for buf in res {
        let (sock, _) = listener.accept().unwrap();
        let fd = sock.into_raw_fd();

        futures.push(async move {
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
        });
    }
    futures
}
