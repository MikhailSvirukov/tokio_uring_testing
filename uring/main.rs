use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use io_uring::squeue::Entry;
use io_uring::{IoUring, opcode, types};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use uring_bench::{BLOCK_SIZE, SOCKET_AMOUNT, create_writers};

#[tokio::main]
async fn main() {
    let writer = std::thread::spawn(|| {
        create_writers();
    });
    let vec = create_readers().await;
    let (sender, recv) = tokio::sync::mpsc::unbounded_channel();
    let tasks = tokio::spawn(async move { start_tasks(vec, sender).await });
    let uring_task = tokio::spawn(async move { start_uring(recv).await });
    let _ = tokio::join!(uring_task, tasks);
    writer.join().unwrap();
}

async fn create_readers() -> Vec<tokio::net::TcpStream> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:12456")
        .await
        .unwrap();
    let mut vec = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        let (sock, _) = listener.accept().await.unwrap();
        vec.push(sock);
    }
    vec
}

async fn start_uring(mut recv: UnboundedReceiver<(Entry, oneshot::Sender<usize>)>) {
    let mut ring = IoUring::new(SOCKET_AMOUNT as u32).unwrap();
    let mut wait_answers = HashMap::with_capacity(SOCKET_AMOUNT);
    let mut numb = 0;
    loop {
        let mut interval = tokio::time::interval(Duration::from_millis(2));
        tokio::select! {
            result = recv.recv() =>
            {
                 match result {
                    Some((entry, reply)) => {
                        let entry = entry.user_data(numb);
                        wait_answers.insert(numb, reply);
                        numb += 1;
                        unsafe {
                            ring.submission().push(&entry).unwrap();
                        }
                        if ring.submission().len() >= SOCKET_AMOUNT/4 {
                            ring.submit().unwrap();
                        }
                    }
                    None => {
                        ring.submit().unwrap();
                        for result in ring.completion() {
                            let numb = result.user_data();
                            let send = wait_answers.remove(&numb).unwrap();
                            send.send(result.result() as usize).unwrap();
                        }
                        return
                    }
                }
            },
            _ = interval.tick() => {
                ring.submit().unwrap();
            }
        }

        //assert_eq!(res, SOCKET_AMOUNT);
        for result in ring.completion() {
            let numb = result.user_data();
            let send = wait_answers.remove(&numb).unwrap();
            send.send(result.result() as usize).unwrap();
        }
        assert!(ring.completion().is_empty())
    }
}

async fn start_tasks(
    streams: Vec<tokio::net::TcpStream>,
    sender: UnboundedSender<(Entry, oneshot::Sender<usize>)>,
) {
    let blocks = vec![vec![0u8; BLOCK_SIZE]; SOCKET_AMOUNT];
    let mut vec = Vec::with_capacity(SOCKET_AMOUNT);
    let start = Instant::now();
    for (mut block, stream) in blocks.into_iter().zip(streams) {
        let value = sender.clone();
        vec.push(tokio::spawn(async move {
            let mut amount = 0;
            loop {
                let read = opcode::Read::new(
                    types::Fd(stream.as_raw_fd()),
                    block[amount..].as_mut_ptr(),
                    BLOCK_SIZE as u32,
                )
                .build();
                let (send, recv) = oneshot::channel::<usize>();
                value.send((read, send)).unwrap();
                let res = recv.await.unwrap();
                amount += res;
                if amount == BLOCK_SIZE {
                    break;
                }
            }
        }))
    }
    for handl in vec {
        handl.await.unwrap();
    }
    let finish = start.elapsed();
    println!(
        "io-uring finished reading {SOCKET_AMOUNT} sockets {BLOCK_SIZE} bytes each in {finish:?}"
    )
}
