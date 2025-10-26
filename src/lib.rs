use std::io::Write;
use std::thread::JoinHandle;

pub const SOCKET_AMOUNT: usize = 100;
pub const BLOCK_SIZE: usize = 1024 * 1024 * 256;

pub fn create_writers() -> Vec<JoinHandle<()>> {
    let block: &'static [u8] = vec![7u8; BLOCK_SIZE].leak();
    let mut vec = Vec::with_capacity(SOCKET_AMOUNT);
    for _ in 0..SOCKET_AMOUNT {
        let mut stream = std::net::TcpStream::connect("127.0.0.1:12456").unwrap();
        vec.push(std::thread::spawn(move || {
            let _ = stream.write_all(block);
        }));
    }
    vec
}
