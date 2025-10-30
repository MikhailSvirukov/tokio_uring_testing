use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::FileExt;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{env, fs, mem};

use io_uring::{IoUring, opcode, types};

fn main() {
    let vec = set_up_files();
    read_files_with_ring(vec.clone());

    sleep(Duration::new(5, 0));

    read_with_continuously(vec.clone());

    sleep(Duration::new(5, 0));

    read_with_threads(vec);

    clear();
}

const FILE_SIZE: usize = 1024;
const AMOUNT: usize = 1000;
fn set_up_files() -> Vec<RawFd> {
    env::set_current_dir("/home/mihailsvirukov/garbage").unwrap();
    let buf: &'static mut [u8] = vec![b'a'; FILE_SIZE].leak();
    let mut files = Vec::with_capacity(AMOUNT);
    for i in 0..AMOUNT {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(i.to_string())
            .unwrap();
        file.write_all(buf).unwrap();
        files.push(file.as_raw_fd());
        mem::forget(file)
    }
    files
}

fn clear() {
    for i in 0..AMOUNT {
        fs::remove_file(format!("/home/mihailsvirukov/garbage/{i}")).unwrap()
    }
}

fn read_files_with_ring(files: Vec<RawFd>) {
    let mut buffers = vec![vec![0u8; FILE_SIZE]; AMOUNT];
    let mut read = vec![0usize; AMOUNT];
    let mut ring = IoUring::new(AMOUNT as u32).unwrap();
    let start = Instant::now();
    loop {
        let mut changed = 0;
        for i in 0..AMOUNT {
            if read[i] < FILE_SIZE {
                changed += 1;
                let entry = opcode::Read::new(
                    types::Fd(files[i]),
                    buffers[i][read[i]..].as_mut_ptr(),
                    (FILE_SIZE - read[i]) as u32,
                )
                .build()
                .user_data(i as u64);
                unsafe {
                    ring.submission().push(&entry).unwrap();
                }
            }
        }
        if ring.submission().is_empty() {
            break;
        }
        ring.submit_and_wait(changed).unwrap();
        while let Some(entry) = ring.completion().next() {
            let result = entry.result() as usize;
            let index = entry.user_data();
            assert!(result > 0);
            read[index as usize] += result;
        }
        assert!(ring.completion().is_empty());
    }
    println!(
        "Uring: Read {AMOUNT} files {FILE_SIZE} each in {:?}",
        start.elapsed()
    );
}

fn read_with_continuously(files: Vec<RawFd>) {
    let buffers = vec![vec![0u8; FILE_SIZE]; AMOUNT];
    let start = Instant::now();
    for (fd, mut buf) in files.into_iter().zip(buffers) {
        let file = unsafe { File::from_raw_fd(fd) };
        file.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        mem::forget(file);
    }
    println!(
        "Continuously: Read {AMOUNT} files {FILE_SIZE} each in {:?}",
        start.elapsed()
    );
}

fn read_with_threads(files: Vec<RawFd>) {
    let mut threads = Vec::with_capacity(AMOUNT);
    let buffers = vec![vec![0u8; FILE_SIZE]; AMOUNT];
    let start = Instant::now();
    for (fd, mut buf) in files.into_iter().zip(buffers) {
        threads.push(std::thread::spawn(move || {
            let file = unsafe { File::from_raw_fd(fd) };
            file.read_exact_at(buf.as_mut_slice(), 0).unwrap();
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    println!(
        "Threads: Read {AMOUNT} files {FILE_SIZE} each in {:?}",
        start.elapsed()
    );
}
