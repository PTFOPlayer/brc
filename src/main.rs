use anyhow::Result as AnyResult;
use std::{
    env::args, fs::File, io::Write, os::unix::fs::FileExt, sync::mpsc, thread, time::Instant,
};

use ahash::AHashMap;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const DISPATCH_LOOPS: usize = 128;

const CHUNK_SIZE: u64 = 1 * 1024 * 1024;

const CHUNK_EXCESS: u64 = 64;

struct Record {
    max: i32,
    min: i32,
    count: i32,
    sum: i32,
}

impl Record {
    #[inline(always)]
    fn new(value: i32) -> Self {
        Record {
            max: value,
            min: value,
            count: 1,
            sum: value,
        }
    }
}

fn read_chunk(file: &File, offset: u64, buffer: &mut [u8]) -> (usize, usize) {
    let total_len = buffer.len();
    let core_len = total_len - CHUNK_EXCESS as usize;

    if file.read_exact_at(buffer, offset).is_err() {
        match file.read_at(buffer, offset) {
            Ok(n) => buffer[n..].fill(0),
            Err(_) => {
                buffer.fill(0);
                return (0, 0);
            }
        }
    }

    let start = if offset != 0 {
        memchr::memchr(b'\n', buffer).map_or(0, |i| i + 1)
    } else {
        0
    };

    let end = match memchr::memchr2(b'\n', 0, &buffer[core_len..]) {
        Some(i) => core_len + i + 1,
        None => total_len,
    };

    (start, end)
}

#[inline(always)]
fn fixed_point_parse(b: &[u8]) -> i32 {
    let mut res: i32 = 0;
    let mut sign = false;

    let mut i = 0;

    if !b.is_empty() && b[0] == b'-' {
        sign = true;
        i += 1;
    }

    while i < b.len() {
        let byte = b[i];
        if byte >= b'0' && byte <= b'9' {
            res = res * 10 + ((byte - b'0') as i32);
        } else {
            break;
        }
        i += 1;
    }

    if sign {
        -res
    } else {
        res
    }
}

type Key = [u8; 32];

fn process_chunk_v2(buffer: &[u8]) -> AHashMap<Key, Record> {
    let mut bmap = AHashMap::<Key, Record>::with_capacity(512);

    let mut end = memchr::memchr2_iter(b';', b'\n', &buffer);
    let mut prev_end = 0;
    while let (Some(semi), Some(end)) = (end.next(), end.next()) {
        let mut arr = Key::default();
        arr[..semi - prev_end].copy_from_slice(&buffer[prev_end..semi]);

        let value = fixed_point_parse(&buffer[semi + 1..end]);

        if let Some(record) = bmap.get_mut(&arr) {
            record.count += 1;
            record.sum += value;
            record.max = record.max.max(value);
            record.min = record.min.min(value);
        } else {
            bmap.insert(arr, Record::new(value));
        }

        prev_end = end + 1;
    }
    bmap
}

fn dispatch(file: &File, offset: u64, file_len: u64) -> AHashMap<Key, Record> {
    let mut buffer = [0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
    let mut map = AHashMap::<Key, Record>::with_capacity(1024);

    for i in 0..DISPATCH_LOOPS {
        if (offset + CHUNK_SIZE * (i as u64)) >= file_len {
            break;
        }
        let (start, end) = read_chunk(&file, offset + (CHUNK_SIZE * i as u64), &mut buffer);
        let l_map = process_chunk_v2(&buffer[start..end]);

        for (key, other) in l_map {
            if let Some(record) = map.get_mut(&key) {
                record.count += other.count;
                record.sum += other.sum;
                record.max = record.max.max(other.max);
                record.min = record.min.min(other.min);
            } else {
                map.insert(key, other);
            }
        }
    }

    map
}

fn main() -> AnyResult<()> {
    let mut args = args();
    _ = args.next();
    let path = args.next().expect("file not found");
    println!("found file: {}", path);

    let file = File::open(path)?;

    let start = Instant::now();

    let mut offset = 0;
    let (tx, rx) = mpsc::channel();
    let mut parts = 0;
    let creation_start = Instant::now();
    let file_len = file.metadata()?.len();

    while offset < file_len {
        let file_c = file.try_clone()?;
        let tx = tx.clone();

        thread::spawn(move || {
            tx.send(dispatch(&file_c, offset, file_len)).unwrap();
        });

        offset += CHUNK_SIZE * DISPATCH_LOOPS as u64;
        parts += 1;
    }
    let creation_finish = creation_start.elapsed();

    println!("handles:{}", parts);
    let awaiting_start = Instant::now();
    let mut map = AHashMap::<Key, Record>::with_capacity(512);
    for _ in 0..parts {
        for (key, other) in rx.recv().unwrap().drain() {
            map.entry(key)
                .and_modify(|record| {
                    record.count += other.count;
                    record.sum += other.sum;
                    record.max = record.max.max(other.max);
                    record.min = record.min.min(other.min);
                })
                .or_insert(other);
        }
    }

    println!("Creation time: {:?}", creation_finish);
    println!("Awaiting time: {:?}", awaiting_start.elapsed());
    println!("Full time: {:?}", start.elapsed());

    let mut file = File::create("./out.txt")?;
    for record in map {
        let (name, rec) = (record.0, record.1);
        let name_buff: Key = unsafe { std::mem::transmute(name) };
        let line = format!(
            "{} Avg:{:.1}, Min:{}, Max:{}\n",
            String::from_utf8_lossy(&name_buff).trim_matches(char::from(0)),
            rec.sum as f32 / 10.0 / rec.count as f32,
            rec.min as f32 / 10.0,
            rec.max as f32 / 10.0
        );
        file.write(&line.as_bytes())?;
    }
    file.flush()?;

    Ok(())
}
