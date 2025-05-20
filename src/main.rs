use anyhow::Result as AnyResult;
use memchr::memchr2;
use std::{
    env::args, fs::File, io::Write, os::unix::fs::FileExt, sync::mpsc, thread, time::Instant,
};

use ahash::AHashMap;

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
    let len = buffer.len() - CHUNK_EXCESS as usize;

    if file.read_exact_at(buffer, offset).is_err() {
        buffer.fill(0);
        file.read_at(buffer, offset).unwrap();
    }

    let start = (offset != 0) as usize * (memchr::memchr(b'\n', &buffer).unwrap() + 1);

    let mem = memchr2(b'\n', 0, &buffer[len..]).unwrap();
    let end = mem + len + 1;

    (start, end)
}

#[inline(always)]
fn fixed_point_parse(b: &[u8]) -> i32 {
    let mut res = 0;
    let mut sign = false;

    for &byte in b {
        match byte {
            b'-' => {
                sign = true;
            }
            b'0'..b':' => {
                res += res * 10 + (byte - b'0') as i32;
            }
            _ => {}
        }
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
    let mut map = AHashMap::<Key, Record>::with_capacity(512);
    let mut maps: Vec<AHashMap<Key, Record>> = vec![];

    for i in 0..DISPATCH_LOOPS {
        if (offset + CHUNK_SIZE * (i as u64)) >= file_len {
            break;
        }
        let (start, end) = read_chunk(&file, offset + (CHUNK_SIZE * i as u64), &mut buffer);
        maps.push(process_chunk_v2(&buffer[start..end]));
    }

    for l_map in maps {
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
