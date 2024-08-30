use anyhow::Result as AnyResult;
use memchr::memchr2;
use std::{env::args, fs::File, io::Write, os::unix::fs::FileExt, thread, time::Instant};

use ahash::AHashMap;

const CHUNK_SIZE: u64 = 96 * 1024 * 1024;

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
        file.read_at(buffer, offset).unwrap();
    }

    let start = if offset != 0 {
        memchr::memchr(b'\n', &buffer).unwrap() + 1
    } else {
        0
    };

    let mem = memchr2(b'\n', 0, &buffer[len..]).unwrap();
    let end = mem + len + 1;

    (start, end)
}

#[inline(always)]
fn fixed_point_parse(b: &[u8]) -> i32 {
    let mut res = 0;
    let mut sign = false;
    let mut pos = 1;

    for byte in b {
        match byte {
            b'-' => {
                sign = true;
            }
            b'0'..=b'9' => {
                res += pos * (*byte - b'0') as i32;
                pos *= 10;
            }
            _ => {}
        }
    }

    if sign {
        res = -res;
    }
    res
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

        prev_end = end+1;
    }
    bmap
}

fn main() -> AnyResult<()> {
    let mut args = args();
    _ = args.next();
    let path = args.next().expect("file not found");
    println!("found file: {}", path);

    let start = Instant::now();

    let file = File::open(path)?;

    let mut offset = 0;
    let mut idx = 0;
    let mut handles: Vec<thread::JoinHandle<AHashMap<[u8; 32], Record>>> = Vec::with_capacity(256);

    let creation_start = Instant::now();
    while offset < file.metadata()?.len() {
        let file_c = file.try_clone()?;
        let handle: thread::JoinHandle<AHashMap<Key, Record>> = thread::Builder::new().name(format!("idx:{idx}")).spawn(move || {
            let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
            let (start, end) = read_chunk(&file_c, offset, &mut buffer);
            process_chunk_v2(&buffer[start..end])
        }).unwrap();
        offset += CHUNK_SIZE;
        handles.push(handle);
        idx+=1;
    }
    let creation_finish = creation_start.elapsed();

    // let mut prev_end = 0;
    // while offset < file.metadata()?.len() {
    //     let mut buffer = vec![b'\n'; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
    //     let file_c = file.try_clone()?;
    //     let (start, end) = read_chunk(&file_c, offset, &mut buffer);
    //     println!(
    //         "s: {}, e: {}, d: {}, s_c: {}, e_c: {}",
    //         start + offset as usize,
    //         end + offset as usize,
    //         start + offset as usize - prev_end,
    //         char::from(buffer[start]),
    //         char::from(buffer[end])
    //     );
    //     prev_end = end + offset as usize;
    //     offset += CHUNK_SIZE;
    // }

    println!("handles:{}", handles.len());
    let awaiting_start = Instant::now();
    let mut map = AHashMap::<Key, Record>::with_capacity(512);
    for handle in handles {
        let handle_map = handle.join().unwrap();
        for (key, other) in handle_map {
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
