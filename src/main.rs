use anyhow::Result as AnyResult;
use std::{
    collections::BTreeMap, env::args, fs::File, io::Write, os::unix::fs::FileExt, thread,
    time::Instant,
};

const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

const CHUNK_EXCESS: u64 = 64;

struct Record {
    max: f32,
    min: f32,
    count: u32,
    sum: f32,
}

impl Record {
    #[inline(always)]
    fn new(value: f32) -> Self {
        Record {
            max: value,
            min: value,
            count: 1,
            sum: value,
        }
    }
}

type Key = (u64, u64);

fn read_chunk(file: &File, offset: u64) -> Vec<u8> {
    let mut buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
    let len = buffer.len() - CHUNK_EXCESS as usize;
    file.read_exact_at(buffer.as_mut(), offset).unwrap();

    let mut start = 0 as usize;
    if offset != 0 {
        while buffer[start] != b'\n' {
            start += 1;
        }
        start += 1;
    }
    let mut end = len - 1;
    while buffer[end] != b'\n' {
        end += 1;
    }
    buffer[start..end].to_vec()
}

#[inline(always)]
fn s_parse(b: &[u8]) -> f32 {
    let mut f: f32 = 0.0;
    let mut sign: f32 = 1.0;
    let mut seen_dot = false;
    let mut pos = 1.0f32;

    for byte in b {
        let byte = *byte;
        match byte {
            b'-' => {
                sign = -1.0;
            }
            b'.' => {
                seen_dot = true;
                pos = 0.1;
            }
            _ => {
                f += sign * pos * (byte - 48) as f32;

                if !seen_dot {
                    pos *= 10.0;
                } else {
                    pos /= 10.0;
                }
            }
        }
    }

    f
}

fn process_chunk_v2(buffer: Vec<u8>) -> BTreeMap<Key, Record> {
    let mut bmap = BTreeMap::<Key, Record>::new();
    let mut line_ind = 0usize;
    let len = buffer.len();
    while line_ind < len {
        let mut end = line_ind;
        while end < len && buffer[end] != b'\n' {
            end += 1;
        }
        let mut semi = end - 1;
        while buffer[semi] != b';' {
            semi -= 1;
        }

        let mut arr = [0u8; 16];
        for i in line_ind..(line_ind + 16).min(semi) {
            arr[i - line_ind] = buffer[i];
        }

        let key = unsafe { std::mem::transmute(arr) };
        let value = s_parse(&buffer[semi + 1..end]);

        if let Some(record) = bmap.get_mut(&key) {
            record.count += 1;
            record.sum += value;
            record.max = record.max.max(value);
            record.min = record.min.min(value);
        } else {
            bmap.insert(key, Record::new(value));
        }

        line_ind = end + 1;
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
    let mut handles = Vec::with_capacity(512);

    let creation_start = Instant::now();
    while offset < file.metadata()?.len() - CHUNK_SIZE {
        let file_c = file.try_clone()?;
        let handle: thread::JoinHandle<BTreeMap<Key, Record>> =
            thread::spawn(move || process_chunk_v2(read_chunk(&file_c, offset)));
        offset += CHUNK_SIZE;
        handles.push(handle);
    }
    let creation_finish = creation_start.elapsed();

    println!("handles:{}", handles.len());
    let awaiting_start = Instant::now();
    let mut map = BTreeMap::<Key, Record>::new();
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
        let name_buff: [u8; 16] = unsafe { std::mem::transmute(name) };
        let line = format!(
            "{} Avg:{:.1}, Min:{}, Max:{}\n",
            String::from_utf8_lossy(&name_buff).trim().trim_matches(char::from(0)),
            rec.sum / rec.count as f32,
            rec.min,
            rec.max
        );
        file.write(&line.as_bytes())?;
    }
    file.flush()?;

    Ok(())
}
