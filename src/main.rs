use anyhow::Result as AnyResult;
use std::{env::args, fs::File, io::Write, os::unix::fs::FileExt, thread, time::Instant};

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
struct MapRecord(pub Key, pub Record);

impl MapRecord {
    #[inline(always)]
    fn new(hash: Key, value: f32) -> Self {
        Self(hash, Record::new(value))
    }

    #[inline(always)]
    fn update(&mut self, other: Self) {
        let s = &mut self.1;
        let o = other.1;
        s.count += o.count;
        s.sum += o.sum;
        s.max = s.max.max(o.max);
        s.min = s.min.min(o.min);
    }
}

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
fn put_in_map(map: &mut Vec<MapRecord>, new: MapRecord) {
    for record in &mut *map {
        if record.0 == new.0 {
            record.update(new);
            return;
        }
    }
    map.push(new);
}
#[inline(always)]
fn s_parse(b: &[u8]) -> f32 {
    let len = b.len();

    let mut f: f32 = 0.0;
    let mut sign: f32 = 1.0;

    let mut iter = 0usize;
    if b[0] == b'-' {
        iter += 1;
        sign = -1.0;
    }

    while iter < len && b[iter] != b'.' {
        f += sign * (b[iter] - 48) as f32;
        f *= 10.;
        iter += 1;
    }
    f /= 10.;

    iter += 1;
    let mut pos = 0.1f32;
    while iter < len {
        f += pos * sign * (b[iter] - 48) as f32;
        iter += 1;
        pos /= 10.;
    }

    f
}

// fn process_chunk(buffer: Vec<u8>) -> Vec<MapRecord> {
//     let mut map: Vec<MapRecord> = Vec::with_capacity(512);
//     let splitted = buffer.split(|b| *b == b'\n');
//     for line in splitted {
//         let mut semi = 0usize;
//         while line[semi] != b';' {
//             semi += 1;
//         }

//         let mut arr = [0u8; 16];
//         for i in 0..16.min(semi) {
//             arr[i] = line[i];
//         }

//         let key = unsafe { std::mem::transmute(arr) };

//         let value = s_parse(&line[semi + 1..line.len()]);
//         put_in_map(&mut map, MapRecord::new(key, value as f32));
//     }
//     map
// }

fn process_chunk_v2(buffer: Vec<u8>) -> Vec<MapRecord> {
    let mut map = Vec::with_capacity(512);
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
        put_in_map(&mut map, MapRecord::new(key, value));
        line_ind = end + 1;
    }

    map
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
        let handle: thread::JoinHandle<Vec<MapRecord>> =
            thread::spawn(move || process_chunk_v2(read_chunk(&file_c, offset)));
        offset += CHUNK_SIZE;
        handles.push(handle);
    }
    let creation_finish = creation_start.elapsed();

    println!("handles:{}", handles.len());
    let awaiting_start = Instant::now();
    let mut map: Vec<MapRecord> = Vec::with_capacity(512);
    for handle in handles {
        let handle_map = handle.join().unwrap();
        for record in handle_map {
            put_in_map(&mut map, record)
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
            String::from_utf8_lossy(&name_buff),
            rec.sum / rec.count as f32,
            rec.min,
            rec.max
        );
        let line_len = line.len();
        let written = file.write(line.as_bytes()).expect("unable to write");
        assert_eq!(line_len, written);
    }
    file.flush()?;

    return Ok(());
}
