use memmap::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Cursor, Read};
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;
use std::time::Instant;

#[derive(Debug, Clone)]
struct TempStats {
    min_temp: f32,
    max_temp: f32,
    total_temp: f32,
    count: usize,
}

impl TempStats {
    fn new(temp: f32) -> Self {
        TempStats {
            min_temp: temp,
            max_temp: temp,
            total_temp: temp,
            count: 1,
        }
    }

    fn update(&mut self, temp: f32) {
        self.min_temp = self.min_temp.min(temp);
        self.max_temp = self.max_temp.max(temp);
        self.total_temp += temp;
        self.count += 1;
    }

    fn mean(&self) -> f32 {
        self.total_temp / self.count as f32
    }
}


fn print(map: Vec<(&String, &TempStats)>) {
    print!("{}", '{');
    for (key, temp) in map {
        print!(
            "{}={}/{}/{}, ",
            key,
            temp.min_temp,
            temp.mean(),
            temp.max_temp
        );
    }
    print!("{}", '}');
}

const CHUNK_SIZE: usize = 1 << 20;  // Less then 1 Mb



fn solution() -> io::Result<()> {
    let start: Instant = Instant::now();


    let path = "measurements.txt";
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mmap_arc = Arc::new(mmap);

    let file_len = mmap_arc.len();
    let thread_count = std::thread::available_parallelism().unwrap().into();
    let pool = ThreadPool::new(thread_count);
    let results = Arc::new(Mutex::new(Vec::new()));


    for i in 0..thread_count {
        let mmap_arc_clone = mmap_arc.clone(); // Clone the Arc, not the Mmap
        let results_clone = results.clone();
        let start = i * CHUNK_SIZE;
        let mut end = start + CHUNK_SIZE;
        if end > file_len {
            end = file_len;
        }

        pool.execute(move || {
            let mut local_map: HashMap<String, TempStats> = HashMap::new();
            let mut reader = Cursor::new(&mmap_arc_clone[start..end]);

            if i > 0 {
                // Adjust reader to start at the beginning of a new line
                let mut buf: [u8; 1] = [0];
                loop {
                    if reader.read_exact(&mut buf).is_err() || buf[0] == b'\n' {
                        break;
                    }
                }
            }

            let mut buffer = String::new();
            while let Ok(bytes_read) = reader.read_line(&mut buffer) {
                if bytes_read == 0 {
                    break;
                }

                let parts: Vec<&str> = buffer.trim_end().split(';').collect();
                if parts.len() == 2 {
                    if let (Some(city), Ok(temp)) = (parts.get(0), parts.get(1).unwrap_or(&"").parse::<f32>()) {
                        local_map.entry(city.to_string())
                            .and_modify(|e| e.update(temp))
                            .or_insert_with(|| TempStats::new(temp));
                    }
                }
                buffer.clear();
            }

            let mut results = results_clone.lock().unwrap();
            results.push(local_map);
        });
    }

    pool.join();


    // Merge local maps into a global map
    let mut global_map: HashMap<String, TempStats> = HashMap::new();
    let results = results.lock().unwrap();

    for local_map in results.iter() {
        for (city, stats) in local_map {
            global_map.entry(city.clone())
                .and_modify(|e| {
                    e.total_temp += stats.total_temp;
                    e.count += stats.count;
                    e.min_temp = e.min_temp.min(stats.min_temp);
                    e.max_temp = e.max_temp.max(stats.max_temp);
                })
                .or_insert_with(|| stats.clone());
        }
    }

    // Sort and print results
    let mut cities: Vec<_> = global_map.iter().collect();
    cities.sort_by_key(|&(city, _)| city);
    print(cities);


    println!();
    println!("Total execution time is : {:?}" ,start.elapsed());


    Ok(())

}

fn main() -> io::Result<()> {
    solution()
}
