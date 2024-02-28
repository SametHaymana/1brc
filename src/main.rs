use memmap::Mmap;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};
use threadpool::ThreadPool;

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

fn print_hm(map: HashMap<String, TempStats>) {
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

const CHUNK_SIZE: usize = 1 << 23; // 1 mb

fn hard_way() -> io::Result<()> {
    let mut map: HashMap<String, TempStats> = HashMap::new();
    let mut _counter = 0;

    let file_path = Path::new("measurements.txt");

    // Open the file in read-only mode (ignoring errors).
    let file = File::open(file_path)?;

    // Create a new buffered reader for the file
    let reader = io::BufReader::new(file);

    // Iterate over each line in the file
    for line in reader.lines() {
        // The `line` is wrapped in a Result for error handling
        match line {
            Ok(line) => {
                let mut iterator = line.split(';');
                // Parse line by ;
                let key = iterator.next().unwrap();
                let _val: f32 = iterator.next().unwrap().parse().unwrap();

                //println!("KEY {} VAL {}", key, _val);
                map.entry(key.to_string())
                    .and_modify(|val| val.update(_val))
                    .or_insert_with(|| TempStats::new(_val));

                _counter += 1;

                if _counter % 50_000_000 == 0 {
                    println!("Readed {}", _counter);
                }
            }
            Err(e) => {
                // Handle any errors that may occur
                println!("Error reading line: {}", e);
            }
        }
    }

    // Write map
    print_hm(map);

    Ok(())
}

fn with_rayon() -> io::Result<()> {
    let file_path = "measurements.txt";
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let results: HashMap<String, TempStats> = reader
        .lines()
        .filter_map(Result::ok)
        .par_bridge() // Convert to a parallel iterator
        .filter_map(|line| {
            let parts: Vec<&str> = line.split(';').collect();
            parts.get(0).and_then(|&city| {
                parts.get(1).and_then(|&value_str| {
                    value_str
                        .parse::<f32>()
                        .ok()
                        .map(|value| (city.to_string(), value))
                })
            })
        })
        .fold(
            || HashMap::new(),
            |mut acc: HashMap<String, TempStats>, (city, value)| {
                acc.entry(city)
                    .and_modify(|stats| stats.update(value))
                    .or_insert_with(|| TempStats::new(value));
                acc
            },
        )
        .reduce(
            || HashMap::new(),
            |mut acc: HashMap<String, TempStats>, curr: HashMap<String, TempStats>| {
                for (city, stats) in curr {
                    acc.entry(city)
                        .and_modify(|e| {
                            e.update(stats.min_temp);
                            e.update(stats.max_temp);
                        })
                        .or_insert(stats);
                }
                acc
            },
        );

    // Sorting results
    let mut results: Vec<_> = results.into_iter().collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    print!("{}", '{');
    // Print results
    for (city, val) in results {
        print!(
            "{}={:.1}/{:.1}/{:.1},",
            city,
            val.min_temp,
            val.mean(),
            val.max_temp
        );
    }
    print!("{}", '}');

    Ok(())
}

fn with_threadpool() -> io::Result<()> {
    let path = "measurements.txt";
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mmap_arc = Arc::new(mmap); // Wrap the Mmap in an Arc here

    let file_len = mmap_arc.len();

    let thread_count = (file_len + CHUNK_SIZE - 1) / CHUNK_SIZE;
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

    Ok(())

}

fn main() -> io::Result<()> {
    with_rayon()

}
