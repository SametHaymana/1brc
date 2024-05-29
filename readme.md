# 1 Billion Row Challenge

For aggregating 1 billion rows with 3 different approaches evaluated on an AMD Ryzen 7 3700U with 16 GB RAM, using thread pooling and memory mapping (MMap), I achieved an aggregation response time under 1 second (50 - 40 ms).

**Update**

By optimizing the configuration and adjusting the thread pool size and CHUNK_SIZE, the aggregation time can be reduced to 35-40 ms on the same device.

```bash
hyperfine --warmup 10 --runs 20 "./target/release/rust_1brc"
```
![](./bench.png)


### Create measurements file

```bash
    python3 data/create_measurements.py  1000000000
```
