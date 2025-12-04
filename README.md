## ParBFT

[parbft](https://eprint.iacr.org/2023/679)

## Quick Start
ParBFT is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:
```
$ git clone https://github.com/
$ cd parbft-parbft1-rust/benchmark
$ pip install -r requirements.txt
```
You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:
```
$ fab local
```
This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.
```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Protocol: 1 
 DDOS attack: False 
 Committee size: 4 nodes
 Input rate: 10,000 tx/s
 Transaction size: 16 B
 Faults: 0 nodes
 Execution time: 101 s

 Consensus timeout delay: 1,000 ms
 Consensus sync retry delay: 5,000 ms
 Consensus max payloads size: 1,000 B
 Consensus min block delay: 0 ms
 Mempool queue capacity: 10,000 B
 Mempool max payloads size: 15,625 B
 Mempool min block delay: 0 ms

 + RESULTS:
 Consensus TPS: 9,278 tx/s
 Consensus BPS: 148,453 B/s
 Consensus latency: 13 ms

 End-to-end TPS: 9,263 tx/s
 End-to-end BPS: 148,207 B/s
 End-to-end latency: 49 ms
-----------------------------------------
```




#### reference

- [parbft](https://eprint.iacr.org/2023/679)
- [Ditto](http://arxiv.org/abs/2106.10362)

