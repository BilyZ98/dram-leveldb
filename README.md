**LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values.**

This is a basic simple implementation of dram based LevelDB, which means
its SSTables are store in DRAM and it has no recovery mechanism and version control. It's only for performance comparision.

# build 
You can refer to the original LevelDB doc to see how to build for posix
[LevelDB](https://github.com/google/leveldb)

# benchmark
After build you can run benchmark comparision for DRAM-LevelDB and LevelDB.
The name of the benchmark executable binary file is db_bench_nvm.

