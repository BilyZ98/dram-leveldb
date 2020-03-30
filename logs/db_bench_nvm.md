bench values bytes size100
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    500000
RawSize:    55.3 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       3.088 micros/op;   35.8 MB/s
fillrandom   :       3.779 micros/op;   29.3 MB/s
readseq      :       0.194 micros/op;  609.4 MB/s
readrandom   :       4.294 micros/op; (90751 of 500000 found)
bench values bytes size400
Keys:       16 bytes each
Values:     400 bytes each (200 bytes after compression)
Entries:    500000
RawSize:    198.4 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       4.183 micros/op;   94.8 MB/s
fillrandom   :       4.597 micros/op;   86.3 MB/s
readseq      :       0.241 micros/op; 1677.9 MB/s
readrandom   :       4.953 micros/op; (42333 of 500000 found)
bench values bytes size1600
Keys:       16 bytes each
Values:     1600 bytes each (800 bytes after compression)
Entries:    500000
RawSize:    770.6 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       5.847 micros/op;  263.6 MB/s
fillrandom   :       5.552 micros/op;  277.6 MB/s
readseq      :       0.518 micros/op; 2989.9 MB/s
readrandom   :       3.135 micros/op; (5996 of 500000 found)
