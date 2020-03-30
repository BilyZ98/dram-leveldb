bench values bytes size100
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    500000
RawSize:    55.3 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       3.029 micros/op;   36.5 MB/s
fillrandom   :       3.492 micros/op;   31.7 MB/s
readseq      :       0.365 micros/op;  324.2 MB/s
readrandom   :       4.325 micros/op; (90751 of 500000 found)
bench values bytes size400
Keys:       16 bytes each
Values:     400 bytes each (200 bytes after compression)
Entries:    500000
RawSize:    198.4 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       4.147 micros/op;   95.7 MB/s
fillrandom   :       4.588 micros/op;   86.5 MB/s
readseq      :       0.538 micros/op;  751.7 MB/s
readrandom   :       4.888 micros/op; (42333 of 500000 found)
bench values bytes size1600
Keys:       16 bytes each
Values:     1600 bytes each (800 bytes after compression)
Entries:    500000
RawSize:    770.6 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       5.606 micros/op;  274.9 MB/s
fillrandom   :       5.605 micros/op;  275.0 MB/s
readseq      :       0.495 micros/op; 3127.3 MB/s
readrandom   :       3.207 micros/op; (5996 of 500000 found)
