```
=== Image Source Benchmark ===
Configuration: 640x480_RGB
Frame size: 921600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 5.24 s
  FPS: 190.88
  Throughput: 167.76 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.20        2.14    0.61    0.23    1.01

=== Image Source Benchmark ===
Configuration: 640x480_GRAY8
Frame size: 307200 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 5.15 s
  FPS: 194.10
  Throughput: 56.87 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.06        0.82    0.30    0.11    0.50

=== Image Source Benchmark ===
Configuration: 1280x720_RGB
Frame size: 2764800 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 5.51 s
  FPS: 181.42
  Throughput: 478.36 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.67        3.64    1.61    0.57    2.60

=== Image Source Benchmark ===
Configuration: 1280x720_GRAY8
Frame size: 921600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 5.24 s
  FPS: 191.02
  Throughput: 167.89 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.21        1.45    0.58    0.21    0.97

=== Image Source Benchmark ===
Configuration: 1920x1080_RGB
Frame size: 6220800 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 6.09 s
  FPS: 164.20
  Throughput: 974.16 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    1.63        15.80   3.48    1.14    5.37

=== Image Source Benchmark ===
Configuration: 1920x1080_GRAY8
Frame size: 2073600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 5.41 s
  FPS: 184.74
  Throughput: 365.33 MB/s
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.43        2.96    1.26    0.44    2.04
```

```
=== Full Pipeline Benchmark ===
Configuration: 640x480_RGB
Frame size: 921600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.28 s
  FPS: 97.25
  Throughput: 85.47 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    0.61        3.80    1.76    0.53    2.65
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.26        1.51    0.66    0.24    1.09
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    0.24        2.37    1.09    0.38    1.72

=== Full Pipeline Benchmark ===
Configuration: 640x480_GRAY8
Frame size: 307200 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.21 s
  FPS: 97.94
  Throughput: 28.69 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    0.25        2.49    1.19    0.37    1.79
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.13        1.05    0.40    0.12    0.62
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    0.10        1.62    0.79    0.33    1.29

=== Full Pipeline Benchmark ===
Configuration: 1280x720_RGB
Frame size: 2764800 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.65 s
  FPS: 93.88
  Throughput: 247.53 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    1.23        9.81    4.23    1.11    5.92
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.61        4.99    1.96    0.61    2.92
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    0.57        5.32    2.27    0.60    3.18

=== Full Pipeline Benchmark ===
Configuration: 1280x720_GRAY8
Frame size: 921600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.24 s
  FPS: 97.64
  Throughput: 85.81 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    0.46        3.37    1.51    0.47    2.35
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.20        1.24    0.53    0.17    0.90
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    0.21        2.79    0.97    0.37    1.57

=== Full Pipeline Benchmark ===
Configuration: 1920x1080_RGB
Frame size: 6220800 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.82 s
  FPS: 92.43
  Throughput: 548.35 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    6.84        21.79   9.80    1.79    13.21
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    1.80        12.01   3.49    1.21    5.67
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    2.40        16.84   6.31    1.97    10.14

=== Full Pipeline Benchmark ===
Configuration: 1920x1080_GRAY8
Frame size: 2073600 bytes
Warmup frames: 50
Benchmark frames: 1000

Results:
  Frames processed: 1000
  Duration: 10.53 s
  FPS: 94.98
  Throughput: 187.84 MB/s
  Total Latency (ms):
    min         max     avg     stddev  p95
    1.19        5.77    3.42    0.82    4.71
  Source Latency (publish -> buffer pushed) (ms):
    min         max     avg     stddev  p95
    0.58        2.93    1.53    0.43    2.24
  Sink Latency (buffer received -> message published) (ms):
    min         max     avg     stddev  p95
    0.54        3.52    1.89    0.47    2.66
```
