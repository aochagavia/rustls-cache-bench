Rustls Cache Bench
==================

See [this rustls issue](https://github.com/rustls/rustls/issues/1200) and the CLI `--help` for
details.

The benchmarks are under `src`.

### Analyzing results

For each benchmarked cache size, the following files are produced:

#### `throughput.json`

Example:

```json
{
    "cache_size": 10000,
    "handshakes_per_second": 4996.793827227934,
    "server_saturation": 0.020581829251588035
}
```

The `server_saturation` parameter is useful to know how much work the server threads where actually
doing.

#### `timings`

Example:

```
182;197;197;212;194;193;199;199;...
```

This file contains a `;`-separated list with the duration of each request in microseconds. You can
use it, for instance, to plot the latency distribution.
