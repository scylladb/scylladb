# scylla-perf-collector

This package collects system-wide perf samples using
`perf record -a -F 2 -z --switch-output=1d --switch-max-files=15`.

- Output is stored in `/var/log/scylla-perf/` as `perf.data.<timestamp>` files.
- `perf` rotates the recording itself, switching to a new file once a day
  (`--switch-output=1d`), so each rotated file is complete and never touched
  again. There is no service restart and no gap in collection.
- Old recordings are expired by `perf` via `--switch-max-files=15` (roughly two
  weeks of daily files, including the one currently being written).
- Recordings are zstd-compressed inline by `perf` (`-z`), so no external
  compression step is needed.
- The collector service runs in the `scylla-helper.slice` systemd slice.
- At 2 Hz system-wide sampling the data rate is low, but ensure `/var/log` has
  headroom for the retained recordings on high-core-count nodes.
- Service: `scylla-perf-collector.service`.
