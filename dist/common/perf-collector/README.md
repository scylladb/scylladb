# scylla-perf-collector

This package collects system-wide perf samples using `perf record -a -F 10 --switch-output=1d`.

- Output is stored in `/var/log/scylla-perf/` and split daily.
- Logrotate compresses and removes files older than 14 days.
- Logrotate runs in the `scylla-helper.slice` systemd slice.
- Service: `scylla-perf-collector.service`.
