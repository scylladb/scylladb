#!/usr/bin/env python3
"""
Latte Metrics Exporter for Prometheus

Tails latte sampling output files and exposes per-workload latency and
throughput as Prometheus metrics on port 9091.

Expected latte sampling output format (every --sampling interval):
  <time>  <cycles>  <errors>  <throughput>  <min> <p50> <p75> <p90> <p95> <p99> <p999> <max>

Each latte instance writes to a separate log file. This exporter monitors
multiple log files and tags metrics with the workload name derived from
the file name.
"""

import os
import re
import sys
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Metrics storage: {workload_name: {metric_name: value}}
metrics_lock = threading.Lock()
metrics = {}


def parse_latte_line(line):
    """Parse a latte sampling output line.
    Format: time  cycles  errors  throughput  min  p50  p75  p90  p95  p99  p999  max
    """
    line = line.strip()
    if not line or line.startswith("Running") or line.startswith("#") or line.startswith("="):
        return None

    parts = line.split()
    if len(parts) < 12:
        return None

    try:
        return {
            "time": float(parts[0]),
            "cycles": int(parts[1]),
            "errors": int(parts[2]),
            "throughput": float(parts[3]),
            "min": float(parts[4]),
            "p50": float(parts[5]),
            "p75": float(parts[6]),
            "p90": float(parts[7]),
            "p95": float(parts[8]),
            "p99": float(parts[9]),
            "p999": float(parts[10]),
            "max": float(parts[11]),
        }
    except (ValueError, IndexError):
        return None


def tail_file(filepath, workload_name):
    """Tail a latte log file and update metrics for this workload."""
    global metrics

    while not os.path.exists(filepath):
        time.sleep(1)

    with open(filepath, "r") as f:
        # Seek to end
        f.seek(0, 2)

        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue

            parsed = parse_latte_line(line)
            if parsed:
                with metrics_lock:
                    metrics[workload_name] = parsed


class MetricsHandler(BaseHTTPRequestHandler):
    """Serve Prometheus metrics."""

    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

        lines = []
        with metrics_lock:
            # Throughput
            lines.append('# HELP latte_throughput_ops Operations per second')
            lines.append('# TYPE latte_throughput_ops gauge')
            for workload, m in metrics.items():
                lines.append(
                    f'latte_throughput_ops{{workload="{workload}"}} {m["throughput"]}'
                )

            # Latency percentiles (in ms)
            lines.append('# HELP latte_latency_ms Latency in milliseconds')
            lines.append('# TYPE latte_latency_ms gauge')
            for workload, m in metrics.items():
                for pct_name, pct_label in [
                    ("min", "0"),
                    ("p50", "50"),
                    ("p75", "75"),
                    ("p90", "90"),
                    ("p95", "95"),
                    ("p99", "99"),
                    ("p999", "99.9"),
                    ("max", "100"),
                ]:
                    lines.append(
                        f'latte_latency_ms{{workload="{workload}",quantile="{pct_label}"}} {m[pct_name]}'
                    )

            # Errors
            lines.append('# HELP latte_errors_total Total errors')
            lines.append('# TYPE latte_errors_total gauge')
            for workload, m in metrics.items():
                lines.append(
                    f'latte_errors_total{{workload="{workload}"}} {m["errors"]}'
                )

            # Cycles (ops in last interval)
            lines.append('# HELP latte_cycles_total Cycles in last interval')
            lines.append('# TYPE latte_cycles_total gauge')
            for workload, m in metrics.items():
                lines.append(
                    f'latte_cycles_total{{workload="{workload}"}} {m["cycles"]}'
                )

        self.wfile.write(("\n".join(lines) + "\n").encode())

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    if len(sys.argv) < 3 or len(sys.argv) % 2 != 1:
        print(
            f"Usage: {sys.argv[0]} <workload1> <logfile1> [<workload2> <logfile2> ...]"
        )
        sys.exit(1)

    pairs = []
    for i in range(1, len(sys.argv), 2):
        pairs.append((sys.argv[i], sys.argv[i + 1]))

    # Start tailer threads
    for workload_name, filepath in pairs:
        t = threading.Thread(
            target=tail_file, args=(filepath, workload_name), daemon=True
        )
        t.start()
        print(f"Tailing {filepath} as workload '{workload_name}'")

    # Start HTTP server
    port = 9091
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    print(f"Latte exporter listening on :{port}/metrics")
    server.serve_forever()


if __name__ == "__main__":
    main()
