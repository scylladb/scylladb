#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from __future__ import annotations

import argparse
import pathlib
import sqlite3
from collections import defaultdict


TESTS_TABLE = "tests"
METRICS_TABLE = "test_metrics"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate and report per-test profiling metrics from testlog sqlite DBs")
    parser.add_argument("--testlog", default="testlog", help="Path to the testlog directory")
    parser.add_argument("--limit", type=int, default=15, help="Number of rows to show per section")
    return parser.parse_args()


def iter_db_paths(testlog: pathlib.Path) -> list[pathlib.Path]:
    return sorted(path for path in testlog.glob("sqlite_*.db") if path.is_file())


def load_rows(db_paths: list[pathlib.Path]) -> list[tuple[str, str, float | None, float | None, float | None, int | None]]:
    rows: list[tuple[str, str, float | None, float | None, float | None, int | None]] = []
    query = f"""
        SELECT
            t.directory,
            t.test_name,
            m.time_taken,
            m.usage_sec,
            m.user_sec,
            CAST(m.memory_peak AS INTEGER)
        FROM {TESTS_TABLE} AS t
        JOIN {METRICS_TABLE} AS m ON m.test_id = t.id
        WHERE m.success = 1
    """
    for db_path in db_paths:
        connection = sqlite3.connect(db_path)
        try:
            rows.extend(connection.execute(query).fetchall())
        finally:
            connection.close()
    return rows


def aggregate(rows: list[tuple[str, str, float | None, float | None, float | None, int | None]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], dict[str, object]] = defaultdict(lambda: {
        "samples": 0,
        "time_sum": 0.0,
        "time_count": 0,
        "cpu_ratio_sum": 0.0,
        "cpu_ratio_count": 0,
        "memory_peak": 0,
    })
    for directory, test_name, time_taken, usage_sec, _user_sec, memory_peak in rows:
        entry = grouped[(directory, test_name)]
        entry["samples"] += 1
        if time_taken is not None and time_taken > 0:
            entry["time_sum"] += float(time_taken)
            entry["time_count"] += 1
            if usage_sec is not None:
                entry["cpu_ratio_sum"] += float(usage_sec) / float(time_taken)
                entry["cpu_ratio_count"] += 1
        if memory_peak is not None:
            entry["memory_peak"] = max(int(entry["memory_peak"]), int(memory_peak))

    result: list[dict[str, object]] = []
    for (directory, test_name), entry in grouped.items():
        avg_time = None if entry["time_count"] == 0 else entry["time_sum"] / entry["time_count"]
        avg_cpu = None if entry["cpu_ratio_count"] == 0 else entry["cpu_ratio_sum"] / entry["cpu_ratio_count"]
        result.append({
            "directory": directory,
            "test_name": test_name,
            "samples": entry["samples"],
            "avg_time": avg_time,
            "avg_cpu": avg_cpu,
            "memory_peak": entry["memory_peak"],
        })
    return result


def format_row(entry: dict[str, object]) -> str:
    avg_cpu = entry["avg_cpu"]
    avg_time = entry["avg_time"]
    memory_peak = int(entry["memory_peak"])
    peak_mib = memory_peak / 1024 / 1024 if memory_peak else 0.0
    avg_cpu_text = "n/a" if avg_cpu is None else f"{avg_cpu:.3f}"
    avg_time_text = "n/a" if avg_time is None else f"{avg_time:.1f}s"
    return (
        f"{entry['directory']}|{entry['test_name']}|"
        f"samples={entry['samples']}|"
        f"avg_cpu={avg_cpu_text}|"
        f"avg_time={avg_time_text}|"
        f"peak_mem={peak_mib:.1f}MiB"
    )


def main() -> int:
    args = parse_args()
    testlog = pathlib.Path(args.testlog).resolve()
    db_paths = iter_db_paths(testlog)
    if not db_paths:
        print(f"No sqlite_*.db files found under {testlog}")
        return 1

    rows = load_rows(db_paths)
    aggregated = aggregate(rows)
    print(f"DB files: {len(db_paths)}")
    print(f"Successful metric rows: {len(rows)}")
    print(f"Aggregated tests: {len(aggregated)}")

    low_cpu = sorted(
        (entry for entry in aggregated if entry["avg_cpu"] is not None and entry["avg_time"] is not None),
        key=lambda entry: (entry["avg_cpu"], -entry["avg_time"], str(entry["test_name"])),
    )[:args.limit]
    print("\nLow CPU lingerers:")
    for entry in low_cpu:
        avg_cpu = entry["avg_cpu"]
        avg_time = entry["avg_time"]
        peak_mib = int(entry["memory_peak"]) / 1024 / 1024
        print(f"{entry['directory']}|{entry['test_name']}|samples={entry['samples']}|avg_cpu={avg_cpu:.3f}|avg_time={avg_time:.1f}s|peak_mem={peak_mib:.1f}MiB")

    memory_hogs = sorted(aggregated, key=lambda entry: (-int(entry["memory_peak"]), -(entry["avg_time"] or 0.0), str(entry["test_name"])))[:args.limit]
    print("\nMemory hogs:")
    for entry in memory_hogs:
        avg_cpu = entry["avg_cpu"]
        avg_time = entry["avg_time"]
        peak_mib = int(entry["memory_peak"]) / 1024 / 1024
        avg_cpu_text = "n/a" if avg_cpu is None else f"{avg_cpu:.3f}"
        avg_time_text = "n/a" if avg_time is None else f"{avg_time:.1f}s"
        print(f"{entry['directory']}|{entry['test_name']}|samples={entry['samples']}|avg_cpu={avg_cpu_text}|avg_time={avg_time_text}|peak_mem={peak_mib:.1f}MiB")

    suite_summary: dict[str, dict[str, float]] = defaultdict(lambda: {"samples": 0.0, "time_sum": 0.0, "cpu_sum": 0.0, "cpu_count": 0.0, "memory_peak": 0.0})
    for entry in aggregated:
        suite = str(entry["directory"])
        summary = suite_summary[suite]
        summary["samples"] += float(entry["samples"])
        summary["time_sum"] += float(entry["avg_time"] or 0.0)
        if entry["avg_cpu"] is not None:
            summary["cpu_sum"] += float(entry["avg_cpu"])
            summary["cpu_count"] += 1.0
        summary["memory_peak"] = max(summary["memory_peak"], float(entry["memory_peak"]))

    ranked_suites = sorted(
        suite_summary.items(),
        key=lambda item: (-item[1]["time_sum"], item[0]),
    )[:args.limit]
    print("\nSuite summary:")
    for suite, summary in ranked_suites:
        avg_cpu = 0.0 if summary["cpu_count"] == 0 else summary["cpu_sum"] / summary["cpu_count"]
        peak_mib = summary["memory_peak"] / 1024 / 1024
        print(f"{suite}|tests={int(summary['samples'])}|sum_avg_time={summary['time_sum']:.1f}s|avg_cpu={avg_cpu:.3f}|peak_mem={peak_mib:.1f}MiB")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
