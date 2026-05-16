#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from __future__ import annotations

import argparse
import pathlib
from collections import defaultdict

from test.pylib.scylla_resource_scheduler import (
    HISTORICAL_DURATION_HEADROOM,
    historical_resource_archive_paths_for_root,
    historical_resource_db_paths_for_root,
    load_historical_resource_history,
)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate and report per-test profiling metrics from testlog sqlite DBs")
    parser.add_argument("--testlog", default="testlog", help="Path to the testlog directory")
    parser.add_argument("--limit", type=int, default=15, help="Number of rows to show per section")
    return parser.parse_args()


def format_float(value: float | None, digits: int = 3, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}{suffix}"


def format_bytes_mib(value: int | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 1024 / 1024:.1f}MiB"


def format_test_label(test_key: tuple[str, str, str]) -> str:
    suite, build_mode, test_name = test_key
    return f"{suite}|{build_mode}|{test_name}"


def format_suite_label(suite_key: tuple[str, str]) -> str:
    suite, build_mode = suite_key
    return f"{suite}|{build_mode}"


def format_percent(value: float | None) -> str:
    return format_float(value, digits=1, suffix="%")


def format_seconds(value: float | None) -> str:
    return format_float(value, digits=1, suffix="s")


def predicted_duration_seconds(duration_seconds: float | None) -> float | None:
    if duration_seconds is None or duration_seconds <= 0:
        return None
    return duration_seconds * HISTORICAL_DURATION_HEADROOM


def print_test_section(title: str, rows: list[tuple[tuple[str, str, str], object]], limit: int, *, sort_key) -> None:
    print(f"\n{title}:")
    if not rows:
        print("none")
        return
    for test_key, entry in sorted(rows, key=sort_key)[:limit]:
        print(
            f"{format_test_label(test_key)}|"
            f"samples={entry.sample_count}|"
            f"avg_cpu={format_float(entry.cores)}|"
            f"avg_time={format_seconds(entry.duration_seconds)}|"
            f"predicted_time={format_seconds(predicted_duration_seconds(entry.duration_seconds))}|"
            f"peak_mem={format_bytes_mib(entry.memory_bytes)}"
        )


def print_suite_section(history, limit: int) -> None:
    print("\nSuite class recommendations:")
    if not history.suites:
        print("none")
        return
    ranked = sorted(history.suites.items(), key=lambda item: (-float(item[1].sum_avg_time or 0.0), item[0]))[:limit]
    for suite_key, entry in ranked:
        print(
            f"{format_suite_label(suite_key)}|"
            f"class={entry.suite_class}|"
            f"samples={entry.sample_count}|"
            f"avg_cpu={format_float(entry.avg_cpu)}|"
            f"sum_avg_time={format_seconds(entry.sum_avg_time)}|"
            f"peak_mem={format_bytes_mib(entry.peak_mem)}"
        )


def print_host_section(history) -> None:
    print("\nHost utilization summary:")
    if not history.system_resource_metrics:
        print("none")
        return
    for host_id, entry in sorted(history.system_resource_metrics.items()):
        print(
            f"{host_id}|"
            f"samples={entry.sample_count}|"
            f"avg_cpu={format_percent(entry.avg_cpu)}|"
            f"peak_cpu={format_percent(entry.peak_cpu)}|"
            f"avg_mem={format_percent(entry.avg_memory)}|"
            f"peak_mem={format_percent(entry.peak_memory)}"
        )


def print_archive_coverage_section(archive_history, archive_count: int) -> None:
    print("\nArchive coverage summary:")
    print(f"archived_runs={archive_count}")
    if not archive_history.tests:
        print("none")
        return

    coverage: dict[str, dict[str, int]] = defaultdict(lambda: {"samples": 0, "tests": 0})
    for (suite, _build_mode, _test_name), entry in archive_history.tests.items():
        summary = coverage[suite]
        summary["samples"] += entry.sample_count
        summary["tests"] += 1

    for suite, summary in sorted(coverage.items(), key=lambda item: (-item[1]["samples"], item[0])):
        print(f"{suite}|tests={summary['tests']}|samples={summary['samples']}")


def main() -> int:
    args = parse_args()
    testlog = pathlib.Path(args.testlog).resolve()
    db_paths = historical_resource_db_paths_for_root(testlog)
    archive_paths = historical_resource_archive_paths_for_root(testlog)
    if not db_paths:
        print(f"No active sqlite DB or history archives found under {testlog}")
        return 1

    history = load_historical_resource_history(db_paths)
    archive_history = load_historical_resource_history(archive_paths)

    print(f"DB files: {len(db_paths)}")
    print(f"Archived runs: {len(archive_paths)}")
    print(f"Successful metric rows: {sum(entry.sample_count for entry in history.tests.values())}")
    print(f"Aggregated tests: {len(history.tests)}")

    low_cpu = [
        (test_key, entry)
        for test_key, entry in history.tests.items()
        if entry.cores is not None and entry.duration_seconds is not None
    ]
    print_test_section(
        "Low CPU lingerers",
        low_cpu,
        args.limit,
        sort_key=lambda item: (item[1].cores, -float(item[1].duration_seconds or 0.0), format_test_label(item[0])),
    )

    memory_hogs = [
        (test_key, entry)
        for test_key, entry in history.tests.items()
        if entry.memory_bytes is not None
    ]
    print_test_section(
        "Memory hogs",
        memory_hogs,
        args.limit,
        sort_key=lambda item: (-int(item[1].memory_bytes or 0), -float(item[1].duration_seconds or 0.0), format_test_label(item[0])),
    )

    tail_contributors = [
        (test_key, entry)
        for test_key, entry in history.tests.items()
        if predicted_duration_seconds(entry.duration_seconds) is not None
    ]
    print_test_section(
        "Top tail contributors",
        tail_contributors,
        args.limit,
        sort_key=lambda item: (-float(predicted_duration_seconds(item[1].duration_seconds) or 0.0), format_test_label(item[0])),
    )

    print_suite_section(history, args.limit)
    print_host_section(history)
    print_archive_coverage_section(archive_history, len(archive_paths))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
