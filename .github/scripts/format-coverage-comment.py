#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2026-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import argparse
import re
from pathlib import Path

COMMENT_MARKER = "<!-- coverage-report-bot -->"

SUMMARY_LINE_RE = re.compile(r'^\s*(lines|functions|branches)\.+:\s*(.+)$')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Render a patch-coverage lcov summary/per-file report as a PR comment body')
    parser.add_argument('--summary', required=True, type=Path,
                        help="Path to 'lcov --summary' output")
    parser.add_argument('--per-file', required=True, type=Path,
                        help="Path to 'lcov --list' output")
    parser.add_argument('--base-commit', required=True,
                        help='Base commit the patch coverage was computed from')
    parser.add_argument('--head-sha', required=True,
                        help='Head commit the patch coverage was computed for')
    parser.add_argument('--output', required=True, type=Path,
                        help='Where to write the rendered Markdown comment body')
    return parser.parse_args()


def parse_summary(summary_text: str) -> dict:
    metrics = {}
    for line in summary_text.splitlines():
        match = SUMMARY_LINE_RE.match(line)
        if match:
            metrics[match.group(1)] = match.group(2).strip()
    return metrics


def render(metrics: dict, per_file_text: str, base_commit: str, head_sha: str) -> str:
    rows = "\n".join(
        f"| {name.capitalize()} | {value} |"
        for name, value in metrics.items()
    ) or "| (no coverage data produced) | |"

    return f"""{COMMENT_MARKER}
## Patch Coverage Report

| Metric | Coverage |
|--------|----------|
{rows}

<details><summary>Per-file breakdown</summary>

```
{per_file_text.strip()}
```

</details>

_Computed for `{base_commit[:12]}..{head_sha[:12]}` against `dev`-mode unit tests only \
(no dtest coverage). Informational only — not a merge gate._
"""


def main() -> None:
    args = parse_args()
    metrics = parse_summary(args.summary.read_text())
    per_file_text = args.per_file.read_text()
    args.output.write_text(render(metrics, per_file_text, args.base_commit, args.head_sha))


if __name__ == '__main__':
    main()
