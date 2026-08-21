#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Unified entry point for the tablet-analysis scripts.

Usage:

    scylla-tablets <command> [options]

Each command maps to a module in this directory, which can also be run directly
(e.g. ./cluster_load.py). Run a command with --help for its options:

    scylla-tablets cluster --help
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets import cluster_load
from tablets import snapshot
from tablets import table_load
from tablets import table_summary

# Command name -> the module implementing it. Each exposes a main() reading sys.argv.
COMMANDS = {
    "snapshot": snapshot,
    "tables": table_summary,
    "table": table_load,
    "cluster": cluster_load,
}

# One-line descriptions shown in --help, mirroring each script's own summary.
COMMAND_HELP = {
    "snapshot": "Capture a tablet metadata and stats snapshot from a live cluster",
    "tables": "Print one summary row per table",
    "table": "Print per-tablet load information for a single table",
    "cluster": "Print tablet load by datacenter, rack, node, and shard",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scylla-tablets",
        description="Inspect tablet placement, load, and load-balancing state in ScyllaDB clusters.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run 'scylla-tablets <command> --help' for command-specific options.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    for name in COMMANDS:
        subparsers.add_parser(name, help=COMMAND_HELP[name], add_help=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    parser = build_parser()
    if not argv or argv[0] in ("-h", "--help"):
        parser.print_help()
        return 0

    command = argv[0]
    if command not in COMMANDS:
        parser.error(f"invalid command: {command!r} (choose from {', '.join(COMMANDS)})")

    # Hand the remaining arguments to the command's own argparse-based main(), which
    # reads sys.argv. Rewrite argv[0] so its usage/error messages name the command.
    sys.argv = [f"scylla-tablets {command}"] + argv[1:]
    return COMMANDS[command].main()


if __name__ == "__main__":
    sys.exit(main())
