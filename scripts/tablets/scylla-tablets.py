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

Each command maps to a standalone script in this directory, which can also be run
directly (e.g. ./cluster-load.py). Run a command with --help for its options:

    scylla-tablets cluster-load --help
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import tablets

# The command scripts sit in the package, which is not this file's directory in an install:
# relocate_python3 leaves this one under libexec/ with the package a level up.
SCRIPT_DIR = Path(tablets.__file__).resolve().parent

# Command name -> script file (relative to this directory). The command names are
# the script file stems without the .py suffix.
COMMANDS = {
    "snapshot": "snapshot.py",
    "table-summary": "table-summary.py",
    "table-load": "table-load.py",
    "cluster-load": "cluster-load.py",
}

# One-line descriptions shown in --help, mirroring each script's own summary.
COMMAND_HELP = {
    "snapshot": "Capture a tablet metadata and stats snapshot from a live cluster",
    "table-summary": "Print one summary row per table",
    "table-load": "Print per-tablet load information for a single table",
    "cluster-load": "Print tablet load by datacenter, rack, node, and shard",
}


def load_command_module(command: str) -> ModuleType:
    """
    Loads a command script as a module by file path.

    The scripts have hyphenated names and so cannot be imported normally. They are
    loaded under the ``tablets`` package so their ``from tablets.X import ...``
    imports resolve.
    """
    script_path = SCRIPT_DIR / COMMANDS[command]
    module_name = f"tablets.{command.replace('-', '_')}_cmd"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load command script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


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

    module = load_command_module(command)
    # Hand the remaining arguments to the command's own argparse-based main(), which
    # reads sys.argv. Rewrite argv[0] so its usage/error messages name the command.
    sys.argv = [f"scylla-tablets {command}"] + argv[1:]
    return module.main()


if __name__ == "__main__":
    sys.exit(main())
