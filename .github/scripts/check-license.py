#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2024-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import argparse
import sys
from pathlib import Path
from typing import Set


def parse_args() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='Check license headers in files')
    parser.add_argument('--files', required=True, nargs="+", type=Path,
                        help='List of files to check')
    parser.add_argument('--license', required=True,
                        help='License to check for')
    parser.add_argument('--check-lines', type=int, default=10,
                        help='Number of lines to check (default: %(default)s)')
    parser.add_argument('--extensions', required=True, nargs="+",
                        help='List of file extensions to check')
    parser.add_argument('--verbose', action='store_true',
                        help='Print verbose output (default: %(default)s)')
    return parser.parse_args()


def should_check_file(file_path: Path, allowed_extensions: Set[str]) -> bool:
    return file_path.suffix in allowed_extensions


def check_license_header(file_path: Path, license_header: str, check_lines: int) -> bool:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for _ in range(check_lines):
                line = f.readline()
                if license_header in line:
                    return True
        return False
    except (UnicodeDecodeError, StopIteration):
        # Handle files that can't be read as text or have fewer lines
        return False


def main() -> int:
    args = parse_args()

    if not args.files:
        print("No files to check")
        return 0

    num_errors = 0

    for file_path in args.files:
        # Skip non-existent files
        if not file_path.exists():
            continue

        # Skip files with non-matching extensions
        if not should_check_file(file_path, args.extensions):
            print(f"ℹ️ Skipping file with unchecked extension: {file_path}")
            continue

        # Check license header
        if check_license_header(file_path, args.license, args.check_lines):
            if args.verbose:
                print(f"✅ License header found in: {file_path}")
        else:
            print(f"❌ Missing license header in: {file_path}")
            num_errors += 1

    if num_errors > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
