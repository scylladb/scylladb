#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Capture a tablet metadata and stats snapshot from a live cluster.

By default, the script will create a directory with a name of the format tablet_snap_YYMMDD_HHMMSS with CSV files
containing contents of system tables which contain relevant metadata, statistics and topology information.

Use --gz to pack that directory into a tar.gz archive, which is easier to pass around. The analysis
scripts accept such an archive directly via --snapshot, without unpacking it.

Use --manual for a dry run which prints cqlsh instructions instead of taking the snapshot.
"""


from __future__ import annotations

import argparse
import csv
import os
import os.path
import shutil
import tarfile
from datetime import datetime
import shlex
import sys
import time
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.topology import CSV_DELIMITER
from tablets.topology import Column
from tablets.topology import SNAPSHOT_TABLES
from tablets.topology import SnapshotTable
from tablets.topology import add_topology_source_args
from tablets.topology import get_live_topology_source_from_args


def _format_field(value: Any, format_value: Callable[[Any], str]) -> str:
    """
    Renders one field; a null becomes the empty field reading takes for one.
    """
    if value is None:
        return ""
    return format_value(value)


def _write_query_rows(path: str, rows, columns: Sequence[Column]) -> None:
    """
    Writes driver rows of a system table to a CSV dump.

    Every column is written, not only the ones read back. Columns needing more than str() to
    render say so in their Column.format.
    """
    formats = {column.name: column.format for column in columns}
    with open(path, "w", newline="") as f:
        writer = csv.writer(f, delimiter=CSV_DELIMITER)
        if not rows:
            return
        fields = list(rows[0]._fields)
        writer.writerow(fields)
        for row in rows:
            writer.writerow([
                _format_field(getattr(row, field), formats.get(field, str))
                for field in fields
            ])


def write_snapshot(snapshot_dir: str, src) -> None:
    """
    Writes a snapshot in a CSV format compatible with cqlsh COPY when used with
    DELIMITER=';' and HEADER=TRUE.

    ``snapshot_dir`` must already exist; the caller creates it atomically to claim
    the name (see :func:`create_snapshot_dir`).

    Read back by the csv module with the same quoting, so a field containing the delimiter
    survives instead of splitting its row.
    """
    # All issued before any is awaited, so they run concurrently.
    pending = {table: src.session.execute_async(src.queries[table]) for table in SNAPSHOT_TABLES}

    for table, result in pending.items():
        rows = list(result.result())
        _write_query_rows(os.path.join(snapshot_dir, table.file), rows, table.columns)


def print_manual_snapshot_instructions(args: argparse.Namespace, snapshot_dir: str) -> None:
    """
    Prints cqlsh commands that can be used to create a compatible snapshot manually.
    """

    cqlsh_args = []
    if args.user:
        cqlsh_args.extend(["-u", args.user])
    if args.password:
        cqlsh_args.extend(["-p", args.password])
    # cqlsh takes host then port positionally, so a port needs a host ahead of it.
    if args.cluster or args.port:
        cqlsh_args.append(args.cluster or "127.0.0.1")
    if args.port:
        cqlsh_args.append(str(args.port))

    cqlsh_prefix = " ".join(["cqlsh"] + [shlex.quote(arg) for arg in cqlsh_args])

    def copy_cmd(table: SnapshotTable) -> str:
        query = (f"COPY {table.name} TO '{snapshot_dir}/{table.file}'"
                 f" WITH DELIMITER='{CSV_DELIMITER}' AND HEADER=TRUE")
        return f"{cqlsh_prefix} -e {shlex.quote(query)}"

    print(f"mkdir -p {shlex.quote(snapshot_dir)}")
    for table in SNAPSHOT_TABLES:
        print(copy_cmd(table))
    if getattr(args, "gz", False):
        quoted_dir = shlex.quote(snapshot_dir)
        print(f"tar -czf {shlex.quote(archive_path_for(snapshot_dir))} {quoted_dir} && rm -r {quoted_dir}")


def archive_path_for(snapshot_dir: str) -> str:
    return f"{snapshot_dir}.tar.gz"


def snapshot_dir_candidates(output_dir: str | None) -> Generator[str, None, None]:
    """
    Yields candidate snapshot directory names.

    A requested name is the only candidate, so the caller gets that name or an error.
    A timestamped one is followed by millisecond suffixes, endlessly, recomputing the
    timestamp each time so a fresh second can be taken plain.

    Never checks whether a name is free: the caller claims one by creating it, keeping
    check and creation a single atomic step so concurrent runs cannot pick the same name.
    """
    if output_dir is not None:
        yield output_dir
        return

    def base() -> str:
        return datetime.now().strftime("tablet_snap_%y%m%d_%H%M%S")

    yield base()
    while True:
        yield f"{base()}_{int(time.time_ns() / 1_000_000) % 1000:03d}"
        time.sleep(0.001)


def create_snapshot_dir(output_dir: str | None, gz: bool = False, workdir: Path = Path(".")) -> str:
    """
    Creates a snapshot directory under ``workdir`` and returns its path.

    Never overwrites: the directory is claimed with ``os.mkdir``, which fails atomically
    if taken, and with ``gz`` the archive name must be free too.

    When output_dir is set, snapshot will be created with that name and function raises if it already exists.
    Otherwise, name is generated based on current timestamp and function will keep trying until it finds a free name.
    """
    for candidate in snapshot_dir_candidates(output_dir):
        path = str(workdir / candidate)
        archive = archive_path_for(path)
        if gz and os.path.exists(archive):
            conflict = archive
            continue
        try:
            os.mkdir(path)
        except FileExistsError:
            conflict = path
            continue
        return path
    # Reachable only for a requested name; a timestamped one never runs out.
    raise Exception(f"Snapshot already exists: {conflict}")


def choose_manual_snapshot_dir(output_dir: str | None, gz: bool = False) -> str:
    """
    Chooses a snapshot directory name for the manual workflow without creating it.

    The printed commands create it, so the name is only checked, not claimed: unlike
    :func:`create_snapshot_dir` this cannot be atomic, and is best-effort.
    """
    def is_taken(name: str) -> bool:
        return os.path.exists(name) or (gz and os.path.exists(archive_path_for(name)))

    for candidate in snapshot_dir_candidates(output_dir):
        if not is_taken(candidate):
            return candidate
    # Only a requested name runs out of candidates; a timestamped one keeps trying.
    raise Exception(f"Snapshot already exists: {candidate}")


def pack_snapshot(snapshot_dir: str) -> str:
    """
    Packs a snapshot directory into a tar.gz archive next to it and removes the
    directory, returning the archive path.

    The snapshot directory is the single top-level entry of the archive, so
    unpacking it recreates the directory layout the analysis scripts expect.
    """
    archive_path = archive_path_for(snapshot_dir)
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(snapshot_dir, arcname=os.path.basename(os.path.normpath(snapshot_dir)))
    shutil.rmtree(snapshot_dir)
    return archive_path


def take_snapshot(args: argparse.Namespace, workdir: Path = Path(".")) -> Path:
    """
    Captures a snapshot from the cluster ``args`` names, returning the directory it wrote,
    or the archive when ``--gz`` is set.

    Writes only under ``workdir``, and leaves the working directory alone: it is
    process-global, so moving it would be seen by everything else in the process.
    """
    snapshot_dir = create_snapshot_dir(args.output_dir, gz=args.gz, workdir=workdir)
    with get_live_topology_source_from_args(args) as src:
        write_snapshot(snapshot_dir, src)
    return Path(pack_snapshot(snapshot_dir) if args.gz else snapshot_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture tablet layout snapshot from a live cluster")
    add_topology_source_args(parser)
    parser.add_argument("--output-dir", metavar="DIR", help="Output snapshot directory (default: tablet_snap_YYMMDD_HHMMSS)")
    parser.add_argument("--gz", action="store_true",
                        help="Pack the snapshot directory into a <name>.tar.gz archive and remove the directory. "
                             "--snapshot reads such an archive directly, without unpacking")
    parser.add_argument("--manual", action="store_true",
                        help="Print cqlsh COPY commands that create the snapshot instead of capturing it directly")

    args = parser.parse_args()

    if args.manual:
        snapshot_dir = choose_manual_snapshot_dir(args.output_dir, gz=args.gz)
        print_manual_snapshot_instructions(args, snapshot_dir)
        return 0

    print(os.path.abspath(take_snapshot(args)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
