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


def _format_field(value: Any, format: Callable[[Any], str]) -> str:
    """
    Renders one field; a null becomes the empty field reading takes for one.
    """
    if value is None:
        return ""
    return format(value)


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
    if args.cluster:
        cqlsh_args.append(args.cluster)
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
    Yields candidate snapshot directory names, starting with the requested (or
    timestamped) base name and then appending millisecond suffixes.

    When no name is requested, the timestamp is recomputed on every yield, so
    spinning into the next second produces a fresh base that may be free on its
    own, rather than only ever suffixing the second the generator started in.

    The generator never decides whether a name is free; the caller claims a
    candidate by atomically creating it and advances to the next on collision.
    This keeps the check and the creation a single atomic step, so concurrent
    runs never pick the same name.
    """
    def base() -> str:
        return output_dir or datetime.now().strftime("tablet_snap_%y%m%d_%H%M%S")

    yield base()
    while True:
        yield f"{base()}_{int(time.time_ns() / 1_000_000) % 1000:03d}"
        time.sleep(0.001)


def create_snapshot_dir(output_dir: str | None, gz: bool = False) -> str:
    """
    Atomically creates a fresh snapshot directory and returns its name.

    Retries over :func:`snapshot_dir_candidates` until ``os.mkdir`` succeeds,
    which fails atomically if the directory already exists. With ``gz``, a
    candidate whose archive name is already taken is skipped too, so packing
    never overwrites an older snapshot.
    """
    for candidate in snapshot_dir_candidates(output_dir):
        if gz and os.path.exists(archive_path_for(candidate)):
            continue
        try:
            os.mkdir(candidate)
        except FileExistsError:
            continue
        return candidate


def choose_manual_snapshot_dir(output_dir: str | None, gz: bool = False) -> str:
    """
    Chooses a snapshot directory name for the manual workflow without creating it.

    The commands printed for the user create the directory themselves, so here we
    only skip names already taken on disk. This is inherently best-effort: unlike
    :func:`create_snapshot_dir`, the name is not claimed atomically.
    """
    def is_taken(name: str) -> bool:
        return os.path.exists(name) or (gz and os.path.exists(archive_path_for(name)))

    for candidate in snapshot_dir_candidates(output_dir):
        if not is_taken(candidate):
            return candidate


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


def take_snapshot(host: str, workdir: Path, output_dir: str | None = None, gz: bool = False) -> Path:
    """
    Take a snapshot from a live cluster node and return the created directory, or
    the tar.gz archive when ``gz`` is set.
    """
    args = argparse.Namespace(cluster=host, port=None, user=None, password=None, min_tablet_size=None)
    old_cwd = Path.cwd()
    try:
        os.chdir(workdir)
        snapshot_dir = create_snapshot_dir(output_dir, gz=gz)
        with get_live_topology_source_from_args(args) as src:
            write_snapshot(snapshot_dir, src)
        if gz:
            return Path(workdir) / pack_snapshot(snapshot_dir)
        return Path(workdir) / snapshot_dir
    finally:
        os.chdir(old_cwd)


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

    snapshot_dir = create_snapshot_dir(args.output_dir, gz=args.gz)
    with get_live_topology_source_from_args(args) as src:
        write_snapshot(snapshot_dir, src)

    print(os.path.abspath(pack_snapshot(snapshot_dir) if args.gz else snapshot_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
