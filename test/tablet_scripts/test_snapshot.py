#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import os
import re
import tarfile
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

from tablets import snapshot
from tablets import topology
from tablets.topology import TopologyFromSnapshot


TABLE = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
HOST1 = UUID("11111111-1111-1111-1111-111111111111")

TABLETS_CSV = (
    "table_id;keyspace_name;table_name;last_token;replicas;new_replicas;stage\n"
    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;ks;tbl;0;"
    "[(11111111-1111-1111-1111-111111111111, 0)];;none\n"
)


def write_snapshot_dir(path: Path) -> None:
    path.mkdir()
    (path / "system_tablets.csv").write_text(TABLETS_CSV)


def read_lines(source) -> list[str]:
    with source() as lines:
        return list(lines)


def test_pack_snapshot_produces_archive_that_unpacks_to_the_snapshot(tmp_path) -> None:
    snapshot_dir = tmp_path / "tablet_snap_260805_120000"
    write_snapshot_dir(snapshot_dir)

    archive = snapshot.pack_snapshot(str(snapshot_dir))

    assert archive == f"{snapshot_dir}.tar.gz"
    # The directory is replaced by the archive.
    assert not snapshot_dir.exists()
    assert tarfile.is_tarfile(archive)

    # The snapshot directory is the single top-level entry, so unpacking recreates
    # a directory the analysis scripts can read.
    with tarfile.open(archive) as tar:
        assert sorted(tar.getnames()) == [
            "tablet_snap_260805_120000",
            "tablet_snap_260805_120000/system_tablets.csv",
        ]
        tar.extractall(tmp_path / "unpacked")

    topo = TopologyFromSnapshot(str(tmp_path / "unpacked" / "tablet_snap_260805_120000")).get_topology()
    assert topo.host_count() == 1

    # ... and the archive itself is readable without unpacking, yielding the same topology.
    assert TopologyFromSnapshot(archive).get_topology() == topo


def test_pack_snapshot_keeps_nested_output_dir_in_place(tmp_path) -> None:
    (tmp_path / "out").mkdir()
    write_snapshot_dir(tmp_path / "out" / "snap")

    archive = snapshot.pack_snapshot(str(tmp_path / "out" / "snap"))

    # The archive sits next to the directory it replaced, and holds the leaf name only.
    assert archive == f"{tmp_path / 'out' / 'snap'}.tar.gz"
    with tarfile.open(archive) as tar:
        assert "snap/system_tablets.csv" in tar.getnames()


def test_create_snapshot_dir_creates_the_requested_name(tmp_path) -> None:
    # The chosen name is created atomically and returned.
    created = snapshot.create_snapshot_dir("tablet_snap_260805_120000", workdir=tmp_path)

    assert Path(created) == tmp_path / "tablet_snap_260805_120000"
    assert Path(created).is_dir()


def test_create_snapshot_dir_refuses_a_requested_name_already_taken(tmp_path) -> None:
    """
    A requested name is honoured exactly or fails; it is never silently redirected.
    """
    created = snapshot.create_snapshot_dir("tablet_snap_260805_120000", workdir=tmp_path)

    with pytest.raises(Exception, match=rf"Snapshot already exists: {re.escape(created)}$"):
        snapshot.create_snapshot_dir("tablet_snap_260805_120000", workdir=tmp_path)


def test_create_snapshot_dir_refuses_when_only_the_archive_is_taken(tmp_path) -> None:
    archive = tmp_path / "tablet_snap_260805_120000.tar.gz"
    archive.write_bytes(b"")

    # With --gz the archive name has to be free too, even though the directory name is.
    with pytest.raises(Exception, match=rf"Snapshot already exists: {re.escape(str(archive))}$"):
        snapshot.create_snapshot_dir("tablet_snap_260805_120000", gz=True, workdir=tmp_path)


def test_create_snapshot_dir_suffixes_a_timestamped_name_on_collision(tmp_path, monkeypatch) -> None:
    """
    A timestamped name always yields a fresh directory, suffixing past any collision.
    """
    class FixedDatetime:
        """Pins the clock so both calls hit the same base name."""
        @staticmethod
        def now():
            return datetime(2026, 8, 5, 12, 0, 0)

    monkeypatch.setattr(snapshot, "datetime", FixedDatetime)

    first = snapshot.create_snapshot_dir(None, workdir=tmp_path)
    second = snapshot.create_snapshot_dir(None, workdir=tmp_path)

    assert Path(first) == tmp_path / "tablet_snap_260805_120000"
    assert Path(second).name.startswith("tablet_snap_260805_120000_")
    assert Path(second).is_dir()


def test_create_snapshot_dir_writes_under_workdir_without_changing_the_cwd(tmp_path) -> None:
    """
    Writing elsewhere never moves the process-global working directory.
    """
    cwd_before = Path.cwd()

    created = snapshot.create_snapshot_dir("tablet_snap_260805_120000", workdir=tmp_path)

    assert Path(created) == tmp_path / "tablet_snap_260805_120000"
    assert Path(created).is_dir()
    assert Path.cwd() == cwd_before


def test_choose_manual_snapshot_dir_does_not_create_and_refuses_a_taken_name(tmp_path) -> None:
    requested = str(tmp_path / "tablet_snap_260805_120000")
    Path(f"{requested}.tar.gz").write_bytes(b"")

    # The manual workflow only names the directory; it must not create it.
    assert snapshot.choose_manual_snapshot_dir(requested) == requested
    assert not Path(requested).exists()

    # With --gz the taken archive name is a conflict the caller has to resolve.
    with pytest.raises(Exception, match=rf"Snapshot already exists: {re.escape(requested)}$"):
        snapshot.choose_manual_snapshot_dir(requested, gz=True)


def test_open_snapshot_files_reads_archive_with_files_at_root(tmp_path) -> None:
    """
    Files are matched by base name, so an archive built without a wrapping directory
    entry is read too, not only the layout that pack_snapshot produces.
    """
    (tmp_path / "system_tablets.csv").write_text(TABLETS_CSV)
    archive = tmp_path / "flat.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tmp_path / "system_tablets.csv", arcname="system_tablets.csv")

    files = topology.open_snapshot_files(str(archive))

    assert list(files) == [topology.TABLETS_TABLE]
    assert read_lines(files[topology.TABLETS_TABLE]) == TABLETS_CSV.splitlines(keepends=True)
    assert TopologyFromSnapshot(str(archive)).get_topology().host_count() == 1


def test_open_snapshot_files_streams_a_directory_snapshot(tmp_path) -> None:
    """
    A directory snapshot is read on the fly: opening the source yields a live file object
    rather than a buffer filled in by open_snapshot_files.
    """
    write_snapshot_dir(tmp_path / "snap")

    files = topology.open_snapshot_files(str(tmp_path / "snap"))

    with files[topology.TABLETS_TABLE]() as lines:
        assert not lines.closed
        # Reading a single line does not consume the rest of the file.
        assert next(iter(lines)) == TABLETS_CSV.splitlines(keepends=True)[0]
    assert lines.closed

    assert read_lines(files[topology.TABLETS_TABLE]) == TABLETS_CSV.splitlines(keepends=True)


def test_open_snapshot_files_ignores_unrelated_archive_entries(tmp_path) -> None:
    write_snapshot_dir(tmp_path / "snap")
    (tmp_path / "snap" / "notes.txt").write_text("unrelated")
    archive = snapshot.pack_snapshot(str(tmp_path / "snap"))

    assert list(topology.open_snapshot_files(archive)) == [topology.TABLETS_TABLE]


def test_open_snapshot_files_rejects_a_non_archive_file(tmp_path) -> None:
    (tmp_path / "not_a_snapshot").write_text("hello")

    with pytest.raises(Exception, match="neither a directory nor a tar archive"):
        topology.open_snapshot_files(str(tmp_path / "not_a_snapshot"))

    with pytest.raises(Exception, match="Snapshot does not exist"):
        topology.open_snapshot_files(str(tmp_path / "missing"))


def test_topology_from_archive_missing_required_file(tmp_path) -> None:
    (tmp_path / "system_topology.csv").write_text("")
    archive = tmp_path / "incomplete.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tmp_path / "system_topology.csv", arcname="system_topology.csv")

    with pytest.raises(Exception, match=r"missing required file: system_tablets\.csv"):
        TopologyFromSnapshot(str(archive))


def test_dump_with_cqlsh_leaves_out_a_table_it_fails_to_dump(tmp_path, monkeypatch, capsys) -> None:
    """
    Every table is dumped with its own cqlsh COPY. One which fails, as a table an older
    cluster lacks does, is reported and left out, rather than failing the rest.
    """
    # Stands in for cqlsh: dumps system.tablets, and fails on every other table after
    # leaving a partial file behind.
    fake_cqlsh = tmp_path / "cqlsh"
    fake_cqlsh.write_text(f"""#!/bin/sh
query="$2"
path=$(printf '%s' "$query" | sed -n "s/.* TO '\\([^']*\\)'.*/\\1/p")
case "$query" in
  "COPY system.tablets "*) printf '%s' '{TABLETS_CSV}' > "$path" ;;
  *) echo partial > "$path"; echo "no such table" >&2; exit 2 ;;
esac
""")
    fake_cqlsh.chmod(0o755)
    monkeypatch.setattr(topology, "CQLSH", str(fake_cqlsh))
    args = argparse.Namespace(cluster=None, port=None, user=None, password=None)

    snapshot_dir = tmp_path / "snap"
    snapshot_dir.mkdir()
    topology.dump_with_cqlsh(args, str(snapshot_dir))

    assert os.listdir(snapshot_dir) == [topology.TABLETS_TABLE.file]
    assert "Failed to dump system.topology: no such table" in capsys.readouterr().err
    assert TopologyFromSnapshot(str(snapshot_dir)).get_topology().host_count() == 1


def test_dump_with_cqlsh_finds_the_installed_cqlsh_off_path(tmp_path, monkeypatch) -> None:
    """A --nonroot install leaves cqlsh off PATH, in the share/cassandra/bin of the same prefix."""
    installed = tmp_path / "share" / "cassandra" / "bin"
    installed.mkdir(parents=True)
    (installed / "cqlsh").write_text("#!/bin/sh\necho installed cqlsh ran >&2\nexit 1\n")
    (installed / "cqlsh").chmod(0o755)
    monkeypatch.setattr(topology, "INSTALLED_CQLSH_DIR", str(installed))
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    args = argparse.Namespace(cluster=None, port=None, user=None, password=None)

    with pytest.raises(Exception, match="Failed to dump system.tablets: installed cqlsh ran"):
        topology.dump_with_cqlsh(args, str(tmp_path))


def test_copy_commands_double_an_apostrophe_in_the_path() -> None:
    args = argparse.Namespace(cluster=None, port=None, user=None, password=None)

    (_, cmd), *_ = topology.cqlsh_copy_commands(args, "bob's")

    assert "TO 'bob''s/system_topology.csv'" in cmd[-1]


def test_manual_instructions_quote_the_commands_for_a_shell(capsys) -> None:
    args = argparse.Namespace(cluster="10.0.0.1", port=19042, user="u", password="p q", gz=False)

    snapshot.print_manual_snapshot_instructions(args, "snap")

    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[2] == ("cqlsh -u u -p 'p q' 10.0.0.1 19042 -e 'COPY system.tablets TO '\"'\"'snap/system_tablets.csv'\"'\"'"
                        " WITH DELIMITER='\"'\"';'\"'\"' AND HEADER=TRUE AND CONFIGFILE='\"'\"'/dev/null'\"'\"''")


def test_manual_instructions_pack_the_snapshot_with_gz(capsys) -> None:
    args = argparse.Namespace(cluster="127.0.0.1", port=None, user=None, password=None, gz=True)

    snapshot.print_manual_snapshot_instructions(args, "tablet_snap_260805_120000")

    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0] == "mkdir -p tablet_snap_260805_120000"
    assert lines[-1] == ("tar -czf tablet_snap_260805_120000.tar.gz tablet_snap_260805_120000"
                         " && rm -r tablet_snap_260805_120000")


def test_manual_instructions_omit_packing_without_gz(capsys) -> None:
    args = argparse.Namespace(cluster="127.0.0.1", port=None, user=None, password=None, gz=False)

    snapshot.print_manual_snapshot_instructions(args, "tablet_snap_260805_120000")

    out = capsys.readouterr().out
    assert "tar -czf" not in out
