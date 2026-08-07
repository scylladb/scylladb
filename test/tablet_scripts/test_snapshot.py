#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import csv
import ipaddress
import os
import tarfile
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace
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


def test_pack_snapshot_produces_archive_that_unpacks_to_the_snapshot(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    write_snapshot_dir(tmp_path / "tablet_snap_260805_120000")

    archive = snapshot.pack_snapshot("tablet_snap_260805_120000")

    assert archive == "tablet_snap_260805_120000.tar.gz"
    # The directory is replaced by the archive.
    assert not os.path.exists("tablet_snap_260805_120000")
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


def test_pack_snapshot_keeps_nested_output_dir_in_place(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "out").mkdir()
    write_snapshot_dir(tmp_path / "out" / "snap")

    archive = snapshot.pack_snapshot("out/snap")

    # The archive sits next to the directory it replaced, and holds the leaf name only.
    assert archive == "out/snap.tar.gz"
    with tarfile.open(archive) as tar:
        assert "snap/system_tablets.csv" in tar.getnames()


def test_create_snapshot_dir_creates_and_avoids_clobbering(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    # The chosen name is created atomically and returned.
    assert snapshot.create_snapshot_dir("tablet_snap_260805_120000") == "tablet_snap_260805_120000"
    assert os.path.isdir("tablet_snap_260805_120000")

    # A second run with the same base finds the directory taken and picks a suffixed name.
    second = snapshot.create_snapshot_dir("tablet_snap_260805_120000")
    assert second != "tablet_snap_260805_120000"
    assert second.startswith("tablet_snap_260805_120000_")
    assert os.path.isdir(second)


def test_create_snapshot_dir_avoids_clobbering_an_existing_archive(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    Path("tablet_snap_260805_120000.tar.gz").write_bytes(b"")

    # With --gz the archive name has to be free too, so a suffix is added even though the
    # directory name itself is free.
    chosen = snapshot.create_snapshot_dir("tablet_snap_260805_120000", gz=True)
    assert chosen != "tablet_snap_260805_120000"
    assert chosen.startswith("tablet_snap_260805_120000_")


def test_choose_manual_snapshot_dir_does_not_create_and_avoids_clobbering(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    Path("tablet_snap_260805_120000.tar.gz").write_bytes(b"")

    # The manual workflow only names the directory; it must not create it.
    assert snapshot.choose_manual_snapshot_dir("tablet_snap_260805_120000") == "tablet_snap_260805_120000"
    assert not os.path.exists("tablet_snap_260805_120000")

    # With --gz the taken archive name forces a suffix.
    chosen = snapshot.choose_manual_snapshot_dir("tablet_snap_260805_120000", gz=True)
    assert chosen != "tablet_snap_260805_120000"
    assert chosen.startswith("tablet_snap_260805_120000_")


def read_lines(source) -> list[str]:
    with source() as lines:
        return list(lines)


def test_open_snapshot_files_reads_archive_with_files_at_root(tmp_path, monkeypatch) -> None:
    """
    Files are matched by base name, so an archive built without a wrapping directory
    entry is read too, not only the layout that pack_snapshot produces.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "system_tablets.csv").write_text(TABLETS_CSV)
    with tarfile.open("flat.tar.gz", "w:gz") as tar:
        tar.add("system_tablets.csv")

    files = topology.open_snapshot_files("flat.tar.gz")

    assert list(files) == [topology.TABLETS_TABLE]
    assert read_lines(files[topology.TABLETS_TABLE]) == TABLETS_CSV.splitlines(keepends=True)
    assert TopologyFromSnapshot("flat.tar.gz").get_topology().host_count() == 1


def test_open_snapshot_files_streams_a_directory_snapshot(tmp_path, monkeypatch) -> None:
    """
    A directory snapshot is read on the fly: opening the source yields a live file object
    rather than a buffer filled in by open_snapshot_files.
    """
    monkeypatch.chdir(tmp_path)
    write_snapshot_dir(tmp_path / "snap")

    files = topology.open_snapshot_files("snap")

    with files[topology.TABLETS_TABLE]() as lines:
        assert not lines.closed
        # Reading a single line does not consume the rest of the file.
        assert next(iter(lines)) == TABLETS_CSV.splitlines(keepends=True)[0]
    assert lines.closed

    assert read_lines(files[topology.TABLETS_TABLE]) == TABLETS_CSV.splitlines(keepends=True)


def test_open_snapshot_files_ignores_unrelated_archive_entries(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    write_snapshot_dir(tmp_path / "snap")
    (tmp_path / "snap" / "notes.txt").write_text("unrelated")
    archive = snapshot.pack_snapshot("snap")

    assert list(topology.open_snapshot_files(archive)) == [topology.TABLETS_TABLE]


def test_open_snapshot_files_rejects_a_non_archive_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    Path("not_a_snapshot").write_text("hello")

    with pytest.raises(Exception, match="neither a directory nor a tar archive"):
        topology.open_snapshot_files("not_a_snapshot")

    with pytest.raises(Exception, match="Snapshot does not exist"):
        topology.open_snapshot_files("missing")


def test_topology_from_archive_missing_required_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "system_topology.csv").write_text("")
    with tarfile.open("incomplete.tar.gz", "w:gz") as tar:
        tar.add("system_topology.csv")

    with pytest.raises(Exception, match="missing required file: system_tablets.csv"):
        TopologyFromSnapshot("incomplete.tar.gz")


def driver_rows(name: str, fields: list[str], rows: list[tuple]) -> list:
    """
    Builds rows shaped as the CQL driver returns them, i.e. named tuples over every column of
    the table, since snapshots are dumped from a "SELECT *".
    """
    row_type = namedtuple(name, fields)
    return [row_type(*row) for row in rows]


def fake_live_source() -> tuple[SimpleNamespace, dict]:
    """
    Stands in for LiveClusterTopologySource, returning it together with the rows it answers
    with. Its query for a table is the table itself, which is all write_snapshot() needs.
    """
    rows = {
        topology.TOPOLOGY_TABLE: driver_rows(
            "topology",
            ["key", "host_id", "shard_count", "node_state", "num_tokens", "version", "datacenter"],
            [("local", HOST1, 4, "normal", 256, 7, "dc1")]),
        topology.TABLETS_TABLE: driver_rows(
            "tablets",
            ["table_id", "last_token", "base_table", "keyspace_name", "table_name",
             "replicas", "new_replicas", "stage", "resize_seq_number"],
            [(TABLE, 0, None, "ks", "tbl", [(HOST1, 0)], None, "none", 0),
             (TABLE, 100, None, "ks", "tbl", [(HOST1, 3)], [(HOST1, 1)], "streaming", 0)]),
        topology.LOAD_PER_NODE_TABLE: driver_rows(
            "load_per_node",
            ["node", "dc", "rack", "ip", "storage_capacity", "effective_capacity"],
            [(HOST1, "dc1", "rack1", ipaddress.ip_address("10.0.0.1"), 1000, 800)]),
        topology.TABLET_SIZES_TABLE: driver_rows(
            "tablet_sizes",
            ["table_id", "last_token", "replicas"],
            [(TABLE, 0, {HOST1: 10}), (TABLE, 100, {HOST1: 30})]),
    }
    src = SimpleNamespace(queries={table: table for table in rows},
                          session=SimpleNamespace(
                              execute_async=lambda query: SimpleNamespace(result=lambda: rows[query])))
    return src, rows


def test_write_snapshot_dumps_every_table_and_reads_back_the_same_topology(tmp_path, monkeypatch) -> None:
    """
    A dump has to yield the topology its rows would have built directly, which is what the
    cluster test asserts against a real cluster.
    """
    monkeypatch.chdir(tmp_path)
    src, rows = fake_live_source()

    os.mkdir("snap")
    snapshot.write_snapshot("snap", src)

    assert sorted(os.listdir("snap")) == sorted(table.file for table in topology.SNAPSHOT_TABLES)

    expected = topology.Topology()
    expected._build({table.name: topology.rows_from_cql(table_rows, table.columns)
                     for table, table_rows in rows.items()})

    assert TopologyFromSnapshot("snap").get_topology() == expected


def test_write_snapshot_renders_collections_the_way_they_are_read_back(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    os.mkdir("snap")
    snapshot.write_snapshot("snap", fake_live_source()[0])

    with open(os.path.join("snap", topology.TABLETS_TABLE.file), newline="") as dump:
        header, first, second = list(csv.reader(dump, delimiter=topology.CSV_DELIMITER))

    # A collection is rendered by its column's Column.format, in the form parsing reads.
    assert first[header.index("replicas")] == f"{{({HOST1}, 0)}}"
    assert second[header.index("new_replicas")] == f"{{({HOST1}, 1)}}"
    # A null becomes the empty field that reading takes for a null.
    assert first[header.index("new_replicas")] == ""
    assert first[header.index("base_table")] == ""
    # Columns a snapshot is not read for are dumped all the same.
    assert first[header.index("resize_seq_number")] == "0"

    with open(os.path.join("snap", topology.TABLET_SIZES_TABLE.file), newline="") as dump:
        header, first, _ = list(csv.reader(dump, delimiter=topology.CSV_DELIMITER))

    assert first[header.index("replicas")] == f"{{{HOST1}: 10}}"


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
