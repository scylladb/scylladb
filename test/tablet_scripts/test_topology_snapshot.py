#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace

import pytest

from tablets import topology
from tablets.topology import TopologyFromSnapshot


def test_snapshot_topology_source_reports_missing_snapshot_clearly(tmp_path) -> None:
    missing_path = tmp_path / "missing-snapshot"

    # A snapshot may be a directory or a tar archive, so the message names neither.
    with pytest.raises(Exception, match=rf"Snapshot does not exist: {missing_path}"):
        TopologyFromSnapshot(str(missing_path))


def test_get_topology_source_from_args_uses_env_snapshot_when_no_option_is_set(tmp_path, monkeypatch) -> None:
    snapshot_dir = tmp_path / "snapshot"
    snapshot_dir.mkdir()
    (snapshot_dir / "system_tablets.csv").write_text("")
    monkeypatch.setenv("SCYLLA_TABLET_SNAPSHOT", str(snapshot_dir))

    src = topology.get_topology_source_from_args(Namespace(
        snapshot=None,
        cluster=None,
        port=None,
        user=None,
        password=None,
        min_tablet_size=None,
        anonymize=False,
    ))

    assert isinstance(src, TopologyFromSnapshot)
    assert src.snapshot_dir == str(snapshot_dir)


def test_get_topology_source_from_args_prefers_explicit_options_over_env_snapshot(tmp_path, monkeypatch) -> None:
    env_snapshot_dir = tmp_path / "env-snapshot"
    env_snapshot_dir.mkdir()
    (env_snapshot_dir / "system_tablets.csv").write_text("")
    explicit_snapshot_dir = tmp_path / "explicit-snapshot"
    explicit_snapshot_dir.mkdir()
    (explicit_snapshot_dir / "system_tablets.csv").write_text("")
    monkeypatch.setenv("SCYLLA_TABLET_SNAPSHOT", str(env_snapshot_dir))

    src = topology.get_topology_source_from_args(Namespace(
        snapshot=str(explicit_snapshot_dir),
        cluster=None,
        port=None,
        user=None,
        password=None,
        min_tablet_size=None,
        anonymize=False,
    ))

    assert isinstance(src, TopologyFromSnapshot)
    assert src.snapshot_dir == str(explicit_snapshot_dir)

    live_src = Namespace()
    monkeypatch.setattr(topology, "get_live_topology_source_from_args", lambda args: live_src)
    src = topology.get_topology_source_from_args(Namespace(
        snapshot=None,
        cluster="127.0.0.1",
        port=None,
        user=None,
        password=None,
        min_tablet_size=None,
        anonymize=False,
    ))

    assert src is live_src
