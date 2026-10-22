#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import os
import random

import pytest

from test.pylib import host_registry
from test.pylib.host_registry import HostRegistry


def _fresh_registry(monkeypatch) -> HostRegistry:
    monkeypatch.setattr(HostRegistry, "_instance", None)  # the real one is a session-wide singleton
    monkeypatch.setattr(HostRegistry, "_initialized", False)
    return HostRegistry()


async def test_a_locked_subnet_is_skipped(tmp_path, monkeypatch):
    """Two runs that draw the same subnet must not both get it, whatever their umask."""
    monkeypatch.setattr(HostRegistry, "LOCK_DIR", tmp_path / "locks")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")  # the worker id picks the second octet
    draws = iter([7, 7, 8])
    monkeypatch.setattr(random, "randrange", lambda *_: next(draws))
    umask = os.umask(0o077)
    try:
        first = _fresh_registry(monkeypatch)
        second = _fresh_registry(monkeypatch)
    finally:
        os.umask(umask)
    assert (await first.lease_host()).startswith("127.1.7.")
    assert (await second.lease_host()).startswith("127.1.8.")
    assert ((tmp_path / "locks").stat().st_mode & 0o7777) == 0o1777
    assert ((tmp_path / "locks" / "subnet-127.1.7").stat().st_mode & 0o777) == 0o644  # readable by other users


async def test_a_planted_symlink_is_not_followed(tmp_path, monkeypatch):
    """A lock path that is a symlink is skipped, and whatever it points at keeps its mode."""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    private = tmp_path / "private"
    private.touch(0o600)
    (lock_dir / "subnet-127.1.7").symlink_to(private)
    monkeypatch.setattr(HostRegistry, "LOCK_DIR", lock_dir)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    draws = iter([7, 8])
    monkeypatch.setattr(random, "randrange", lambda *_: next(draws))
    assert (await _fresh_registry(monkeypatch).lease_host()).startswith("127.1.8.")
    assert (private.stat().st_mode & 0o777) == 0o600


def test_refused_lock_files_are_reported(tmp_path, monkeypatch):
    """Twenty refusals must not end in a bare 'failed after 20 attempts'."""
    def refuse(path):
        raise PermissionError(f"{path}: not ours and not readable")
    monkeypatch.setattr(HostRegistry, "LOCK_DIR", tmp_path / "locks")
    monkeypatch.setattr(host_registry, "_open_lock_file", refuse)
    with pytest.raises(RuntimeError) as error:
        _fresh_registry(monkeypatch)
    assert isinstance(error.value.__cause__, PermissionError)
