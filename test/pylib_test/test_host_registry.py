#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import random

from test.pylib.host_registry import HostRegistry


async def test_a_locked_subnet_is_skipped(tmp_path, monkeypatch):
    """Two runs that draw the same subnet must not both get it."""
    monkeypatch.setattr(HostRegistry, "LOCK_DIR", tmp_path / "locks")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")  # the worker id picks the second octet
    draws = iter([7, 7, 8])
    monkeypatch.setattr(random, "randrange", lambda *_: next(draws))
    registries = []
    for _ in range(2):
        monkeypatch.setattr(HostRegistry, "_instance", None)  # the real one is a session-wide singleton
        monkeypatch.setattr(HostRegistry, "_initialized", False)
        registries.append(HostRegistry())
    first, second = registries
    assert (await first.lease_host()).startswith("127.1.7.")
    assert (await second.lease_host()).startswith("127.1.8.")
    assert ((tmp_path / "locks").stat().st_mode & 0o7777) == 0o1777
