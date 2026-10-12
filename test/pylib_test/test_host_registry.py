#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import errno
import random
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from test.pylib.host_registry import HostRegistry


@pytest.fixture
def lock_prefix(monkeypatch, tmp_path) -> str:
    """Locks of this test only: the real prefix is shared with every test.py run on the host.
    Every registry tries 127.1 (the gw0 worker's octet) first, then 127.2, 127.3, ..."""
    prefix = f"\0{tmp_path}/subnet-"
    monkeypatch.setattr(HostRegistry, "LOCK_PREFIX", prefix)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setattr(random, "shuffle", lambda _: None)
    return prefix


def _fresh_registry(monkeypatch) -> HostRegistry:
    monkeypatch.setattr(HostRegistry, "_instance", None)  # the real one is a session-wide singleton
    monkeypatch.setattr(HostRegistry, "_initialized", False)
    return HostRegistry()


def _hold(name: str) -> socket.socket:
    """What another run holding the subnet looks like to us, whoever and wherever it runs."""
    lock = socket.socket(socket.AF_UNIX)
    lock.bind(name)
    return lock


async def test_a_held_subnet_is_skipped(lock_prefix, monkeypatch):
    held = _hold(lock_prefix + "127.1")
    assert (await _fresh_registry(monkeypatch).lease_host()).startswith("127.2.")
    held.close()
    assert (await _fresh_registry(monkeypatch).lease_host()).startswith("127.1.")


def test_all_subnets_held(lock_prefix, monkeypatch):
    held = [_hold(f"{lock_prefix}127.{octet}") for octet in range(1, 255)]
    with pytest.raises(RuntimeError, match="all 127.<1..254> subnets are in use"):
        _fresh_registry(monkeypatch)
    for lock in held:
        lock.close()


def test_the_lock_dies_with_its_process(lock_prefix, monkeypatch):
    """A SIGKILLed test.py releases its subnet without any cleanup of its own."""
    run = subprocess.Popen([sys.executable, "-c", f"""
import asyncio, time
from test.pylib.host_registry import HostRegistry
HostRegistry.LOCK_PREFIX = {lock_prefix!r}
print(asyncio.run(HostRegistry().lease_host()), flush=True)
time.sleep(60)
"""], cwd=Path(__file__).parents[2], stdout=subprocess.PIPE, text=True)
    try:
        assert run.stdout.readline().startswith("127.1.")
        with pytest.raises(OSError) as error:
            _hold(lock_prefix + "127.1")
        assert error.value.errno == errno.EADDRINUSE
    finally:
        run.kill()
        run.wait()
    _hold(lock_prefix + "127.1").close()
