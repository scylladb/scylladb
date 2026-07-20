#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import errno
import fcntl
import os
import random
from collections.abc import Iterator
from pathlib import Path
from typing import NewType, Optional


Host = NewType('Host', str)


class HostRegistry:
    """A Scylla servers needs a unique IP address and working directory
    which we need to manage and share across many running tests. Store
    all shared external resources within this class to make sure
    nothing is leaked by the harness. Lease addresses with lease_host(),
    release with release_host(). Each returned address is from a unique
    class-B (/16) subnet created just for this test run. I.e. in X.Y.Z.W
    X.Y is unique for each test.py invocation, while Z and W together form
    a host counter that is unique across all hosts in a single run.
    """
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if HostRegistry._initialized:
            return
        HostRegistry._initialized = True

        # Imagine multiple instances of test.py run concurrently.
        # Each will be trying to start and stop Scylla servers.
        # If different runs share the same Scylla IP pool, Scyllas may
        # fail to bind CQL port, and tests will fail. So let's
        # give each run its own class B network in 127.*.*.* range.
        # Each scylla server will get an IP in this network.
        #
        # Why not simply give each Scylla a unique IP direved from its own
        # pid? The pid changes between restarts, and harness does start and
        # stops Scyllas.
        #
        # A subnet is identified by a lock file placed into /tmp. If the file
        # doesn't exist, the subnet is free. If it already exists and is
        # locked, there is another process running with this subnet and we
        # should try again with another subnet. If the file isn't locked, it
        # remains from some previous invocation and can be locked and reused.
        worker_id = os.getenv('PYTEST_XDIST_WORKER', 'gw0')
        second_octet = int(worker_id[2:]) + 1
        # Try the worker-derived octet first, so xdist workers of the same run never clash, then
        # every other octet once in random order, to find a subnet not used by another test.py
        # invocation on the same machine. Avoid 127.0.*.* since CCM (a different test framework)
        # assumes it will be available for it to run Scylla instances.
        other_octets = [octet for octet in range(1, 255) if octet != second_octet]
        random.shuffle(other_octets)
        for octet in [second_octet, *other_octets]:
            subnet = f"127.{octet}"
            self._lock_filename: Optional[Path] = Path("/tmp") / f"scylla-{subnet}"
            self._lock_file = self._lock_filename.open("w")
            try:
                fcntl.lockf(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as e:
                self._lock_file.close()
                if e.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
        else:
            raise RuntimeError("Failed to acquire a subnet lock: all 127.<1..254> subnets are in use")

        # Never-yet-leased hosts, minted on demand as 127.<octet>.<hi>.<lo>, skipping the .0 and .255
        # 4th octets. Older test.py branches lease class-C subnets 127.<octet>.<0..254> under other
        # lock file names, so start with hi=255, which they never use: released hosts are reused
        # first, so a run that holds at most 254 hosts at once never clashes with them.
        # Hosts past #254 (scale tests only) can still clash with an older-branch run on the same machine.
        self._new_hosts: Iterator[Host] = (Host(f"{subnet}.{hi}.{lo}")
                                           for hi in (255, *range(255)) for lo in range(1, 255))

        # Hosts released by their previous user, available for reuse.
        self._released_hosts: list[Host] = []

        # Limits the number of leased hosts to the number of minted ones (256 3rd x 254 4th octets):
        # lease #65025 waits for a release.
        self._semaphore = asyncio.Semaphore(256 * 254)

    async def cleanup(self) -> None:
        if self._lock_filename:
            self._lock_filename.unlink()
            self._lock_filename = None

    async def lease_host(self) -> Host:
        await self._semaphore.acquire()
        if self._released_hosts:
            return self._released_hosts.pop(0)  # use the list as a FIFO queue
        return next(self._new_hosts)

    async def release_host(self, host: Host) -> None:
        self._released_hosts.append(host)
        self._semaphore.release()
