#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import errno
import os
import random
import socket
from collections.abc import Iterator
from typing import NewType


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
    # Abstract AF_UNIX socket name (leading NUL, see unix(7)) + the subnet, see __init__.
    LOCK_PREFIX = "\0scylla-test/subnet-"
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
        # A subnet is held by binding an abstract socket named after it. The name
        # lives in the kernel, not on disk, and is scoped to the network namespace:
        # exactly the runs that share our loopback, and so could collide on the
        # subnet, see it, whichever user or container they run in (dbuild runs with
        # --network host). bind() fails with EADDRINUSE while another run holds the
        # name. The kernel drops it when this process exits, however it exits, so
        # the subnet is ours for as long as this process runs; children that outlive
        # it are on their own (see ldap_server.kill_leaked_toxiproxy).
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
            self._lock = socket.socket(socket.AF_UNIX)
            try:
                self._lock.bind(self.LOCK_PREFIX + subnet)
                break
            except OSError as e:
                self._lock.close()
                if e.errno != errno.EADDRINUSE:
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

    async def lease_host(self) -> Host:
        await self._semaphore.acquire()
        if self._released_hosts:
            return self._released_hosts.pop(0)  # use the list as a FIFO queue
        return next(self._new_hosts)

    async def release_host(self, host: Host) -> None:
        self._released_hosts.append(host)
        self._semaphore.release()
