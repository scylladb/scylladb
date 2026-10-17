#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import fcntl
import os
import random
from collections.abc import Iterator
from pathlib import Path
from typing import NewType


Host = NewType('Host', str)


def _open_lock_file(path: Path) -> int:
    """Read-only is enough for flock(), and lets us lock a file another user left behind.
    No O_CREAT on an existing file: in a sticky directory the kernel refuses that for a file
    someone else owns (fs.protected_regular). O_NONBLOCK: a FIFO planted there must not hang us."""
    try:
        return os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    except FileNotFoundError:
        return os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CREAT, 0o644)


class HostRegistry:
    """A Scylla servers needs a unique IP address and working directory
    which we need to manage and share across many running tests. Store
    all shared external resources within this class to make sure
    nothing is leaked by the harness. Lease addresses with lease_host(),
    release with release_host(). Each returned address is from a unique
    class-C subnet created just for this test run, so each address
    is quaranteed to have a unique 4th component. I.e. in X.Y.Z.W
    X.Y.Z is unique for each test.py invocation, and W is unique
    across all hosts in a single run.
    """
    LOCK_DIR = Path("/tmp/scylla-test")
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
        # To create a subnet let's randomly generate a number and put
        # a file with this name into a well-known directory. If the file
        # doesn't exist, the subnet is free. If it already exists and is
        # locked, there is another process running with this subnet and
        # we should try again with another subnet. If the file isn't
        # locked, it remains from some previous invocation and can be
        # locked and reused. The lock is held until the process exits, so
        # the subnet is ours for as long as anything of ours may be on it.
        #
        # The directory is shared by every run on the host, whoever started
        # it and from whatever container: dbuild bind-mounts it from the host,
        # as the rest of a container's /tmp is private to it. Anyone may
        # create a lock file in it (1777) and lock one another user left
        # behind (see _open_lock_file).
        try:
            self.LOCK_DIR.mkdir(0o1777)
            self.LOCK_DIR.chmod(0o1777)  # mkdir's mode went through the umask
        except FileExistsError:
            pass

        worker_id = os.getenv('PYTEST_XDIST_WORKER', 'gw0')
        second_octet = int(worker_id[2:]) + 1
        # HostRegistry is a singleton, so there should be no possibility to mess and overlap in IP for one use
        # however, when there are several users using the same machine for testing, they can overlap,
        # so this simple retry should help to eliminate the overlap and just find another random IP
        max_attempts = 20
        for attempt in range(max_attempts):
            # Avoid 127.0.*.* since CCM (a different test framework)
            # assumes it will be available for it to run Scylla
            # instances. 127.255.255.255 is also illegal.
            subnet = f"127.{second_octet}.{random.randrange(0, 255)}"
            self._lock_filename = self.LOCK_DIR / f"subnet-{subnet}"
            try:
                self._lock_fd = _open_lock_file(self._lock_filename)
            except PermissionError:  # someone else's, left with a umask stricter than ours
                continue
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                os.close(self._lock_fd)
        else:
            raise RuntimeError(
                f"Failed to acquire a subnet lock after {max_attempts} attempts"
            )

        # Never-yet-leased hosts, minted on demand: one for each of the 254 usable 4th-octet values.
        self._new_hosts: Iterator[Host] = (Host(f"{subnet}.{host_id}") for host_id in range(1, 255))

        # Hosts released by their previous user, available for reuse.
        self._released_hosts: list[Host] = []

        # Limits the number of leased hosts to the number of usable 4th-octet values: lease #255 waits for a release.
        self._semaphore = asyncio.Semaphore(254)

    async def lease_host(self) -> Host:
        await self._semaphore.acquire()
        if self._released_hosts:
            return self._released_hosts.pop(0)  # use the list as a FIFO queue
        return next(self._new_hosts)

    async def release_host(self, host: Host) -> None:
        self._released_hosts.append(host)
        self._semaphore.release()
