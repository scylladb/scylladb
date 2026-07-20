#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import errno
import fcntl
import os
import random
import threading
from pathlib import Path

from test.pylib.pool import Pool
from typing import NewType, Optional

Host = NewType('Host', str)


class HostRegistry:
    _instance = None
    _initialized = False
    """A Scylla servers needs a unique IP address and working directory
    which we need to manage and share across many running tests. Store
    all shared external resources within this class to make sure
    nothing is leaked by the harness. Lease addresses with lease_host(),
    release with release_host(). Each returned address is from a unique
    class-B (/16) subnet created just for this test run. I.e. in X.Y.Z.W
    X.Y is unique for each test.py invocation, while Z and W together form
    a host counter that is unique across all hosts in a single run.
    """

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
        # HostRegistry is a singleton, so there should be no possibility to mess and overlap in IP for one use
        # however, when there are several users using the same machine for testing, they can overlap,
        # so this simple retry should help to eliminate the overlap and just find another free subnet
        max_attempts = 20
        for attempt in range(max_attempts):
            with threading.Lock():
                # Avoid 127.0.*.* since CCM (a different test framework)
                # assumes it will be available for it to run Scylla
                # instances. On the first attempt use the worker-derived
                # octet so xdist workers of the same run never clash; on
                # retries pick a random one to recover from a subnet that is
                # already in use by another test.py invocation.
                octet = second_octet if attempt == 0 else random.randrange(1, 255)
                self.subnet = "127.{}".format(octet)
                self.lock_filename: Optional[Path] = Path(os.getenv('TMPDIR', '/tmp')) / ('scylla-' + self.subnet)
                self.lock_file = self.lock_filename.open('w')
                try:
                    fcntl.lockf(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except OSError as e:
                    self.lock_file.close()
                    if e.errno not in (errno.EACCES, errno.EAGAIN):
                        raise
        else:
            raise RuntimeError(
                f"Failed to acquire a subnet lock after {max_attempts} attempts"
            )

        self.next_host_id = 0

        async def create_host() -> Host:
            self.next_host_id += 1
            # Use the 3rd and 4th octets together as the host counter within
            # the /16 subnet, e.g. 127.<second_octet>.<hi>.<lo>. This yields
            # far more addresses than a single class-C subnet.
            third_octet, fourth_octet = divmod(self.next_host_id, 256)
            return Host("{}.{}.{}".format(self.subnet, third_octet, fourth_octet))

        async def destroy_host(h: Host) -> None:
            # Doesn't matter, we never return hosts to the pool as 'dirty'.
            pass

        # A /16 subnet holds 65536 addresses; exclude the network (x.0.0) and
        # broadcast (x.255.255) addresses by counting from 1 to 65534.
        self.pool = Pool[Host](65534, create_host, destroy_host)

        async def cleanup() -> None:
            if self.lock_filename:
                self.lock_filename.unlink()
                self.lock_filename = None

        self.cleanup = cleanup

    async def lease_host(self) -> Host:
        return await self.pool.get()

    async def release_host(self, host: Host) -> None:
        return await self.pool.put(host, is_dirty=False)
