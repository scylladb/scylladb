#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Manager client.
   Communicates with Manager server via socket.
   Provides helper methods to test cases.
   Manages driver refresh when cluster is cycled.
"""

import os.path
from typing import List, Optional, Callable, NamedTuple
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore
from cassandra.cluster import Session as CassandraSession  # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster as CassandraCluster  # type: ignore # pylint: disable=no-name-in-module


class ManagerClient():
    """Helper Manager API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Manager server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
    # pylint: disable=too-many-instance-attributes
    conn: aiohttp.UnixConnector
    session: aiohttp.ClientSession

    def __init__(self, sock_path: str, port: int, ssl: bool,
                 con_gen: Optional[Callable[[List[str], int, bool], CassandraSession]]) -> None:
        self.port = port
        self.ssl = ssl
        self.con_gen = con_gen
        self.ccluster: Optional[CassandraCluster] = None
        self.cql: Optional[CassandraSession] = None
        # API
        self.sock_path = sock_path
        self.sock_name = os.path.basename(sock_path)

    async def start(self):
        """Setup connection to Manager server"""
        self.conn = aiohttp.UnixConnector(path=self.sock_path)
        self.session = aiohttp.ClientSession(connector=self.conn)

    async def driver_connect(self) -> None:
        """Connect to cluster"""
        if self.con_gen is not None:
            self.ccluster = self.con_gen(await self.servers(), self.port, self.ssl)
            self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster"""
        if self.ccluster is not None:
            self.ccluster.shutdown()
            self.ccluster = None
        self.cql = None

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def before_test(self, test_name: str) -> None:
        """Before a test starts check if cluster needs cycling and update driver connection"""
        dirty = await self.is_dirty()
        if dirty:
            self.driver_close()  # Close driver connection to old cluster
        resp = await self._request(f"/cluster/before-test/{test_name}")
        assert resp.status == 200, f"Could not run before test checks for test {test_name}"
        if self.cql is None:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self.driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_name: str) -> None:
        """Tell harness this test finished"""
        await self._request(f"/cluster/after-test/{test_name}")

    class RequestReturn(NamedTuple):
        """Return status and message for API requests"""
        status: int
        text: str

    async def _request(self, resource: str) -> RequestReturn:
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        async with self.session.get(f"http+unix://{self.sock_name}{resource}") as resp:
            # NOTE: always await text to avoid possible issues with aiohttp
            ret = ManagerClient.RequestReturn(resp.status, await resp.text())
        return ret

    async def is_manager_up(self) -> bool:
        """Check if Manager server is up"""
        resp = await self._request("/up")
        return resp.status == 200 and resp.text == "Up"

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        resp = await self._request("/cluster/up")
        return resp.status == 200 and resp.text == "Up"

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        resp = await self._request("/cluster/is-dirty")
        return resp.status == 200 and resp.text == "Dirty"

    async def replicas(self) -> Optional[int]:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request("/cluster/replicas")
        if resp.status == 200:
            return int(resp.text)
        return None

    async def servers(self) -> List[str]:
        """Get list of running servers"""
        resp = await self._request("/cluster/servers")
        if resp.status == 200:
            return resp.text.split(',')
        return []

    async def mark_dirty(self) -> bool:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        resp = await self._request("/cluster/mark-dirty")
        return resp.status == 200 and resp.text == "OK"

    async def server_stop(self, server_id: str) -> bool:
        """Stop specified server"""
        resp = await self._request(f"/cluster/server/{server_id}/stop")
        return resp.status == 200

    async def server_stop_gracefully(self, server_id: str) -> bool:
        """Stop specified server gracefully"""
        resp = await self._request(f"/cluster/server/{server_id}/stop_gracefully")
        return resp.status == 200

    async def server_start(self, server_id: str) -> bool:
        """Start specified server"""
        resp = await self._request(f"/cluster/server/{server_id}/start")
        if resp.status == 200:
            self._driver_update()
            return True
        return False

    async def server_restart(self, server_id: str) -> bool:
        """Restart specified server"""
        servers = await self.servers()
        if not servers or server_id not in servers:
            return False
        if len(servers) == 1:
            # Only 1 server, so close connection and reopen fresh afterwards
            self.driver_close()
            resp = await self._request(f"/cluster/server/{server_id}/restart")
            if resp.status == 200:
                await self.driver_connect()
                return True
        elif len(servers) > 1:
            # Multiple servers, make sure other nodes are known by the driver
            self._driver_update()
            resp = await self._request(f"/cluster/server/{server_id}/restart")
            if resp.status == 200:
                return True
        return False

    async def server_add(self) -> Optional[str]:
        """Add a new server"""
        resp = await self._request("/cluster/addserver")
        if resp.status == 200:
            server_id = resp.text
            self._driver_update()
            return server_id
        return None

    async def server_remove(self, server_id: str) -> bool:
        """Remove a specified server"""
        resp = await self._request(f"/cluster/removeserver/{server_id}")
        if resp.status == 200:
            self._driver_update()
            return True
        return False

    async def start_stopped(self) -> None:
        """Start all previously stopped servers"""
        resp = await self._request(f"/cluster/start_stopped")
        if resp.status == 200:
            self._driver_update()
            return True
        return False
