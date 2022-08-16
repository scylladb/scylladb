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

from typing import List, Optional, Callable
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore
from cassandra.cluster import Session as CassandraSession                  # type: ignore
from cassandra.cluster import Cluster as CassandraCluster                  # type: ignore


class ManagerClient():
    """Helper Manager API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Manager server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
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
        await self._request(f"http://localhost/cluster/before-test/{test_name}")
        if self.cql is None:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self.driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_name: str) -> None:
        """Tell harness this test finished"""
        await self._request(f"http://localhost/cluster/after-test/{test_name}")

    async def _request(self, url: str) -> str:
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        resp = await self.session.get(url)
        return await resp.text()

    async def is_manager_up(self) -> bool:
        """Check if Manager server is up"""
        ret = await self._request("http://localhost/up")
        return ret == "True"

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        ret = await self._request("http://localhost/cluster/up")
        return ret == "True"

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._request("http://localhost/cluster/is-dirty")
        return dirty == "True"

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._request("http://localhost/cluster/replicas")
        return int(resp)

    async def servers(self) -> List[str]:
        """Get list of running servers"""
        host_list = await self._request("http://localhost/cluster/servers")
        return host_list.split(',')

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        await self._request("http://localhost/cluster/mark-dirty")

    async def server_stop(self, server_id: str) -> bool:
        """Stop specified node"""
        ret = await self._request(f"http://localhost/cluster/node/{server_id}/stop")
        return ret == "OK"

    async def server_stop_gracefully(self, server_id: str) -> bool:
        """Stop specified node gracefully"""
        ret = await self._request(f"http://localhost/cluster/node/{server_id}/stop_gracefully")
        return ret == "OK"

    async def server_start(self, server_id: str) -> bool:
        """Start specified node"""
        ret = await self._request(f"http://localhost/cluster/node/{server_id}/start")
        self._driver_update()
        return ret == "OK"

    async def server_restart(self, server_id: str) -> bool:
        """Restart specified node"""
        ret = await self._request(f"http://localhost/cluster/node/{server_id}/restart")
        self._driver_update()
        return ret == "OK"

    async def server_add(self) -> str:
        """Add a new node"""
        server_id = await self._request("http://localhost/cluster/addnode")
        self._driver_update()
        return server_id

    async def server_remove(self, server_id: str) -> None:
        """Remove a specified node"""
        await self._request(f"http://localhost/cluster/removenode/{server_id}")
        self._driver_update()
