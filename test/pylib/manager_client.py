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
