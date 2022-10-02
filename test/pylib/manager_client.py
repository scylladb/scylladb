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
from typing import List, Optional, Callable
import logging
import aiohttp                                                             # type: ignore
import aiohttp.web                                                         # type: ignore
from cassandra.cluster import Session as CassandraSession  # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster as CassandraCluster  # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)


class ManagerClient():
    """Helper Manager API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Manager server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
    # pylint: disable=too-many-instance-attributes
    conn: aiohttp.UnixConnector
    session: aiohttp.ClientSession

    def __init__(self, sock_path: str, port: int, use_ssl: bool,
                 con_gen: Optional[Callable[[List[str], int, bool], CassandraSession]]) -> None:
        self.port = port
        self.use_ssl = use_ssl
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

    async def stop(self):
        """Close client session and close driver"""
        await self.session.close()
        self.driver_close()

    async def driver_connect(self) -> None:
        """Connect to cluster"""
        if self.con_gen is not None:
            servers = await self.servers()
            logger.debug("driver connecting to %s", servers)
            self.ccluster = self.con_gen(servers, self.port, self.use_ssl)
            self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster"""
        if self.ccluster is not None:
            logger.debug("shutting down driver")
            self.ccluster.shutdown()
            self.ccluster = None
        self.cql = None

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            logger.debug("refresh driver node list")
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def before_test(self, test_case_name: str) -> None:
        """Before a test starts check if cluster needs cycling and update driver connection"""
        logger.debug("before_test for %s", test_case_name)
        dirty = await self.is_dirty()
        if dirty:
            self.driver_close()  # Close driver connection to old cluster
        resp = await self._get(f"/cluster/before-test/{test_case_name}")
        if resp.status != 200:
            raise RuntimeError(f"Failed before test check {await resp.text()}")
        if self.cql is None:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self.driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_case_name: str) -> None:
        """Tell harness this test finished"""
        logger.debug("after_test for %s", test_case_name)
        await self._get(f"/cluster/after-test")

    async def _get(self, resource: str) -> aiohttp.ClientResponse:
        # Can raise exception. See https://docs.aiohttp.org/en/latest/web_exceptions.html
        # NOTE: using Python requests style URI for Unix domain sockets to avoid using "localhost"
        resp = await self.session.get(self._resource_uri(resource))
        if resp.status != 200:
            text = await resp.text()
            raise Exception(f'status code: {resp.status}, body text: {text}')
        return resp

    async def _get_text(self, resource: str) -> str:
        resp = await self._get(resource)
        return await resp.text()

    async def _put_json(self, resource: str, json: dict) -> aiohttp.ClientResponse:
        return await self.session.request(method='PUT', url=self._resource_uri(resource), json=json)

    def _resource_uri(self, resource: str) -> str:
        return f"http+unix://{self.sock_name}{resource}"

    async def is_manager_up(self) -> bool:
        """Check if Manager server is up"""
        ret = await self._get_text("/up")
        return ret == "True"

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        ret = await self._get_text("/cluster/up")
        return ret == "True"

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        dirty = await self._get_text("/cluster/is-dirty")
        return dirty == "True"

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        resp = await self._get_text("/cluster/replicas")
        return int(resp)

    async def servers(self) -> List[str]:
        """Get list of running servers"""
        host_list = await self._get_text("/cluster/servers")
        return host_list.split(',')

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        await self._get_text("/cluster/mark-dirty")

    async def server_stop(self, server_id: str) -> None:
        """Stop specified server"""
        logger.debug("ManagerClient stopping %s", server_id)
        await self._get_text(f"/cluster/server/{server_id}/stop")

    async def server_stop_gracefully(self, server_id: str) -> None:
        """Stop specified server gracefully"""
        logger.debug("ManagerClient stopping gracefully %s", server_id)
        await self._get_text(f"/cluster/server/{server_id}/stop_gracefully")

    async def server_start(self, server_id: str) -> None:
        """Start specified server"""
        logger.debug("ManagerClient starting %s", server_id)
        await self._get_text(f"/cluster/server/{server_id}/start")
        self._driver_update()

    async def server_restart(self, server_id: str) -> None:
        """Restart specified server"""
        logger.debug("ManagerClient restarting %s", server_id)
        await self._get_text(f"/cluster/server/{server_id}/restart")
        self._driver_update()

    async def server_add(self) -> str:
        """Add a new server"""
        server_id = await self._get_text("/cluster/addserver")
        self._driver_update()
        logger.debug("ManagerClient added %s", server_id)
        return server_id

    async def server_remove(self, server_id: str) -> None:
        """Remove a specified server"""
        logger.debug("ManagerClient removing %s", server_id)
        await self._get_text(f"/cluster/removeserver/{server_id}")
        self._driver_update()

    async def server_get_config(self, server_id: str) -> dict[str, object]:
        resp = await self._get(f"/cluster/server/{server_id}/get_config")
        if resp.status != 200:
            raise Exception(await resp.text())
        return await resp.json()

    async def server_update_config(self, server_id: str, key: str, value: object) -> None:
        resp = await self._put_json(f"/cluster/server/{server_id}/update_config", {'key': key, 'value': value})
        if resp.status != 200:
            raise Exception(await resp.text())
