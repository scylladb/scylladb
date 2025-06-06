#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""Manager client.
   Communicates with Manager server via socket.
   Provides helper methods to test cases.
   Manages driver refresh when cluster is cycled.
"""
import pathlib
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional, Callable, Any, Awaitable, Dict, Tuple, Union
from time import time
import logging
from test.pylib.log_browsing import ScyllaLogFile
from test.pylib.rest_client import UnixRESTClient, ScyllaRESTAPIClient, ScyllaMetricsClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, Host
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.scylla_cluster import ReplaceConfig, ScyllaServer
from cassandra.cluster import Session as CassandraSession, \
    ExecutionProfile, EXEC_PROFILE_DEFAULT  # type: ignore # pylint: disable=no-name-in-module
from cassandra.policies import LoadBalancingPolicy, RoundRobinPolicy, WhiteListRoundRobinPolicy
from cassandra.cluster import Cluster as CassandraCluster  # type: ignore # pylint: disable=no-name-in-module
from cassandra.auth import AuthProvider
import aiohttp
import asyncio

import universalasync


logger = logging.getLogger(__name__)


class NoSuchProcess(Exception):
    ...


@universalasync.wrap
class ManagerClient:
    """Helper Manager API client
    Args:
        sock_path (str): path to an AF_UNIX socket where Manager server is listening
        con_gen (Callable): generator function for CQL driver connection to a cluster
    """
    # pylint: disable=too-many-public-methods

    def __init__(self, sock_path: str, port: int, use_ssl: bool, auth_provider: Any|None,
                 con_gen: Callable[[List[IPAddress], int, bool, Any, LoadBalancingPolicy], CassandraCluster]) \
                         -> None:
        self.test_log_fh: Optional[logging.FileHandler] = None
        self.port = port
        self.use_ssl = use_ssl
        self.auth_provider = auth_provider
        self.load_balancing_policy = RoundRobinPolicy()
        self.con_gen = con_gen
        self.ccluster: Optional[CassandraCluster] = None
        self.cql: Optional[CassandraSession] = None
        # A client for communicating with ScyllaClusterManager (server)
        self.sock_path = sock_path
        self.client_for_asyncio_loop = {asyncio.get_running_loop(): UnixRESTClient(sock_path)}
        self.api = ScyllaRESTAPIClient()
        self.metrics = ScyllaMetricsClient()
        self.thread_pool = ThreadPoolExecutor()
        self.test_finished_event = asyncio.Event()

    @property
    def client(self):
        if self.test_finished_event.is_set():
            raise Exception("ManagerClient.after_test method was called, client object is not accessible anymore")
            # there are still can be issue when some task first obtains the client object,
            # but usually, tests obtains the manager and uses the client as manager.client
            # and there is an actual workaround for this case.
            # It is better to fix the task rather than every time doing
            # close all clients in after_test->create new during after_test->close again after after_test.
        _client = self.client_for_asyncio_loop.get(asyncio.get_running_loop(), None)
        if _client is None:
            _client = UnixRESTClient(self.sock_path)
            self.client_for_asyncio_loop[asyncio.get_running_loop()] = _client
        return _client

    async def stop(self):
        """Close driver"""
        self.driver_close()
        # TODO: good candidate for safe_gather  https://github.com/scylladb/scylladb/pull/17781
        #  to make sure that all connections is closed
        await asyncio.gather(*[client.shutdown() for client in self.client_for_asyncio_loop.values()])

    async def driver_connect(self, server: Optional[ServerInfo] = None, auth_provider: Optional[AuthProvider] = None) -> None:
        """Connect to cluster"""
        targets = [server] if server else await self.running_servers()
        servers = [s_info.rpc_address for s_info in targets]
        # avoids leaking connections if driver wasn't closed before
        self.driver_close()
        logger.debug("driver connecting to %s", servers)
        self.ccluster = self.con_gen(servers, self.port, self.use_ssl,
                                     auth_provider if auth_provider else self.auth_provider, self.load_balancing_policy)
        self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster"""
        if self.ccluster is not None:
            logger.debug("shutting down driver")
            self.ccluster.shutdown()
            self.ccluster = None
        self.cql = None

    def get_cql(self) -> CassandraSession:
        """Precondition: driver is connected"""
        assert self.cql
        return self.cql

    # More robust version of get_cql, when topology changes
    # or cql statement is executed immediately after driver_connect
    # it may fail unless we perform additional readiness checks
    async def get_ready_cql(self, servers: List[ServerInfo]) -> tuple[CassandraSession, list[Host]]:
        """Precondition: driver is connected"""
        cql = self.get_cql()
        await self.servers_see_each_other(servers)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + 60)
        return cql, hosts

    async def get_cql_exclusive(self, server: ServerInfo):
        cql = self.con_gen([server.ip_addr], self.port, self.use_ssl, self.auth_provider,
                                     WhiteListRoundRobinPolicy([server.ip_addr])).connect()
        await wait_for_cql_and_get_hosts(cql, [server], time() + 60)
        return cql

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            logger.debug("refresh driver node list")
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def before_test(self, test_case_name: str, test_log: Path) -> None:
        # Add handler to the root logger to intercept all logs produced by pytest process
        test_logger = logging.getLogger()
        self.test_log_fh = logging.FileHandler(test_log, mode='w+')
        # to have the custom formatter with a timestamp that used in a test.py but for each testcase's log, we need to
        # extract it from the root logger and apply to the handler
        self.test_log_fh.setFormatter(logging.getLogger().handlers[0].formatter)
        self.test_log_fh.setLevel(test_logger.getEffectiveLevel())
        test_logger.addHandler(self.test_log_fh)
        # Before a test starts check if cluster needs cycling and update driver connection
        logger.debug("before_test for %s", test_case_name)
        dirty = await self.is_dirty()
        if dirty:
            self.driver_close()  # Close driver connection to old cluster
        try:
            cluster_str = await self.client.put_json(f"/cluster/before-test/{test_case_name}", timeout=600,
                                                     response_type = "json")
            logger.info(f"Using cluster: {cluster_str} for test {test_case_name}")
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"Failed before test check {exc}") from exc
        servers = await self.running_servers()
        if self.cql is None and servers:
            # TODO: if cluster is not up yet due to taking long and HTTP timeout, wait for it
            # await self._wait_for_cluster()
            await self.driver_connect()  # Connect driver to new cluster

    async def after_test(self, test_case_name: str, success: bool) -> None:
        """Tell harness this test finished"""
        self.test_finished_event.set()
        _client = self.client_for_asyncio_loop.get(asyncio.get_running_loop())
        logging.getLogger().removeHandler(self.test_log_fh)
        pathlib.Path(self.test_log_fh.baseFilename).unlink()
        logger.debug("after_test for %s (success: %s)", test_case_name, success)
        cluster_status = await _client.put_json(f"/cluster/after-test/{success}",
                                                 response_type = "json")
        logger.info("Cluster after test %s: %s", test_case_name, cluster_status)

        return cluster_status

    async def gather_related_logs(self, failed_test_path_dir: Path, logs: Dict[str, Path]) -> None:
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            shutil.copyfile(log_file.file, failed_test_path_dir / f"{pathlib.Path(log_file.file).name}")
        for name, log in logs.items():
            shutil.copyfile(log, failed_test_path_dir / name)

    async def is_manager_up(self) -> bool:
        """Check if Manager server is up"""
        return await self.client.get_json("/up")

    async def is_cluster_up(self) -> bool:
        """Check if cluster is up"""
        return await self.client.get_json("/cluster/up")

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        return await self.client.get_json("/cluster/is-dirty")

    async def replicas(self) -> int:
        """Get number of configured replicas for the cluster (replication factor)"""
        return await self.client.get_json("/cluster/replicas")

    async def running_servers(self) -> list[ServerInfo]:
        """Get List of server info (id and IP address) of running servers"""
        try:
            server_info_list = await self.client.get_json("/cluster/running-servers")
        except RuntimeError as exc:
            raise Exception("Failed to get list of running servers") from exc
        assert isinstance(server_info_list, list), "running_servers got unknown data type"
        return [ServerInfo(ServerNum(int(info[0])), IPAddress(info[1]), IPAddress(info[2]), info[3], info[4])
                for info in server_info_list]

    async def all_servers(self) -> list[ServerInfo]:
        """Get List of server info (id and IP address) of all servers"""
        try:
            server_info_list = await self.client.get_json("/cluster/all-servers")
        except RuntimeError as exc:
            raise Exception("Failed to get list of servers") from exc
        assert isinstance(server_info_list, list), "all_servers got unknown data type"
        return [ServerInfo(ServerNum(int(info[0])), IPAddress(info[1]), IPAddress(info[2]), info[3], info[4])
                for info in server_info_list]

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        await self.client.put_json("/cluster/mark-dirty")

    async def mark_clean(self) -> None:
        """Manually mark current cluster not dirty.
           To be used when a current cluster wants to be reused."""
        await self.client.put_json("/cluster/mark-clean")

    async def server_stop(self, server_id: ServerNum) -> None:
        """Stop specified server"""
        logger.debug("ManagerClient stopping %s", server_id)
        await self.client.put_json(f"/cluster/server/{server_id}/stop")

    async def server_stop_gracefully(self, server_id: ServerNum, timeout: float = 60) -> None:
        """Stop specified server gracefully"""
        logger.debug("ManagerClient stopping gracefully %s", server_id)
        await self.client.put_json(f"/cluster/server/{server_id}/stop_gracefully", timeout=timeout)

    async def server_start(self,
                           server_id: ServerNum,
                           expected_error: str | None = None,
                           wait_others: int = 0,
                           wait_interval: float = 45,
                           seeds: list[IPAddress] | None = None,
                           timeout: float | None = None,
                           connect_driver: bool = True,
                           expected_server_up_state: ServerUpState = ServerUpState.CQL_QUERIED,
                           cmdline_options_override: list[str] | None = None,
                           append_env_override: dict[str, str] | None = None) -> None:
        """Start specified server and optionally wait for it to learn of other servers.

        Replace CLI options and environment variables with `cmdline_options_override` and `append_env_override`
        if provided.
        """
        logger.debug("ManagerClient starting %s", server_id)
        data = {
            "expected_error": expected_error,
            "seeds": seeds,
            "expected_server_up_state": expected_server_up_state.name,
            "cmdline_options_override": cmdline_options_override,
            "append_env_override": append_env_override,
        }
        await self.client.put_json(f"/cluster/server/{server_id}/start", data, timeout=timeout)
        await self.server_sees_others(server_id, wait_others, interval = wait_interval)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            else:
                await self.driver_connect()

    async def server_restart(self, server_id: ServerNum, wait_others: int = 0,
                             wait_interval: float = 45) -> None:
        """Restart specified server and optionally wait for it to learn of other servers"""
        await self.server_stop_gracefully(server_id)
        await self.server_start(server_id=server_id, wait_others=wait_others, wait_interval=wait_interval)

    async def rolling_restart(self, servers: List[ServerInfo], with_down: Optional[Callable[[ServerInfo], Awaitable[Any]]] = None):
        # `servers` might not include all the running servers, but we want to check against all of them
        servers_running = await self.running_servers()

        for s in servers:
            await self.server_stop_gracefully(s.server_id)

            # Wait for other servers to see the server to be stopped
            # so that the later server_sees_other_server() call will not
            # exit immediately, making it moot.
            for s2 in servers_running:
                if s2.server_id != s.server_id:
                    await self.server_not_sees_other_server(s2.ip_addr, s.ip_addr)

            if with_down:
                up_servers = [u for u in servers if u.server_id != s.server_id]
                await wait_for_cql_and_get_hosts(self.cql, up_servers, time() + 60)
                await with_down(s)

            await self.server_start(s.server_id)

            # Wait for other servers to see the restarted server.
            # Otherwise, the next server we are going to restart may not yet see "s" as restarted
            # and will not send graceful shutdown message to it. Server "s" may learn about the
            # restart from gossip later and close connections while we already sent CQL requests
            # to it, which will cause them to time out. Refs #14746.
            for s2 in servers_running:
                if s2.server_id != s.server_id:
                    await self.server_sees_other_server(s2.ip_addr, s.ip_addr)

        await wait_for_cql_and_get_hosts(self.cql, servers_running, time() + 60)

    async def server_pause(self, server_id: ServerNum) -> None:
        """Pause the specified server."""
        logger.debug("ManagerClient pausing %s", server_id)
        await self.client.put_json(f"/cluster/server/{server_id}/pause")

    async def server_unpause(self, server_id: ServerNum) -> None:
        """Unpause the specified server."""
        logger.debug("ManagerClient unpausing %s", server_id)
        await self.client.put_json(f"/cluster/server/{server_id}/unpause")

    async def server_wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str) -> None:
        """Delete all files for the given table from the data directory"""
        logger.debug("ManagerClient wiping sstables on %s, keyspace=%s, table=%s", server_id, keyspace, table)
        await self.client.put_json(f"/cluster/server/{server_id}/wipe_sstables", {"keyspace": keyspace, "table": table})

    async def server_get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        """Get the total size of all sstable files for the given table"""
        return await self.client.get_json(f"/cluster/server/{server_id}/sstables_disk_usage", params={"keyspace": keyspace, "table": table})

    def _create_server_add_data(self,
                                replace_cfg: Optional[ReplaceConfig],
                                cmdline: Optional[List[str]],
                                config: Optional[dict[str, Any]],
                                property_file: Union[List[dict[str, Any]], dict[str, Any], None],
                                start: bool,
                                seeds: Optional[List[IPAddress]],
                                expected_error: Optional[str],
                                server_encryption: Optional[str],
                                expected_server_up_state: Optional[ServerUpState]) -> dict[str, Any]:
        data: dict[str, Any] = {'start': start}
        if replace_cfg:
            data['replace_cfg'] = replace_cfg._asdict()
        if cmdline:
            data['cmdline'] = cmdline
        if config:
            data['config'] = config
        if property_file:
            data['property_file'] = property_file
        if seeds:
            data['seeds'] = seeds
        if expected_error:
            data['expected_error'] = expected_error
        if server_encryption:
            data['server_encryption'] = server_encryption
        if expected_server_up_state:
            data['expected_server_up_state'] = expected_server_up_state.name
        return data

    async def server_add(self,
                         replace_cfg: Optional[ReplaceConfig] = None,
                         cmdline: Optional[List[str]] = None,
                         config: Optional[dict[str, Any]] = None,
                         property_file: Optional[dict[str, Any]] = None,
                         start: bool = True,
                         expected_error: Optional[str] = None,
                         seeds: Optional[List[IPAddress]] = None,
                         timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT,
                         server_encryption: str = "none",
                         expected_server_up_state: Optional[ServerUpState] = None,
                         connect_driver: bool = True) -> ServerInfo:
        """Add a new server"""
        try:
            data = self._create_server_add_data(
                replace_cfg,
                cmdline,
                config,
                property_file,
                start,
                seeds,
                expected_error,
                server_encryption,
                expected_server_up_state,
            )

            # If we replace, we should wait until other nodes see the node being
            # replaced as dead because the replace operation can be rejected if
            # the node being replaced is considered alive. However, we sometimes
            # do not want to wait, for example, when we test that replace fails
            # as expected. Therefore, we make waiting optional and default.
            if replace_cfg and replace_cfg.wait_replaced_dead:
                replaced_ip = await self.get_host_ip(replace_cfg.replaced_id)
                await self.others_not_see_server(replaced_ip)

            server_info = await self.client.put_json("/cluster/addserver", data, response_type="json",
                                                     timeout=timeout)
        except Exception as exc:
            raise Exception("Failed to add server") from exc
        try:
            s_info = ServerInfo(ServerNum(int(server_info["server_id"])),
                                IPAddress(server_info["ip_addr"]),
                                IPAddress(server_info["rpc_address"]),
                                server_info["datacenter"],
                                server_info["rack"])
        except Exception as exc:
            raise RuntimeError(f"server_add got invalid server data {server_info}") from exc
        logger.debug("ManagerClient added %s", s_info)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect()
        return s_info

    async def servers_add(self, servers_num: int = 1,
                          cmdline: Optional[List[str]] = None,
                          config: Optional[dict[str, Any]] = None,
                          property_file: Union[List[dict[str, Any]], dict[str, Any], None] = None,
                          start: bool = True,
                          seeds: Optional[List[IPAddress]] = None,
                          driver_connect_opts: dict[str, Any] = {},
                          expected_error: Optional[str] = None,
                          server_encryption: str = "none",
                          auto_rack_dc: Optional[str] = None) -> List[ServerInfo]:
        """Add new servers concurrently.
        This function can be called only if the cluster uses consistent topology changes, which support
        concurrent bootstraps. If your test does not fulfill this condition and you want to add multiple
        servers, you should use multiple server_add calls."""
        assert servers_num > 0, f"servers_add: cannot add {servers_num} servers, servers_num must be positive"
        assert not (property_file and auto_rack_dc), f"Either property_file or auto_rack_dc can be provided, but not both"

        if auto_rack_dc:
            property_file = [{"dc":auto_rack_dc, "rack":f"rack{i+1}"} for i in range(servers_num)]

        try:
            data = self._create_server_add_data(None, cmdline, config, property_file, start, seeds, expected_error, server_encryption, None)
            data['servers_num'] = servers_num
            server_infos = await self.client.put_json("/cluster/addservers", data, response_type="json",
                                                      timeout=ScyllaServer.TOPOLOGY_TIMEOUT * servers_num)
        except Exception as exc:
            raise Exception("Failed to add servers") from exc

        assert len(server_infos) == servers_num, f"servers_add requested adding {servers_num} servers but " \
                                    f"got server data about {len(server_infos)} servers: {server_infos}"
        s_infos = list[ServerInfo]()
        for server_info in server_infos:
            try:
                s_info = ServerInfo(ServerNum(int(server_info["server_id"])),
                                    IPAddress(server_info["ip_addr"]),
                                    IPAddress(server_info["rpc_address"]),
                                    server_info["datacenter"],
                                    server_info["rack"])
                s_infos.append(s_info)
            except Exception as exc:
                raise RuntimeError(f"servers_add got invalid server data {server_info}") from exc

        logger.debug("ManagerClient added %s", s_infos)
        if expected_error is None:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect(**driver_connect_opts)
        return s_infos

    async def remove_node(self, initiator_id: ServerNum, server_id: ServerNum,
                          ignore_dead: List[IPAddress] | List[HostID] = list[IPAddress](),
                          expected_error: str | None = None,
                          wait_removed_dead: bool = True,
                          timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Invoke remove node Scylla REST API for a specified server"""
        logger.debug("ManagerClient remove node %s on initiator %s", server_id, initiator_id)

        # If we remove a node, we should wait until other nodes see it as dead
        # because the removenode operation can be rejected if the node being
        # removed is considered alive. However, we sometimes do not want to
        # wait, for example, when we test that removenode fails as expected.
        # Therefore, we make waiting optional and default.
        if wait_removed_dead:
            removed_ip = await self.get_host_ip(server_id)
            await self.others_not_see_server(removed_ip)

        data = {"server_id": server_id, "ignore_dead": ignore_dead, "expected_error": expected_error}
        await self.client.put_json(f"/cluster/remove-node/{initiator_id}", data,
                                   timeout=timeout)
        self._driver_update()

    async def decommission_node(self, server_id: ServerNum,
                                expected_error: str | None = None,
                                timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to decommission with Scylla REST API"""
        logger.debug("ManagerClient decommission %s", server_id)
        data = {"expected_error": expected_error}
        await self.client.put_json(f"/cluster/decommission-node/{server_id}", data,
                                   timeout=timeout)
        self._driver_update()

    async def rebuild_node(self, server_id: ServerNum,
                           expected_error: str | None = None,
                           timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to rebuild with Scylla REST API"""
        logger.debug("ManagerClient rebuild %s", server_id)
        data = {"expected_error": expected_error}
        await self.client.put_json(f"/cluster/rebuild-node/{server_id}", data,
                                   timeout=timeout)
        self._driver_update()

    async def server_get_config(self, server_id: ServerNum) -> dict[str, object]:
        data = await self.client.get_json(f"/cluster/server/{server_id}/get_config")
        assert isinstance(data, dict), f"server_get_config: got {type(data)} expected dict"
        return data

    async def server_update_config(self, server_id: ServerNum, key: str, value: object) -> None:
        await self.client.put_json(f"/cluster/server/{server_id}/update_config",
                                   {"key": key, "value": value})

    async def server_update_cmdline(self, server_id: ServerNum, cmdline_options: List[str]) -> None:
        await self.client.put_json(f"/cluster/server/{server_id}/update_cmdline",
                                   {"cmdline_options": cmdline_options})

    async def server_change_ip(self, server_id: ServerNum) -> IPAddress:
        """Change server IP address. Applicable only to a stopped server"""
        ret = await self.client.put_json(f"/cluster/server/{server_id}/change_ip", {},
                                         response_type="json")
        return IPAddress(ret["ip_addr"])

    async def server_change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Change server RPC IP address.

        Applicable only to a stopped server.
        """
        ret = await self.client.put_json(
            resource_uri=f"/cluster/server/{server_id}/change_rpc_address",
            data={},
            response_type="json",
        )
        rpc_address = ret["rpc_address"]

        logger.debug("ManagerClient has changed RPC IP for server %s to %s", server_id, rpc_address)
        return IPAddress(rpc_address)

    async def wait_for_host_known(self, dst_server_ip: IPAddress, expect_host_id: HostID,
                                  deadline: Optional[float] = None) -> None:
        """Waits until dst_server_id knows about expect_host_id, with timeout"""
        async def host_is_known():
            host_id_map = await self.api.get_host_id_map(dst_server_ip)
            return True if any(entry for entry in host_id_map if entry['value'] == expect_host_id) else None

        return await wait_for(host_is_known, deadline or (time() + 30))

    async def wait_for_scylla_process_status(self,
                                             server_id: ServerNum,
                                             expected_statuses: list[str],
                                             deadline: Optional[float] = None) -> str:
        """Wait for Scylla's process status for server_id will be as expected, with timeout."""
        async def process_status_is_as_expected() -> str | None:
            current_status = await self.client.get_json(f"/cluster/server/{server_id}/process_status")
            if current_status in expected_statuses:
                return current_status

        return await wait_for(process_status_is_as_expected, deadline or (time() + 30))

    async def get_host_ip(self, server_id: ServerNum) -> IPAddress:
        """Get host IP Address"""
        try:
            server_ip = await self.client.get_json(f"/cluster/host-ip/{server_id}")
        except Exception as exc:
            raise Exception(f"Failed to get host IP address for server {server_id}") from exc
        return IPAddress(server_ip)

    async def get_host_id(self, server_id: ServerNum) -> HostID:
        """Get local host id of a server"""
        try:
            host_id = await self.client.get_json(f"/cluster/host-id/{server_id}")
        except Exception as exc:
            raise Exception(f"Failed to get local host id address for server {server_id}") from exc
        return HostID(host_id)

    async def get_table_id(self, keyspace: str, table: str):
        rows = await self.cql.run_async(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
        return rows[0].id

    async def get_view_id(self, keyspace: str, view: str):
        rows = await self.cql.run_async(f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{view}'")
        return rows[0].id

    async def get_table_or_view_id(self, keyspace: str, table: str):
        rows = await self.cql.run_async(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
        if len(rows) > 0:
            return rows[0].id
        rows = await self.cql.run_async(f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{table}'")
        return rows[0].id

    async def server_sees_others(self, server_id: ServerNum, count: int, interval: float = 45.):
        """Wait till a server sees a minimum given count of other servers"""
        if count < 1:
            return
        server_ip = await self.get_host_ip(server_id)
        async def _sees_min_others():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if len(alive_nodes) > count:
                return True
        await wait_for(_sees_min_others, time() + interval, period=.5)

    async def server_sees_other_server(self, server_ip: IPAddress, other_ip: IPAddress,
                                       interval: float = 45.):
        """Wait till a server sees another specific server IP as alive"""
        async def _sees_another_server():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if other_ip in alive_nodes:
                return True
        await wait_for(_sees_another_server, time() + interval, period=.5)

    async def servers_see_each_other(self, servers: List[ServerInfo], interval: float = 45.):
        """Wait till all servers see all other servers in the list"""
        others = [self.server_sees_others(srv.server_id, len(servers) - 1, interval) for srv in servers]
        await asyncio.gather(*others)

    async def server_not_sees_other_server(self, server_ip: IPAddress, other_ip: IPAddress,
                                           interval: float = 45.):
        """Wait till a server sees another specific server IP as dead"""
        async def _not_sees_another_server():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if not other_ip in alive_nodes:
                return True
        await wait_for(_not_sees_another_server, time() + interval, period=.5)

    async def others_not_see_server(self, server_ip: IPAddress, interval: float = 45.):
        """Wait till a server is seen as dead by all other running servers in the cluster"""
        others_ips = [srv.ip_addr for srv in await self.running_servers() if srv.ip_addr != server_ip]
        await asyncio.gather(*(self.server_not_sees_other_server(ip, server_ip, interval)
                               for ip in others_ips))

    async def server_open_log(self, server_id: ServerNum) -> ScyllaLogFile:
        logger.debug("ManagerClient getting log filename for %s", server_id)
        log_filename = await self.client.get_json(f"/cluster/server/{server_id}/get_log_filename")
        return ScyllaLogFile(self.thread_pool, log_filename)

    async def server_get_workdir(self, server_id: ServerNum) -> str:
        return await self.client.get_json(f"/cluster/server/{server_id}/workdir")

    async def server_get_maintenance_socket_path(self, server_id: ServerNum) -> str:
        return await self.client.get_json(f"/cluster/server/{server_id}/maintenance_socket_path")

    async def server_get_exe(self, server_id: ServerNum) -> str:
        return await self.client.get_json(f"/cluster/server/{server_id}/exe")

    async def server_get_returncode(self, server_id: ServerNum) -> int | None:
        match await self.client.get_json(f"/cluster/server/{server_id}/returncode"):
            case "NO_SUCH_PROCESS":
                raise NoSuchProcess(f"No process found for {server_id=}")
            case "RUNNING":
                return None
            case returncode:
                return int(returncode)
