#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import asyncio

from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import IPAddress
from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.host_registry import HostRegistry
from cassandra.cluster import ConsistencyLevel

logger = logging.getLogger(__name__)

# Refs #27429
# Transposed/adapted from dtest with same name
async def do_test_internode_compression_between_datacenters(manager: ManagerClient, compression: str, verifier) -> None:
    """
    Verify that compression between datacenters is compressed if internode_compression is set to dc and
    not compressed for intra-dc communication.
    """

    class Stats:
        def __init__(self):
            self.bytes_written = 0
            self.max_packet_size = 0

        def add(self, n: int):
            self.bytes_written = self.bytes_written + n;
            self.max_packet_size = max(self.max_packet_size, n)

    # Simple python async io proxy type
    class Proxy:
        def __init__(self, listen_addr: str, dest_addr: str, port:int = 7000):
            self.list_addr = listen_addr
            self.dest_addr = dest_addr
            self.port = port
            self.server = None
            self.stats = Stats()
            self.conn_id = 0

        def reset(self):
            self.stats = Stats()

        async def start(self):
            # IO worker per direction
            async def pipe(reader, writer, src, dst, cid):
                logger.debug("pipe %s %s -> %s:%s", cid, src, dst, self.port)
                try:
                    written = 0
                    while not reader.at_eof():
                        buf = await reader.read(64*1024)
                        n = len(buf)
                        if n == 0:
                            break
                        self.stats.add(n)
                        written = written + n
                        writer.write(buf)
                        await writer.drain()
                        #logger.debug("pipe wrote %s %s -> %s:%s, %s", cid, src, dst, self.port, n)

                    if writer.can_write_eof():
                        writer.write_eof()
                except Exception as e:
                    logger.debug("pipe error %s %s -> %s:%s, %s (%s)", cid, src, dst, self.port, e, e.__class__)
                finally:
                    logger.debug("end pipe %s %s -> %s:%s", cid, src, dst, self.port)
                    writer.close()
                    await writer.wait_closed()

            # Invoked by asyncio.start_server accept
            async def handle_client(local_reader, local_writer):
                cid = self.conn_id
                self.conn_id += 1
                logger.debug("Connection %s %s -> %s %s", cid, self.list_addr, self.dest_addr, self.port)
                while True:
                    try:
                        # Connect can/will fail repeatedly during startup. Since we 
                        # need to emulate being a scylla connected to, and we've already
                        # been accepted, we can't really return "nope". So just keep trying...
                        remote_reader, remote_writer = await asyncio.open_connection(self.dest_addr, self.port)
                        pipe1 = pipe(local_reader, remote_writer, self.list_addr, self.dest_addr, cid)
                        pipe2 = pipe(remote_reader, local_writer, self.dest_addr, self.list_addr, cid)
                        await asyncio.gather(pipe1, pipe2)
                    except ConnectionRefusedError as e:
                        # retry!
                        logger.debug("Connection refused %s %s -> %s:%s (%s)", cid, self.list_addr, self.dest_addr, self.port, e)
                        continue
                    except Exception as e:
                        logger.debug("Connection error %s %s -> %s:%s, %s (%s)", cid, self.list_addr, self.dest_addr, self.port, e, e.__class__)
                    finally:
                        local_writer.close()
                        await local_writer.wait_closed()
                    break
            # creates an accepting server
            self.server = await asyncio.start_server(handle_client,self.list_addr, self.port, keep_alive=True)

        async def run(self):
            async with self.server:
                await self.server.serve_forever()

        async def stop(self):
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()

    logger.info("Creating a new cluster of 2 nodes in 1st DC and 1 node in 2nd DC")

    hosts = HostRegistry()
    dcs = [('dc1','rack1'), ('dc1', 'rack2'), ('dc2', 'rack3')]
    proxy_addrs = [ (await hosts.lease_host(),dc,rack) for dc,rack in dcs]
    seeds = [IPAddress(addr) for addr,_,_ in proxy_addrs]
    seeds = [proxy_addrs[0][0]]
    config = {"internode_compression": compression, "ssl_storage_port": 0 }

    # create unstarted servers
    # use our proxy addresses as broadcast so we can insert proxies
    servers = [(await manager.server_add(config=config | { 'broadcast_address':addr }, start=False,
                                         property_file={"dc": dc, "rack": rack}), addr)
                                         for addr, dc, rack in proxy_addrs]

    # now create our proxies and start them
    proxies = [Proxy(addr, s.ip_addr) for s,addr in servers]
    for p in proxies:
        await p.start()

    proxy_futs = [p.run() for p in proxies]

    # Now we can start servers
    await asyncio.gather(*[manager.server_start(s.server_id, seeds=seeds, connect_driver=True) for s,_ in servers])
    # And connect the driver.
    await manager.driver_connect(servers[0][0])

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "with replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1 }") as ks:
        async with new_test_table(manager, ks, "key int PRIMARY KEY, val TEXT") as table:

            msg_size = 8192
            insert_stmt = cql.prepare(f"insert into {table} (key, val) values (?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.ALL

            # reset all stats. only want IO metrics for the actual insert
            for p in proxies:
                p.reset()

            # insert a compressible row of size ~8 kiB
            def insert_once():
                cql.execute(insert_stmt, [1, "1" * msg_size])

            # need to do non-blocking
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, insert_once)

            node1_proxy = proxies[0]
            node2_proxy = proxies[1]
            node3_proxy = proxies[2]

            verifier(msg_size, node1_proxy, node2_proxy, node3_proxy)

    await asyncio.gather(*[manager.server_stop(s.server_id) for s,_ in servers])
    await asyncio.gather(*[p.stop() for p in proxies])


async def test_internode_compression_compress_packets_between_nodes(request, manager: ManagerClient) -> None:
    def check_expected(msg_size, node1_proxy, node2_proxy, node3_proxy):
        # get the stats
        max_intra_pkg = max(node1_proxy.stats.max_packet_size, node2_proxy.stats.max_packet_size)
        max_dc_pkg = max(node3_proxy.stats.max_packet_size, node3_proxy.stats.max_packet_size)

        expected = msg_size / 2
        assert max_dc_pkg < expected
        assert max_intra_pkg < expected

    await do_test_internode_compression_between_datacenters(manager, "all", check_expected)

async def test_internode_compression_between_datacenters(request, manager: ManagerClient) -> None:
    def check_expected(msg_size, node1_proxy, node2_proxy, node3_proxy):
        # get the stats
        max_intra_pkg = max(node1_proxy.stats.max_packet_size, node2_proxy.stats.max_packet_size)
        max_dc_pkg = max(node3_proxy.stats.max_packet_size, node3_proxy.stats.max_packet_size)

        expected = msg_size / 2
        assert max_dc_pkg < expected
        assert max_intra_pkg > msg_size

    await do_test_internode_compression_between_datacenters(manager, "dc", check_expected)

