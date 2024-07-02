#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import asyncio
import pytest
import time
import logging
from test.topology.conftest import skip_mode
from cassandra.cqltypes import Int32Type
import requests
import re

logger = logging.getLogger(__name__)

async def wait_for_view(cql, name, node_count):
    deadline = time.time() + 120
    while time.time() < deadline:
        done = await cql.run_async(f"SELECT COUNT(*) FROM system_distributed.view_build_status WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING")
        if done[0][0] == node_count:
            return
        else:
            time.sleep(0.2)
    raise Exception("Timeout waiting for views to build")


def get_metrics(addr):
    # The Prometheus API is on port 9180, and always http
    addr = 'http://' + addr + ':9180/metrics'
    resp = requests.get(addr)
    if resp.status_code != 200:
        pytest.skip('Metrics port 9180 is not available')
    response = requests.get(addr)
    assert response.status_code == 200
    return response.text

def get_metric(metrics, name, requested_labels=None):
    the_metrics = get_metrics(metrics)
    total = 0.0
    lines = re.compile('^'+name+'{.*$', re.MULTILINE)
    for match in re.findall(lines, the_metrics):
        a = match.split()
        metric = a[0]
        val = float(a[1])
        # Check if match also matches the requested labels
        if requested_labels:
            # we know metric begins with name{ and ends with } - the labels
            # are what we have between those
            got_labels = metric[len(name)+1:-1].split(',')
            # Check that every one of the requested labels is in got_labels:
            for k, v in requested_labels.items():
                if not f'{k}="{v}"' in got_labels:
                    # No match for requested label, skip this metric (python
                    # doesn't have "continue 2" so let's just set val to 0...
                    val = 0
                    break
        total += float(val)
    return total

def get_replicas(cql, key):
    return cql.cluster.metadata.get_replicas("ks", Int32Type.serialize(key, cql.cluster.protocol_version))

async def enable_on_servers(manager, servers, injection, oneshot=False):
    errs = [manager.api.enable_injection(s.ip_addr, injection, oneshot) for s in servers]
    return await asyncio.gather(*errs)
async def disable_on_servers(manager, servers, injection):
    errs = [manager.api.disable_injection(s.ip_addr, injection) for s in servers]
    return await asyncio.gather(*errs)

def get_remote_key(cql, local_node):
    i = 0
    while local_node == get_replicas(cql, i)[0]:
        i = i + 1
    return i

# This test reproduces issue #18542
# In the test, we create a table and perform a write to it a couple of times
# Each time, we check that a view update backlog on some shard increased
# due to the write.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_view_backlog_increased_after_write(manager: ManagerClient) -> None:
    node_count = 2
    # Use a higher smp to make it more likely that the writes go to a different shard than the coordinator.
    servers = await manager.servers_add(node_count, cmdline=['--smp', '5'])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE ks.tab (key int, c int, v text, PRIMARY KEY (key, c))")
    await cql.run_async(f"CREATE MATERIALIZED VIEW ks.mv_cf_view AS SELECT * FROM ks.tab "
                    "WHERE c IS NOT NULL and key IS NOT NULL PRIMARY KEY (c, key) ")
    await wait_for_view(cql, 'mv_cf_view', node_count)

    key = int(time.time())
    logger.info(f"Base table key: {key}")
    local_node = get_replicas(cql, key)[0]
    # Only remote updates hold on to memory, so make the update remote
    remote_key = get_remote_key(cql, local_node)
    for v in [1000, 4000, 16000, 64000, 256000]:
        # Make sure the view update backlog is still high (view udpate hasn't finished) when the replica returns
        await enable_on_servers(manager, servers, "never_finish_remote_view_updates")

        await cql.run_async(f"INSERT INTO ks.tab (key, c, v) VALUES ({key}, {remote_key}, '{v*'a'}')")
        # The view update backlog should increase on the node generating view updates
        view_backlog = get_metric(local_node.address, 'scylla_storage_proxy_replica_view_update_backlog')
        # The read view_backlog might still contain backlogs from the previous iterations, so we only assert that it is large enough
        assert view_backlog > v
        await disable_on_servers(manager, servers, "never_finish_remote_view_updates")

    await cql.run_async(f"DROP KEYSPACE ks")
