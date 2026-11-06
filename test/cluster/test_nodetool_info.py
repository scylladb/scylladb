#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.cluster.util import run_nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest


# Ported from scylla-dtest nodetool_test.py::TestNodetool::test_correct_dc_rack_in_nodetool_info.
# On ccm's populate([2, 2]) (dc1 and dc2, GossipingPropertyFileSnitch) with
# the nodes' racks set to rack0, rack1, rack0, rack1, "nodetool info" of
# every node reports its own data center and rack, with no stderr output
# (CASSANDRA-10382).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_nodetool_info_reports_own_dc_and_rack(manager: ScyllaClusterManager):
    locations = [{"dc": dc, "rack": f"rack{idx % 2}"} for idx, dc in enumerate(["dc1", "dc1", "dc2", "dc2"])]
    servers = await manager.servers_add(len(locations), property_file=locations,
                                        config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256,
                                                "endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"})
    for server, location in zip(servers, locations):
        out, err, _ = await run_nodetool(manager, server, "info")
        assert not err, err
        for line in out.splitlines():
            if line.startswith("Data Center"):
                assert line.endswith(location["dc"]), f"Expected dc {location['dc']} for {server.ip_addr} but got {line.rsplit(None, 1)[-1]}"
            elif line.startswith("Rack"):
                assert line.endswith(location["rack"]), f"Expected rack {location['rack']} for {server.ip_addr} but got {line.rsplit(None, 1)[-1]}"
