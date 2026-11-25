import pytest
import asyncio
import subprocess
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_first_completed

pytestmark = pytest.mark.prepare_3_nodes_cluster


async def run_nodetool(manager: ScyllaClusterManager, server: ServerInfo, *args: str,
                       expected_exit_code: int = 0) -> subprocess.CompletedProcess:
    """
    Runs `nodetool <args>` against `server`, with the server's executable,
    and checks that it exits with `expected_exit_code`.
    """
    exe_path = await manager.server_get_exe(server.server_id)
    cmd = [exe_path, "nodetool", *args, "--logger-log-level", "scylla-nodetool=trace", "-h", server.ip_addr]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    assert proc.returncode is not None
    result = subprocess.CompletedProcess(cmd, proc.returncode, stdout.decode(), stderr.decode())
    assert result.returncode == expected_exit_code, \
        f"{cmd} exited with {result.returncode}, expected {expected_exit_code}\n" \
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    return result


async def validate_status_operation(result: str, live_eps: list, down_eps: list, leaving: list, joining: list,
                                    zero_token_eps: list, host_id_map: dict, num_tokens: object):
    """Validates the nodetool status output"""

    lines = result.split('\n')
    i = 0
    dc_line = lines[i]
    assert dc_line.startswith("Datacenter:")
    result_dc = dc_line.split()[1]
    assert result_dc == "datacenter1"
    dc_line_len = len(dc_line)

    i += 1
    assert lines[i] == "=" * dc_line_len

    i += 1
    assert lines[i].startswith("Status=Up/Down")

    i += 1
    assert lines[i] == "|/ State=Normal/Leaving/Joining/Moving"

    i += 1
    assert lines[i].split() == ["--", "Address", "Load", "Tokens", "Owns", "Host", "ID", "Rack"]

    visited_eps = set()
    for j in range(len(live_eps + down_eps)):
        i += 1
        assert lines[i] != ""
        pieces = tuple(lines[i].split())
        if len(pieces) == 8:
            status_state, ep, load, load_unit, tokens, owns, host_id, rack = pieces
        else:
            status_state, ep, load, tokens, owns, host_id, rack = pieces

        assert ep not in visited_eps
        visited_eps.add(ep)

        assert ep in (live_eps + down_eps)

        if ep in live_eps:
            assert status_state[0] == 'U'
        else:
            assert status_state[0] in ['D', 'X']

        if ep in joining:
            assert status_state[1] == 'J'
        elif ep in leaving:
            assert status_state[1] == 'L'
        else:
            assert status_state[1] == 'N'

        if ep in zero_token_eps:
            assert int(tokens) == 0
        else:
            assert int(tokens) == num_tokens

        assert host_id == host_id_map[ep]
        assert rack == "rack1"

    i += 1
    assert lines[i] == ""


async def test_zero_token_node_normal(manager: ScyllaClusterManager):
    zero_token_nodes = await manager.servers_add(servers_num=2, config={'join_ring': False})

    servers = await manager.running_servers()

    result = await run_nodetool(manager, servers[0], "status")

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == []
    assert leaving == []

    zero_token_eps = [z.ip_addr for z in zero_token_nodes]

    host_id_map = {}
    for srv in servers:
        host_id_map[srv.ip_addr] = await manager.get_host_id(srv.server_id)
    for srv in zero_token_nodes:
        host_id_map[srv.ip_addr] = await manager.get_host_id(srv.server_id)

    config = await manager.server_get_config(servers[0].server_id)
    await validate_status_operation(result.stdout, live_eps, down_eps, leaving, [], zero_token_eps, host_id_map, config['num_tokens'])

    zero_token_result = await run_nodetool(manager, zero_token_nodes[0], "status")
    await validate_status_operation(zero_token_result.stdout, live_eps, down_eps, leaving, [], zero_token_eps,
                                    host_id_map, config['num_tokens'])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_zero_token_node_down_leaving(manager: ScyllaClusterManager):
    servers = await manager.running_servers()
    [await manager.api.enable_injection(s.ip_addr, 'delay_node_removal', one_shot=True) for s in servers]

    zero_token_node = await manager.server_add(config={'join_ring': False})
    await manager.server_stop_gracefully(zero_token_node.server_id)
    task = asyncio.create_task(manager.remove_node(servers[0].server_id, zero_token_node.server_id))

    await wait_for_first_completed([
        manager.api.wait_for_injection_enter(srv.ip_addr, "delay_node_removal") for srv in servers
    ])

    result = await run_nodetool(manager, servers[0], "status")

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == [zero_token_node.ip_addr]
    assert leaving == [zero_token_node.ip_addr]

    host_id_map = {}
    for srv in servers:
        host_id_map[srv.ip_addr] = await manager.get_host_id(srv.server_id)
    host_id_map[zero_token_node.ip_addr] = await manager.get_host_id(zero_token_node.server_id)

    config = await manager.server_get_config(servers[0].server_id)
    await validate_status_operation(result.stdout, live_eps, down_eps, leaving, [], [zero_token_node.ip_addr],
                                    host_id_map, config['num_tokens'])
    [await manager.api.message_injection(s.ip_addr, 'delay_node_removal') for s in servers]

    await task


async def test_zero_token_node_down_normal(manager: ScyllaClusterManager):
    servers = await manager.running_servers()

    zero_token_node = await manager.server_add(config={'join_ring': False})
    await manager.server_stop_gracefully(zero_token_node.server_id)

    result = await run_nodetool(manager, servers[0], "status")

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(server_eps) == len(live_eps)
    for ep in live_eps:
        assert ep in server_eps
    assert down_eps == [zero_token_node.ip_addr]
    assert leaving == []

    host_id_map = {}
    for srv in servers:
        host_id_map[srv.ip_addr] = await manager.get_host_id(srv.server_id)
    host_id_map[zero_token_node.ip_addr] = await manager.get_host_id(zero_token_node.server_id)

    config = await manager.server_get_config(servers[0].server_id)
    await validate_status_operation(result.stdout, live_eps, down_eps, leaving, [], [zero_token_node.ip_addr],
                                    host_id_map, config['num_tokens'])


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_regular_node_joining(manager: ScyllaClusterManager):
    servers = await manager.running_servers()
    [await manager.api.enable_injection(s.ip_addr, 'delay_node_bootstrap', one_shot=True) for s in servers]

    joining_server = await manager.server_add(start=False)
    task = asyncio.create_task(manager.server_start(joining_server.server_id))

    await wait_for_first_completed([
        manager.api.wait_for_injection_enter(srv.ip_addr, "delay_node_bootstrap") for srv in servers
    ])
    result = await run_nodetool(manager, servers[0], "status")

    live_eps = await manager.api.client.get_json("/gossiper/endpoint/live", host=servers[0].ip_addr)
    down_eps = await manager.api.client.get_json("/gossiper/endpoint/down", host=servers[0].ip_addr)
    leaving = await manager.api.client.get_json("/storage_service/nodes/leaving", host=servers[0].ip_addr)
    joining = await manager.api.client.get_json("/storage_service/nodes/joining", host=servers[0].ip_addr)

    server_eps = [s.ip_addr for s in servers]

    assert len(live_eps) == 4
    assert len(server_eps) == 3
    assert len(joining) == 1
    for ep in live_eps:
        assert ep in (server_eps + joining)
    assert down_eps == []
    assert leaving == []

    host_id_map = {}
    for srv in servers:
        host_id_map[srv.ip_addr] = await manager.get_host_id(srv.server_id)
    host_id_map[joining_server.ip_addr] = await manager.get_host_id(joining_server.server_id)

    config = await manager.server_get_config(servers[0].server_id)
    await validate_status_operation(result.stdout, live_eps, down_eps, leaving, joining, [], host_id_map,
                                    config['num_tokens'])
    [await manager.api.message_injection(s.ip_addr, 'delay_node_bootstrap') for s in servers]

    await task
