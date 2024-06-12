# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#


from collections import defaultdict
from enum import Enum
from test.nodetool.rest_api_mock import expected_request
from socket import getnameinfo
from typing import NamedTuple

import pytest


class NodeStatus(Enum):
    Up = 'U'
    Down = 'D'
    Unknown = '?'


class NodeState(Enum):
    Joining = 'J'
    Leaving = 'L'
    Moving = 'M'
    Normal = 'N'


class Node(NamedTuple):
    endpoint: str
    host_id: str
    load: int
    tokens: list[str]
    datacenter: str
    rack: str
    status: NodeStatus
    state: NodeState

class StatusQueryTarget(NamedTuple):
    keyspace: str
    table: str
    uses_tablets: bool

null_ownership_error = ("Non-system keyspaces don't have the same replication settings, "
                        "effective ownership information is meaningless")


def validate_status_output(res, keyspace, nodes, ownership, resolve, effective_ownership_unknown, token_count_unknown,
                           cassandra_nodetool):
    datacenters = sorted(list(set([node.datacenter for node in nodes.values()])))
    load_multiplier = {"bytes": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    lines = res.split('\n')
    i = 0

    for dc in datacenters:
        dc_line = lines[i]
        assert dc_line.startswith("Datacenter:")
        res_dc = dc_line.split()[1]
        assert dc == res_dc
        dc_line_len = len(dc_line)

        i += 1
        assert lines[i] == "=" * dc_line_len

        i += 1
        assert lines[i] == "Status=Up/Down"

        i += 1
        assert lines[i] == "|/ State=Normal/Leaving/Joining/Moving"

        i += 1
        if keyspace is None:
            assert lines[i].split() == ["--", "Address", "Load", "Tokens", "Owns", "Host", "ID", "Rack"]
        else:
            assert lines[i].split() == ["--", "Address", "Load", "Tokens", "Owns", "(effective)", "Host", "ID", "Rack"]

        dc_eps = {ep for ep, node in nodes.items() if node.datacenter == dc}
        if resolve:
            name_to_ep = {getnameinfo((ep, 0), 0)[0]: ep for ep in nodes.keys()}

        # The legacy nodetool prints endpoints in random order (probably in hash-map order).
        # So just make sure here, that we see all endpoints from each DC and that their properties are correct.
        while dc_eps:
            i += 1

            assert lines[i] != ""

            pieces = tuple(lines[i].split())
            if len(pieces) == 8:
                status_state, ep, load, load_unit, tokens, owns, host_id, rack = pieces
            else:
                status_state, ep, load, tokens, owns, host_id, rack = pieces

            if resolve:
                assert ep in name_to_ep
                ep = name_to_ep[ep]

            assert ep in dc_eps
            dc_eps.remove(ep)

            node = nodes[ep]

            assert status_state == "{}{}".format(nodes[ep].status.value, nodes[ep].state.value)
            if node.load is None:
                assert load == "?"
            else:
                assert load_unit is not None
                assert load == "{:.2f}".format(int(node.load) / load_multiplier[load_unit])
            if token_count_unknown:
                tokens == "?"
            else:
                assert int(tokens) == len(node.tokens)
            if effective_ownership_unknown:
                assert owns == "?"
            else:
                assert owns == "{:.1f}%".format(float(ownership[ep]) * 100)
            if node.host_id is not None:
                assert host_id == node.host_id
            else:
                if cassandra_nodetool:
                    assert host_id == "null"
                else:
                    assert host_id == "?"
            assert rack == node.rack

        assert len(dc_eps) == 0
        i += 1

    i += 1
    if keyspace is None:
        assert lines[i] == f"Note: {null_ownership_error}"
    else:
        assert lines[i] == ""


def ratio_helper(a: int, b: int):
    maxint_64 = (2**63)-1
    minint_64 = -2**63
    int64_range = 2**64
    if a > b:
        val = a - b
    else:
        val = -(a - minint_64) + (maxint_64 - b)
    return val / int64_range


# Mirrors dht::token::describe_ownership()
def _describe_token_ownership(sorted_tokens: list[int]):
    if len(sorted_tokens) == 0:
        return {str(sorted_tokens[0]): 1.0}

    ownerships = {}

    start = sorted_tokens[0]
    ti = start  # The first token and its value
    tim1 = ti  # The last token and its value (after loop)

    for i in range(1, len(sorted_tokens)):
        ti = sorted_tokens[i]  # The next token and its value
        ownerships[str(ti)] = ratio_helper(ti, tim1)  # save (T(i) -> %age)
        tim1 = ti

    # The start token's range extends backward to the last token, which is why both were saved above.
    ownerships[str(start)] = ratio_helper(start, ti)

    return ownerships


# Mirrors service::storage_service::get_ownership()
def _get_ownership(nodes):
    sorted_tokens = sorted([int(token) for node in nodes for token in node.tokens])
    ownership_by_token = _describe_token_ownership(sorted_tokens)
    token_to_endpoint = {token: node.endpoint for node in nodes for token in node.tokens}

    ownership = defaultdict(int)
    for token, own in ownership_by_token.items():
        ep = token_to_endpoint[token]
        ownership[ep] += own

    return ownership


def _do_test_status(request, nodetool, status_query_target, node_list, resolve=None):
    uses_cassandra_nodetool = request.config.getoption("nodetool") == "cassandra"

    if status_query_target:
        keyspace = status_query_target.keyspace
        table = status_query_target.table
        keyspace_uses_tablets = status_query_target.uses_tablets
    else:
        keyspace = None
        table = None
        keyspace_uses_tablets = False

    nodes = {node.endpoint: node for node in node_list}

    joining = [n.endpoint for n in node_list if n.state == NodeState.Joining]
    leaving = [n.endpoint for n in node_list if n.state == NodeState.Leaving]
    moving = [n.endpoint for n in node_list if n.state == NodeState.Moving]
    live = [n.endpoint for n in node_list if n.status == NodeStatus.Up]
    down = [n.endpoint for n in node_list if n.status == NodeStatus.Down]

    load_map = [{"key": ep, "value": node.load} for ep, node in nodes.items() if node.load is not None]

    host_id_map = [{"key": ep, "value": node.host_id} for ep, node in nodes.items() if node.host_id is not None]

    tokens_endpoint_params = {}
    if keyspace_uses_tablets and table:
        tokens_endpoint_params["keyspace"] = keyspace
        tokens_endpoint_params["cf"] = table

    tokens_endpoint = []
    for ep, node in nodes.items():
        for token in node.tokens:
            tokens_endpoint.append({"key": token, "value": ep})
    tokens_endpoint.sort(key=lambda x: int(x['key']))

    ownership = _get_ownership(node_list)
    ownership_response = [{"key": ep, "value": str(own)} for ep, own in ownership.items()]

    expected_requests = [
        expected_request("GET", "/storage_service/nodes/joining", response=joining),
        expected_request("GET", "/storage_service/nodes/leaving", response=leaving),
        expected_request("GET", "/storage_service/nodes/moving", response=moving),
        expected_request("GET", "/storage_service/load_map", response=load_map),
        expected_request("GET", "/storage_service/tokens_endpoint", params=tokens_endpoint_params,
                         response=tokens_endpoint),
        expected_request("GET", "/gossiper/endpoint/live", response=live),
        expected_request("GET", "/gossiper/endpoint/down", response=down),
        expected_request("GET", "/storage_service/host_id", response=host_id_map),
    ]

    if keyspace is None:
        expected_requests += [
                expected_request("GET",
                                 "/storage_service/ownership/null",
                                 response_status=500,
                                 multiple=expected_request.ANY,
                                 response={"message": f"std::runtime_error({null_ownership_error})", "code": 500}),
                expected_request("GET", "/storage_service/ownership", multiple=expected_request.ANY,
                                 response=ownership_response)]
    else:
        if not uses_cassandra_nodetool:
            keyspaces_using_tablets = [keyspace] if keyspace_uses_tablets else []
            expected_requests.append(
                expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"},
                                 multiple=expected_request.ONE, response=keyspaces_using_tablets))
        if table is None:
            if not keyspace_uses_tablets:
                expected_requests.append(
                    expected_request("GET", f"/storage_service/ownership/{keyspace}",
                                     multiple=expected_request.ONE, response=ownership_response))
        else:
            expected_requests.append(
                    expected_request("GET", f"/storage_service/ownership/{keyspace}", params={"cf": table},
                                     response=ownership_response))

    for ep, node in nodes.items():
        expected_requests += [
            expected_request("GET", "/snitch/datacenter", params={"host": ep}, multiple=expected_request.ANY,
                             response=node.datacenter),
            expected_request("GET", "/snitch/rack", params={"host": ep}, multiple=expected_request.ANY,
                             response=node.rack),
        ]

    args = ["status"]

    if keyspace is not None:
        args.append(keyspace)

    if table is not None:
        args.append(table)

    if resolve is not None:
        args.append(resolve)

    res = nodetool(*args, expected_requests=expected_requests)

    effective_ownership_unknown = keyspace is None or (table is None and keyspace_uses_tablets)
    token_count_unknown = keyspace_uses_tablets and not table
    validate_status_output(res.stdout, keyspace, nodes, ownership, bool(resolve), effective_ownership_unknown,
                           token_count_unknown, uses_cassandra_nodetool)


def test_status_no_keyspace_single_dc(request, nodetool):
    nodes = [
        Node(
            endpoint="127.0.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.0.0.2",
            host_id="ed341f60-b12a-4fd4-9917-e80977ded0f9",
            load=277624,
            tokens=["-1810801828328238220", "2983536194780899528"],
            datacenter="datacenter1",
            rack="rack2",
            status=NodeStatus.Down,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.0.0.3",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["3810801828328238220", "6810801828328238220"],
            datacenter="datacenter1",
            rack="rack3",
            status=NodeStatus.Up,
            state=NodeState.Leaving,
        ),
    ]

    _do_test_status(request, nodetool, None, nodes)


@pytest.mark.parametrize("uses_tablets", (False, True))
@pytest.mark.parametrize("table", (None, "cf"))
def test_status_keyspace_single_dc(request, nodetool, uses_tablets, table):
    if request.config.getoption("nodetool") == "cassandra" and (uses_tablets or table):
        pytest.skip("skipping tablets-related test with Cassandra nodetool")

    nodes = [
        Node(
            endpoint="127.0.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Unknown,
            state=NodeState.Joining,
        ),
        Node(
            endpoint="127.0.0.2",
            host_id="ed341f60-b12a-4fd4-9917-e80977ded0f9",
            load=277624,
            tokens=["-1810801828328238220", "2983536194780899528"],
            datacenter="datacenter1",
            rack="rack2",
            status=NodeStatus.Down,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.0.0.3",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["3810801828328238220", "6810801828328238220"],
            datacenter="datacenter1",
            rack="rack3",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
    ]

    status_target = StatusQueryTarget(keyspace="ks", table=table, uses_tablets=uses_tablets)
    _do_test_status(request, nodetool, status_target, nodes)


def test_status_no_keyspace_multi_dc(request, nodetool):
    nodes = [
        Node(
            endpoint="127.1.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.1.0.2",
            host_id="ed341f60-b12a-4fd4-9917-e80977ded0f9",
            load=277624,
            tokens=["1810801828328238220", "2810801828328238220"],
            datacenter="datacenter1",
            rack="rack2",
            status=NodeStatus.Down,
            state=NodeState.Moving,
        ),
        Node(
            endpoint="127.2.0.1",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["3810801828328238220", "6810801828328238220"],
            datacenter="datacenter2",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.2.0.2",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["8810801828328238220", "9810801828328238220"],
            datacenter="datacenter2",
            rack="rack2",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
    ]

    _do_test_status(request, nodetool, None, nodes)

@pytest.mark.parametrize("uses_tablets", (False, True))
@pytest.mark.parametrize("table", (None, "cf"))
def test_status_keyspace_multi_dc(request, nodetool, uses_tablets, table):
    if request.config.getoption("nodetool") == "cassandra" and (uses_tablets or table):
        pytest.skip("skipping tablets-related test with Cassandra nodetool")

    nodes = [
        Node(
            endpoint="127.1.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Down,
            state=NodeState.Joining,
        ),
        Node(
            endpoint="127.1.0.2",
            host_id="ed341f60-b12a-4fd4-9917-e80977ded0f9",
            load=277624,
            tokens=["1810801828328238220", "2810801828328238220"],
            datacenter="datacenter1",
            rack="rack2",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.2.0.1",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["3810801828328238220", "6810801828328238220"],
            datacenter="datacenter2",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.2.0.2",
            host_id="1e77eb26-a372-4eb4-aeaa-72f224cf6b4c",
            load=353236,
            tokens=["8810801828328238220", "9810801828328238220"],
            datacenter="datacenter2",
            rack="rack2",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
    ]

    status_target = StatusQueryTarget(keyspace="ks", table=table, uses_tablets=uses_tablets)
    _do_test_status(request, nodetool, status_target, nodes)


def test_status_keyspace_joining_node(request, nodetool):
    """ Joining nodes do not have some attributes available yet:
    * load
    * host_id
    """
    nodes = [
        Node(
            endpoint="127.0.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
        Node(
            endpoint="127.0.0.2",
            host_id=None,
            load=None,
            tokens=["-1810801828328238220", "2983536194780899528"],
            datacenter="datacenter1",
            rack="rack2",
            status=NodeStatus.Up,
            state=NodeState.Joining,
        ),
    ]

    status_target = StatusQueryTarget(keyspace="ks", table=None, uses_tablets=False)
    _do_test_status(request, nodetool, status_target, nodes)


@pytest.mark.parametrize("resolve", (None, '-r', '--resolve-ip'))
def test_status_resolve(request, nodetool, resolve):
    nodes = [
        Node(
            endpoint="127.0.0.1",
            host_id="78a9c1d0-b341-467e-a076-9eff4cf7ffc6",
            load=206015,
            tokens=["-9175818098208185248", "-3983536194780899528"],
            datacenter="datacenter1",
            rack="rack1",
            status=NodeStatus.Up,
            state=NodeState.Normal,
        ),
    ]

    _do_test_status(request, nodetool, None, nodes, resolve)
