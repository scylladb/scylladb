#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from typing import NamedTuple
import functools
import operator
from socket import getnameinfo
import pytest
from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import format_size, check_nodetool_fails_with


null_ownership_error = ("Non-system keyspaces don't have the same replication settings, "
                        "effective ownership information is meaningless")


class Host(NamedTuple):
    dc: str
    rack: str
    endpoint: str
    status: str
    state: str
    load: float
    ownership: float
    tokens: list[str]

    def token_to_endpoint(self):
        return {
            token: self.endpoint for token in self.tokens
        }

    def get_address(self, resolve_ip):
        if resolve_ip:
            host, _ = getnameinfo((self.endpoint, 0), 0)
            return host
        return self.endpoint


def map_to_json(mapper, mapped_type=None):
    json = []
    for key, value in mapper.items():
        if mapped_type is None:
            json.append({'key': key, 'value': value})
        else:
            json.append({'key': key, 'value': mapped_type(value)})
    return json


def format_stat(width, address, rack, status, state, load, owns, token):
    return f"{address:<{width}}  {rack:<12}{status:<7}{state:<8}{load:<16}{owns:<20}{token:<44}\n"


@pytest.mark.parametrize("keyspace_table,resolve_ip,host_status,host_state",
                         [
                             ('ks', '', 'live', 'joining'),
                             ('ks.table', '', 'live', 'normal'),
                             ('ks', '', 'live', 'leaving'),
                             ('ks.table', '', 'live', 'moving'),
                             ('ks', '', 'down', 'n/a'),
                             ('', '', 'live', 'normal'),
                             ('', '-r', 'live', 'normal'),
                             ('', '--resolve-ip', 'live', 'normal'),
                         ])
def test_ring(request, nodetool, keyspace_table, resolve_ip, host_status, host_state):
    uses_cassandra_nodetool = request.config.getoption("nodetool") == "cassandra"

    if "." in keyspace_table:
        keyspace, table = keyspace_table.split(".")
    else:
        keyspace, table = keyspace_table, None

    if uses_cassandra_nodetool and table is not None:
        pytest.skip("skipping tablets-related test with Cassandra nodetool")

    host = Host('dc0', 'rack0', '127.0.0.1', host_status, host_state,
                6414780.0, 1.0,
                ["-9217327499541836964",
                 "9066719992055809912",
                 "50927788561116407"])
    endpoint_to_ownership = {
        host.endpoint: host.ownership,
    }

    all_hosts = [host]
    token_to_endpoint = functools.reduce(operator.or_,
                                         (h.token_to_endpoint() for h in all_hosts))

    def hosts_in_status(status):
        return list(h.endpoint for h in all_hosts if h.status == status)

    def hosts_in_state(state):
        return list(h.endpoint for h in all_hosts if h.state == state)

    status_to_endpoints = dict(
        (status, hosts_in_status(status)) for status in ['live', 'down'])
    state_to_endpoints = dict(
        (state, hosts_in_state(state)) for state in ['joining', 'leaving', 'moving'])
    load_map = dict((h.endpoint, h.load) for h in all_hosts)

    expected_requests = []

    if keyspace != '' and table is None and not uses_cassandra_nodetool:
        expected_requests.append(expected_request("GET", "/storage_service/keyspaces",
                                                  params={"replication": "tablets"},
                                                  multiple=expected_request.ONE, response=[]))

    tokens_endpoint_params = {}
    if table is not None:
        tokens_endpoint_params["keyspace"] = keyspace
        tokens_endpoint_params["cf"] = table
    expected_requests += [
        expected_request('GET', '/storage_service/tokens_endpoint', params=tokens_endpoint_params,
                         response=map_to_json(token_to_endpoint))
    ]

    is_scylla = request.config.getoption("nodetool") == "scylla"
    print_all_keyspaces = keyspace == ''

    if is_scylla and print_all_keyspaces:
        # scylla nodetool does not bother getting ownership if keyspace is not
        # specified
        pass
    else:
        expected_requests.append(
            expected_request(
                    "GET",
                    "/storage_service/ownership/null",
                    response_status=500,
                    multiple=expected_request.ANY,
                    response={"message": f"std::runtime_error({null_ownership_error})", "code": 500}))
        params = {}
        if table is not None:
            params["cf"] = table
        expected_requests.append(
            expected_request('GET', f'/storage_service/ownership/{keyspace}', params=params,
                             response=map_to_json(endpoint_to_ownership, str)))
    expected_requests += [
        expected_request('GET', '/snitch/datacenter',
                         params={'host': host.endpoint},
                         multiple=expected_request.ANY,
                         response=host.dc),
        expected_request('GET', '/gossiper/endpoint/live',
                         response=status_to_endpoints['live']),
        expected_request('GET', '/gossiper/endpoint/down',
                         response=status_to_endpoints['down']),
        expected_request('GET', '/storage_service/nodes/joining',
                         response=state_to_endpoints['joining']),
        expected_request('GET', '/storage_service/nodes/leaving',
                         response=state_to_endpoints['leaving']),
        expected_request('GET', '/storage_service/nodes/moving',
                         response=state_to_endpoints['moving']),
        expected_request('GET', '/storage_service/load_map',
                         response=map_to_json(load_map)),
        expected_request('GET', '/snitch/rack',
                         params={'host': host.endpoint},
                         multiple=expected_request.ANY,
                         response=host.rack),
    ]
    args = []
    if keyspace:
        args.append(keyspace)
    if table:
        args.append(table)
    if resolve_ip:
        args.append(resolve_ip)
    res = nodetool('ring', *args, expected_requests=expected_requests)
    actual_output = res.stdout

    expected_output = f'''
Datacenter: {host.dc}
==========
'''
    max_width = max(len(h.get_address(resolve_ip)) for h in all_hosts)
    last_token = list(token_to_endpoint)[-1]
    expected_output += format_stat(max_width, 'Address', 'Rack',
                                   'Status', 'State', 'Load', 'Owns', 'Token')
    expected_output += format_stat(max_width, '', '',
                                   '', '', '', '', last_token)
    have_vnode = False
    all_endpoints = set()
    for token, endpoint in token_to_endpoint.items():
        assert host.endpoint == endpoint

        if endpoint in all_endpoints:
            have_vnode = True
        all_endpoints.add(endpoint)

        if endpoint in status_to_endpoints['live']:
            status = 'Up'
        elif endpoint in status_to_endpoints['down']:
            status = 'Down'
        else:
            status = '?'

        if endpoint in state_to_endpoints['joining']:
            state = 'Joining'
        elif endpoint in state_to_endpoints['leaving']:
            state = 'Leaving'
        elif endpoint in state_to_endpoints['moving']:
            state = 'Moving'
        else:
            state = 'Normal'

        load = format_size(host.load)

        if print_all_keyspaces:
            ownership = '?'
        else:
            # scylla nodetool always prints out the ownership percentage,
            # since it prints out the warning, so user is aware if the
            # ownership is meaningless or not
            ownership_percent = host.ownership * 100
            ownership = f'{ownership_percent:.2f}%'

        expected_output += format_stat(max_width, host.get_address(resolve_ip),
                                       host.rack, status, state, load,
                                       ownership, token)

    expected_output += '\n'

    if have_vnode:
        expected_output += '''\
  Warning: "nodetool ring" is used to output all the tokens of a node.
  To view status related info of a node use "nodetool status" instead.


'''
    warning = ''
    if print_all_keyspaces:
        warning = '''Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless\n'''
    expected_output += f'''\
  {warning}'''
    assert actual_output == expected_output


def test_ring_tablet_keyspace_no_table(nodetool, scylla_only):
    keyspace = "ks"

    check_nodetool_fails_with(
            nodetool,
            ("ring", keyspace),
            {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", params={"replication": "tablets"},
                                 multiple=expected_request.ONE, response=[keyspace])]},
            ["error processing arguments: need a table to obtain ring for tablet keyspace"])
