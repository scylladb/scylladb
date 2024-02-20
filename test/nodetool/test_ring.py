#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from typing import NamedTuple
import pytest
import subprocess
from rest_api_mock import expected_request


class Host(NamedTuple):
    dc: str
    rack: str
    endpoint: str
    status: str
    state: str
    load: float
    ownership: float


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


def format_size(v):
    units = {
        1 << 40: 'TB',
        1 << 30: 'GB',
        1 << 20: 'MB',
        1 << 10: 'KB',
    }
    for n, unit in units.items():
        if v > n:
            d = v / n
            return f'{d:.2f} {unit}'
    return f'{v} bytes'


@pytest.mark.parametrize("host_status,host_state",
                         [
                             ('live', 'joining'),
                             ('live', 'normal'),
                             ('live', 'leaving'),
                             ('live', 'moving'),
                             ('down', 'n/a')
                         ])
def test_ring(nodetool, host_status, host_state):
    keyspace = 'ks'
    host = Host('dc0', 'rack0', '127.0.0.1', host_status, host_state,
                6414780.0, 1.0)
    token_to_endpoint = {
        "-9217327499541836964": host.endpoint,
        "9066719992055809912": host.endpoint,
        "50927788561116407": host.endpoint,
    }
    endpoint_to_ownership = {
        host.endpoint: host.ownership,
    }

    all_hosts = [host]

    def hosts_in_status(status):
        return list(h.endpoint for h in all_hosts if h.status == status)

    def hosts_in_state(state):
        return list(h.endpoint for h in all_hosts if h.state == state)

    status_to_endpoints = dict(
        (status, hosts_in_status(status)) for status in ['live', 'down'])
    state_to_endpoints = dict(
        (state, hosts_in_state(state)) for state in ['joining', 'leaving', 'moving'])
    load_map = dict((h.endpoint, h.load) for h in all_hosts)

    expected_requests = [
        expected_request('GET', '/storage_service/tokens_endpoint',
                         response=map_to_json(token_to_endpoint)),
        expected_request('GET', f'/storage_service/ownership/{keyspace}',
                         response=map_to_json(endpoint_to_ownership, str)),
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
    actual_output = nodetool('ring', keyspace, expected_requests=expected_requests)

    expected_output = f'''
Datacenter: {host.dc}
==========
'''
    max_width = max(len(endpoint) for endpoint in token_to_endpoint.values())
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

        ownership_percent = host.ownership * 100
        ownership = f'{ownership_percent:.2f}%'

        expected_output += format_stat(max_width, endpoint, host.rack,
                                       status, state, load, ownership, token)

    expected_output += '\n'

    if have_vnode:
        expected_output += '''\
  Warning: "nodetool ring" is used to output all the tokens of a node.
  To view status related info of a node use "nodetool status" instead.


'''
    expected_output += '  '
    assert actual_output == expected_output


def test_ring_no_args(nodetool, scylla_only):
    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool('ring')
    assert e.value.returncode == 1
    assert 'required parameters are missing: keyspace' in e.value.stderr
