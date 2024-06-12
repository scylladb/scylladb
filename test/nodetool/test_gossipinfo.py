#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
from copy import deepcopy
from enum import Enum, auto
from textwrap import indent
from typing import NamedTuple
from test.nodetool.rest_api_mock import expected_request


# see gms/application_state.hh
# cassandra sends the index of application_state over wire, but nodetool
# prints their names, so we have to replicate the mapping here
class State(Enum):
    STATUS = 0
    LOAD = auto()
    SCHEMA = auto()
    DC = auto()
    RACK = auto()
    RELEASE_VERSION = auto()
    REMOVAL_COORDINATOR = auto()
    INTERNAL_IP = auto()
    RPC_ADDRESS = auto()
    X_11_PADDIN = auto()
    SEVERITY = auto()
    NET_VERSION = auto()
    HOST_ID = auto()
    TOKENS = auto()
    SUPPORTED_FEATURES = auto()
    CACHE_HITRATES = auto()
    SCHEMA_TABLES_VERSION = auto()
    RPC_READY = auto()
    VIEW_BACKLOG = auto()
    SHARD_COUNT = auto()
    IGNORE_MSB_BITS = auto()
    CDC_GENERATION_ID = auto()
    SNITCH_NAME = auto()


class VersionedValue(NamedTuple):
    version: int
    value: str


def endpoint_to_jsonable(e):
    application_state = e['application_state']
    states = []
    for state, versioned_value in application_state.items():
        jsonable = {'application_state': state.value,
                    'version': versioned_value.version,
                    'value': versioned_value.value}
        states.append(jsonable)
    j = deepcopy(e)
    j['application_state'] = states
    return j


def print_application_state(application_state):
    lines = ''
    for state, versioned_value in application_state.items():
        if state != State.TOKENS:
            lines += f'{state.name}:{versioned_value.value}\n'
    return lines


def normalize_endpoints(endpoints):
    # Cassandra's nodetool uses HashMap under the hood for collecting the map
    # from state to the value of that state, and print out them by iterating
    # the keys, but the order is not guaranteed to be ordered or consistent, so
    # let's extract the state values out and sort them before comparing.
    normalized = {}
    addrs = None
    state = []

    def maybe_add_endpoint():
        if addrs is not None:
            normalized[addrs] = sorted(state)

    for line in endpoints.split('\n'):
        if line.startswith('/'):
            maybe_add_endpoint()
            addrs = line
            state = []
        else:
            state.append(line)
    maybe_add_endpoint()

    return sorted(normalized)


def create_endpoint_info(i):
    return {
        'addrs': f'127.0.0.{i}',
        'is_alive': True,
        'update_time': 4096,
        'generation': 1707827511,
        'version': 400,
        'application_state': {
            State.DC: VersionedValue(17, "datacenter1"),
            State.HOST_ID: VersionedValue(2, 'c850baa6-1f8f-431d-b898-09b646288c13'),
            State.TOKENS: VersionedValue(24, 'secret'),
        }
    }


def format_endpoint_info(endpoint):
    states_fmt = print_application_state(endpoint['application_state'])
    return """\
/{addrs}
  generation:{generation}
  heartbeat:{heartbeat}
{more_info}
""".format(addrs=endpoint['addrs'],
           generation=endpoint['generation'],
           heartbeat=endpoint['version'],
           more_info=indent(states_fmt, '  '))


@pytest.mark.parametrize("num_endpoints", [1, 2])
def test_gossipinfo(nodetool, num_endpoints):
    endpoints = [create_endpoint_info(i) for i in range(num_endpoints)]
    res = nodetool('gossipinfo', expected_requests=[
        expected_request('GET', '/failure_detector/endpoints',
                         response=[endpoint_to_jsonable(endpoint) for endpoint in endpoints])
    ])
    actual_output = res.stdout
    expected_output = ''.join(format_endpoint_info(endpoint) for endpoint in endpoints)
    assert normalize_endpoints(actual_output) == normalize_endpoints(expected_output)
