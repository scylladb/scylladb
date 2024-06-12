#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from typing import NamedTuple
import pytest
from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with_error_contains


class Record(NamedTuple):
    partition: str
    count: int
    error: int

    def to_json(self):
        return {
            'partition': self.partition,
            'count': self.count,
            'error': self.error
        }

    @classmethod
    def header(cls):
        partition = 'Partition'
        count = 'Count'
        error = '+/-'
        return f"\t{partition:<17}{count:>10}{error:>10}\n"

    def format(self):
        return f"\t{self.partition:<17}{self.count:>10}{self.error:>10}\n"


def normalize_samplings(samplings):
    # Cassandra's nodetool uses Map under the hood for collecting the samplings,
    # and prints out the items in the map, but the order is not guaranteed to be
    # ordered or consistent, so let's extract the state values out and sort them
    # before comparing.
    normalized = {}
    header = None
    report = []

    def maybe_add_report():
        if header is not None:
            normalized[header] = report

    for line in samplings.split('\n'):
        if not line.strip():
            continue
        if line[0] not in ('\t', ' '):
            maybe_add_report()
            header = line
            report = []
        else:
            report.append(line)
    maybe_add_report()

    return sorted(normalized.items())


@pytest.mark.parametrize("empty_samplings,samplers", [(True, "READS"),
                                                      (True, "READS,WRITES"),
                                                      (True, ""),
                                                      (False, "WRITES"),
                                                      (False, "")])
def test_toppartitions(nodetool, request, empty_samplings, samplers):

    if empty_samplings:
        samplings = {
            'read_cardinality': 0,
            'write_cardinality': 0,
        }
        response = samplings
    else:
        samplings = {
            'read_cardinality': 5,
            'read': [Record('Russell Westbrook', 100, 3),
                     Record('Jermi Grant',       25,  1),
                     Record('Victor Oladipo',    17,  0),
                     Record('Andre Roberson',    1,   0),
                     Record('Steven Adams',      1,   0)],
            'write_cardinality': 5,
            'write': [Record('Russell Westbrook', 101, 3),
                      Record('Jermi Grant',       26,  1),
                      Record('Victor Oladipo',    18,  0),
                      Record('Andre Roberson',    2,   0),
                      Record('Steven Adams',      2,   0)],
        }
        response = {
            'read_cardinality': samplings['read_cardinality'],
            'read': [record.to_json() for record in samplings['read']],
            'write_cardinality': samplings['write_cardinality'],
            'write': [record.to_json() for record in samplings['write']],
        }
    duration = 50000
    capacity = 512
    list_size = 20
    table_filters = 'ks:cf'
    options = {
        "--duration": duration,
        "-s": capacity,
        "-k": list_size,
        "--cf-filters": table_filters,
    }
    if samplers:
        options["-a"] = samplers
    else:
        samplers = "WRITES,READS"
    args = []
    for name, value in options.items():
        args.extend([name, str(value)])
    params = {
        'duration': str(duration),
        'table_filters': 'ks:cf',
        'capacity': str(capacity),
    }
    if request.config.getoption("nodetool") == "scylla":
        # scylla sends list_size, while cassandra's nodetool does not.
        params['list_size'] = str(list_size)
    res = nodetool("toppartitions", *args, expected_requests=[
        expected_request("GET", "/storage_service/toppartitions/",
                         params=params,
                         response=response),
    ])
    actual_output = res.stdout

    expected_output = ''
    first = True
    for sampler in samplers.lower().split(','):
        # remove the trailing "s"
        operation = sampler[:-1]
        operation_upper = operation.upper()
        cardinality = samplings[f'{operation}_cardinality']
        expected_output += f'''\
{operation_upper}S Sampler:
  Cardinality: ~{cardinality} ({capacity} capacity)
  Top {list_size} partitions:
'''
        if operation not in samplings:
            expected_output += "\tNothing recorded during sampling period...\n"
            continue

        expected_output += Record.header()
        for record in samplings[operation]:
            expected_output += record.format()
        if first:
            expected_output += '\n'
            first = False

    assert normalize_samplings(actual_output) == normalize_samplings(expected_output)


def test_toppartitions_invalid_capacity(nodetool, request):
    options = {
        "--duration": 50000,
        "-s": 20,
        "-k": 512,
        "--cf-filters": 'ks:cf',
    }
    args = ["toppartitions"]
    for name, value in options.items():
        args.extend([name, str(value)])
    error = "TopK count (-k) option must be smaller than the summary capacity (-s)"
    check_nodetool_fails_with_error_contains(
        nodetool,
        tuple(args),
        {},
        [error])


def test_toppartitions_invalid_sampler(nodetool):
    invalid_samplers = "ecrires"
    options = {
        "--duration": 50000,
        "--cf-filters": 'ks:cf',
        "-a": invalid_samplers,
    }
    args = ["toppartitions"]
    for name, value in options.items():
        args.extend([name, str(value)])
    check_nodetool_fails_with_error_contains(
        nodetool,
        tuple(args),
        {},
        [f"{invalid_samplers} is not a valid sampler, choose one of: READS, WRITES"])


@pytest.mark.parametrize("positional_args",
                         [
                             ("ks0",),
                             ("ks0", "cf0"),
                             ("ks0", "50000"),
                         ])
def test_toppartitions_missing_positional_args(nodetool, positional_args):
    # toppartitions allows 3 positional args. either specify all of them,
    # or none of them, but not some of them.
    check_nodetool_fails_with_error_contains(
        nodetool,
        tuple(["toppartitions", *positional_args]),
        {},
        ["toppartitions requires either a keyspace, column family name and duration or no arguments at all"])
