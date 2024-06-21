#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
from subprocess import CalledProcessError
from test.nodetool.rest_api_mock import expected_request


def get_keyspace_view_from_args(args):
    keyspace = None
    view = None

    if len(args) == 1:
        keyspace_view = args[0].replace('/', '.', 1)
        try:
            keyspace, view = keyspace_view.split('.', 1)
        except ValueError:
            pass
    elif len(args) == 2:
        keyspace, view = args
    return keyspace, view


def format_status(host, info):
    return f"{host:<9} {info:<7}\n"


def format_output(keyspace, view, view_status):
    table = format_status("Host", "Info")
    succeed = True
    for host, status in view_status.items():
        if status != 'SUCCESS':
            succeed = False
        table += format_status(host, status)
    output = ''
    if succeed:
        output += f"{keyspace}.{view} has finished building\n"
    else:
        output += f"{keyspace}.{view} has not finished building; node status is below.\n"
        output += "\n"
        output += table
    return output



def id_from_param(param):
    args, statuses, _ = param
    if args:
        args_fmt = ','.join(args)
    else:
        args_fmt = '<none>'
    if statuses:
        statuses_fmt = ','.join(statuses)
    else:
        statuses_fmt = '<none>'
    return f"args({args_fmt})-statuses({statuses_fmt})"


def format_invalid_argument_error(is_scylla):
    stdout = ''
    stderr = ''
    message = 'viewbuildstatus requires keyspace and view name arguments'
    if is_scylla:
        stderr += f'error processing arguments: {message}\n'
    else:
        progname = "nodetool"
        stdout += f"{progname}: {message}\n"
        stdout += f"See '{progname} help' or '{progname} help <command>'.\n"
    return stdout, stderr


test_params = [
    ([], [], 1),
    (["ks"], [], 1),
    (["ks.view"], ['UNKNOWN'], 1),
    (["ks.view"], ['SUCCESS'], 0),
    (["ks.view"], ['STARTED', 'UNKNOWN', 'SUCCESS'], 1),
    (["ks.view"], ['STARTED', 'STARTED', 'STARTED'], 1),
    (["ks.view"], ['SUCCESS', 'SUCCESS', 'SUCCESS'], 0),
    (["ks/view"], ['SUCCESS'], 0),
    (["ks", "view"], ['SUCCESS'], 0),
    (["ks", "view", "something-more"], [], 1),
]

@pytest.mark.parametrize("args,statuses,returncode", [pytest.param(*param, id=id_from_param(param))
                                                      for param in test_params])
def test_viewbuildstatus(request, nodetool, args, statuses, returncode):
    keyspace, view = get_keyspace_view_from_args(args)
    if keyspace is None:
        expected_requests = []
    else:
        view_status = {f"127.0.0.{i}": status for i, status in enumerate(statuses)}
        expected_requests = [
            expected_request("GET", f"/storage_service/view_build_statuses/{keyspace}/{view}",
                             response=[{"key": host, "value": status} for host, status in view_status.items()])
        ]
        expected_output = format_output(keyspace, view, view_status)

    if returncode != 0:
        is_scylla = request.config.getoption("nodetool") == "scylla"
        if all(status == 'SUCCESS' for status in statuses):
            # invalid argument
            with pytest.raises(CalledProcessError) as exc_info:
                res = nodetool("viewbuildstatus", *args, expected_requests=expected_requests)
                actual_output = res.stdout
            assert exc_info.type is CalledProcessError
            assert exc_info.value.returncode == 1
            expected_stdout, expected_stderr = format_invalid_argument_error(is_scylla)
            assert exc_info.value.stdout == expected_stdout
            assert expected_stderr in exc_info.value.stderr
        else:
            with pytest.raises(CalledProcessError) as exc_info:
                res = nodetool("viewbuildstatus", *args, expected_requests=expected_requests)
                actual_output = res.stdout
            assert exc_info.type is CalledProcessError
            assert exc_info.value.returncode == 1
            assert exc_info.value.output == expected_output
    else:
        res = nodetool("viewbuildstatus", *args, expected_requests=expected_requests)
        actual_output = res.stdout
        assert actual_output == expected_output
