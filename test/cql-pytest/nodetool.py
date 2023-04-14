# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
##################################################################

# This file provides a few nodetool-compatible commands that may be useful
# for tests. The intent is *not* to test nodetool itself, but just to
# use nodetool functionality - e.g., "nodetool flush" when testing CQL.
#
# When testing against a locally-running Scylla these functions do not
# actually use nodetool but rather Scylla's REST API. This simplifies
# running the test because an external Java process implementing JMX is
# not needed. However, when the REST API port is not available (i.e, when
# testing against Cassandra or some remote installation of Scylla) the
# external "nodetool" command is used, and must be available in the path or
# chosen with the NODETOOL environment variable.

import requests
import os
import subprocess
import shutil
import pytest

# For a "cql" object connected to one node, find the REST API URL
# with the same node and port 10000.
# TODO: We may need to change this function or its callers to add proper
# support for testing on multi-node clusters.
def rest_api_url(cql):
    return f'http://{cql.cluster.contact_points[0]}:10000'

# Check whether the REST API at port 10000 is available - if not we will
# fall back to using an external "nodetool" program.
# We only check this once per REST API URL, and cache the decision.
checked_rest_api = {}
def has_rest_api(cql):
    url = rest_api_url(cql)
    if not url in checked_rest_api:
        # Scylla's REST API does not have an official "ping" command,
        # so we just list the keyspaces as a (usually) short operation
        try:
            ok = requests.get(f'{url}/column_family/name/keyspace').ok
        except:
            ok = False
        checked_rest_api[url] = ok
    return checked_rest_api[url]


# Find the external "nodetool" executable (can be overridden by the NODETOOL
# environment variable). Only call this if the REST API doesn't work.
def nodetool_cmd():
    if nodetool_cmd.cmd:
        return nodetool_cmd.cmd
    if not nodetool_cmd.failed:
        nodetool_cmd.conf = os.getenv('NODETOOL') or 'nodetool'
        nodetool_cmd.cmd = shutil.which(nodetool_cmd.conf)
        if nodetool_cmd.cmd is None:
            nodetool_cmd.failed = True
    if nodetool_cmd.failed:
        pytest.fail(f"Error: Can't find {nodetool_cmd.conf}. Please set the NODETOOL environment variable to the path of the nodetool utility.", pytrace=False)
    return nodetool_cmd.cmd
nodetool_cmd.cmd = None
nodetool_cmd.failed = False
nodetool_cmd.conf = False

# Run the external "nodetool" executable (can be overridden by the NODETOOL
# environment variable). Only call this if the REST API doesn't work.
def run_nodetool(cql, *args):
    # TODO: We may need to change this function or its callers to add proper
    # support for testing on multi-node clusters.
    host = cql.cluster.contact_points[0]
    subprocess.run([nodetool_cmd(), '-h', host, *args])

def flush(cql, table):
    ks, cf = table.split('.')
    if has_rest_api(cql):
        requests.post(f'{rest_api_url(cql)}/storage_service/keyspace_flush/{ks}', params={'cf' : cf})
    else:
        run_nodetool(cql, "flush", ks, cf)

def flush_keyspace(cql, ks):
    if has_rest_api(cql):
        requests.post(f'{rest_api_url(cql)}/storage_service/keyspace_flush/{ks}')
    else:
        run_nodetool(cql, "flush", ks)

def compact(cql, table):
    ks, cf = table.split('.')
    if has_rest_api(cql):
        requests.post(f'{rest_api_url(cql)}/storage_service/keyspace_compaction/{ks}', params={'cf' : cf})
    else:
        run_nodetool(cql, "compact", ks, cf)

def take_snapshot(cql, table, tag, skip_flush):
    ks, cf = table.split('.')
    if has_rest_api(cql):
        requests.post(f'{rest_api_url(cql)}/storage_service/snapshots/', params={'kn': ks, 'cf' : cf, 'tag': tag, 'sf': skip_flush})
    else:
        args = ['--tag', tag, '--table', cf]
        if skip_flush:
            args.append('--skip-flush')
        args.append(ks)
        run_nodetool(cql, "snapshot", *args)

def refreshsizeestimates(cql):
    if has_rest_api(cql):
        # The "nodetool refreshsizeestimates" is not available, or needed, in Scylla
        pass
    else:
        run_nodetool(cql, "refreshsizeestimates")

class no_autocompaction_context:
    """Disable autocompaction for the enclosed scope, for the provided keyspace(s).
    """
    def __init__(self, cql, *keyspaces):
        self._cql = cql
        self._keyspaces = list(keyspaces)

    def __enter__(self):
        for ks in self._keyspaces:
            if has_rest_api(self._cql):
                requests.delete(f'{rest_api_url(self._cql)}/column_family/autocompaction/{ks}')
            else:
                run_nodetool(self._cql, "disableautocompaction", ks)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for ks in self._keyspaces:
            if has_rest_api(self._cql):
                requests.post(f'{rest_api_url(self._cql)}/column_family/autocompaction/{ks}')
            else:
                run_nodetool(self._cql, "enableautocompaction", ks)
