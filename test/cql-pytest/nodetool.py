# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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


# Run the external "nodetool" executable (can be overridden by the NODETOOL
# environment variable). Only call this if the REST API doesn't work.
nodetool_cmd = os.getenv('NODETOOL') or 'nodetool'
def run_nodetool(cql, *args):
    # TODO: We may need to change this function or its callers to add proper
    # support for testing on multi-node clusters.
    host = cql.cluster.contact_points[0]
    subprocess.run([nodetool_cmd, '-h', host, *args])

def flush(cql, table):
    ks, cf = table.split('.')
    if has_rest_api(cql):
        requests.post(f'{rest_api_url(cql)}/storage_service/keyspace_flush/{ks}', params={'cf' : cf})
    else:
        run_nodetool(cql, "flush", ks, cf)
