# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later


# This file provides utility to REST API requests.
# Some metrics cannot be obtained by nodetool, but they are available by API.

import requests
import nodetool
import pytest

# Sends GET request to REST API. Response is restured as JSON.
# If API isn't available, `pytest.skip()` is called.
def get_request(cql, *path):
    if nodetool.has_rest_api(cql):
        response = requests.get(f"{nodetool.rest_api_url(cql)}/{'/'.join(path)}")
        return response.json()
    else:
        pytest.skip("REST API not available")

# Get column family's metric.
# metric - name of matric
# table - (optional) column family name to add to request's path
#         Expected format: `keyspace.name`
def get_column_family_metric(cql, metric, table=None):
    args = ["column_family", "metrics", metric]
    if table != None:
        ks, cf = table.split('.')
        args.append(f"{ks}:{cf}")
        
    return get_request(cql, *args)