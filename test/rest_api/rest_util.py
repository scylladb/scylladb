import time

from contextlib import contextmanager

# A utility function for creating a new temporary snapshot.
# If no keyspaces are given, a snapshot is taken over all keyspaces and tables.
# If no tables are given, a snapshot is taken over all tables in the keyspace.
# If no tag is given, a unique tag will be computed using the current time, in milliseconds.
# It can be used in a "with", as:
#   with new_test_snapshot(cql, tag, keyspace, [table(s)]) as snapshot:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_snapshot(rest_api, keyspaces=[], tables=[], tag=""):
    if not tag:
        tag = f"test_snapshot_{int(time.time() * 1000)}"
    params = { "tag": tag }
    if type(keyspaces) is str:
        params["kn"] = keyspaces
    else:
        params["kn"] = ",".join(keyspaces)
    if tables:
        if type(tables) is str:
            params["cf"] = tables
        else:
            params["cf"] = ",".join(tables)
    resp = rest_api.send("POST", "storage_service/snapshots", params)
    resp.raise_for_status()
    try:
        yield tag
    finally:
        resp = rest_api.send("DELETE", "storage_service/snapshots", params)
        resp.raise_for_status()
