#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

def change_schema_safely(session, nodes, query, timeout=30):
    """
    Run a schema-altering query (e.g. ALTER KEYSPACE)
    and watch all given nodes' log for a "Schema version changed" message
    (naively) indicating that the schema was modified on all nodes.
    This prevents races with following queries that depend on the schema change.
    Note: this function is suitable if no other schema-modifications are
    expected to happen in parallel.
    """
    marks = [(node, node.mark_log()) for node in nodes]
    session.execute(query, timeout=timeout)
    for node, mark in marks:
        node.watch_log_for("Schema version changed", timeout=timeout)
    if not session.cluster.control_connection.wait_for_schema_agreement(wait_time=timeout):
        raise TimeoutError(f"Schema agreement timed out after {timeout} seconds")


def parse_replication_options(replication_column) -> dict:
    """
    Parses the value of the "replication" column from system_schema.keyspaces, which is a flattened map of options,
     into an expanded map.
    Expands a flattened map like {"dc0:0": "r1", "dc0:1": "r2"} into {"dc0": ["r1", "r2"]}.
    See docs/dev/system_schema_keyspace.md
    """
    result = {}
    for key, value in replication_column.items():
        if ":" in key:
            sub_key, index_str = key.split(":", 1)
            if sub_key not in result:
                result[sub_key] = []
            index = int(index_str)
            while len(result[sub_key]) <= index:
                result[sub_key].append(None)
            if index >= 0:
                result[sub_key][index] = value
        else:
            result[key] = value
    return result


def describe_rf(rf):
    """
    Formats replication factor into a string which can be passed to ALTER or CREATE KEYSPACE statement
    as an option value in the REPLICATION clause.
    The rf can be either a numeric (string or int) or a list of strings (rack list).
    """
    if type(rf) is list:
        return "[" + ", ".join(f"'{rack}'" for rack in rf) + "]"
    else:
        return f"'{rf}'"


def decrease_rf(rf):
    """
    Takes rf and returns one that is one replica smaller.
    The rf can be either a numeric (string or int) or a list of strings (rack list).
    In case of a rack list, the last rack is dropped.
    """
    if type(rf) is list:
        return rf[:-1]
    else:
        num_rf = int(rf)
        if num_rf == 0:
            raise Exception("Cannot decrease RF which is already 0")
        return str(int(rf) - 1)


def get_replication_options(session, ks):
    """
    Returns replication options of a given keyspace in the form of a map.
    The value can be a string or a list of strings (e.g. rack list RF).

    Example result: {"dc1: "3", "dc2": ["rack1", "rack2"]}
    """

    row = session.execute(f"SELECT * FROM system_schema.keyspaces  WHERE keyspace_name = '{ks}'").one()
    if hasattr(row, "replication_v2") and row.replication_v2:
        return parse_replication_options(row.replication_v2)
    return parse_replication_options(row.replication)
