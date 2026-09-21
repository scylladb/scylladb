#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


def data_resource_creator_permissions(creator, resource, support_func=True):
    permissions = []
    for perm in "SELECT", "MODIFY", "ALTER", "DROP", "AUTHORIZE":
        permissions.append((creator, resource, perm))
    if resource.startswith("<keyspace "):
        permissions.append((creator, resource, "CREATE"))
        keyspace = resource[10:-1]
        if support_func:
            # also grant the creator of a ks perms on functions in that ks
            for perm in "CREATE", "ALTER", "DROP", "AUTHORIZE", "EXECUTE":
                permissions.append((creator, "<all functions in %s>" % keyspace, perm))
    return permissions


def role_creator_permissions(creator, role):
    permissions = []
    for perm in "ALTER", "DROP", "AUTHORIZE":
        permissions.append((creator, role, perm))
    return permissions


def function_resource_creator_permissions(creator, resource):
    permissions = []
    for perm in "ALTER", "DROP", "AUTHORIZE", "EXECUTE":
        permissions.append((creator, resource, perm))
    return permissions
