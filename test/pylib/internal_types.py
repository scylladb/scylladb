#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Internal types for handling Scylla test servers.
"""

from typing import NewType, NamedTuple


ServerNum = NewType('ServerNum', int)
IPAddress = NewType('IPAddress', str)
HostID = NewType('HostID', str)


class ServerInfo(NamedTuple):
    """Server id (test local) and IP address"""
    server_id: ServerNum
    ip_addr: IPAddress
    rpc_address: IPAddress

    def __str__(self):
        return f"Server({self.server_id}, {self.ip_addr}, {self.rpc_address})"
