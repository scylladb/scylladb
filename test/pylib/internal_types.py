#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
"""Internal types for handling Scylla test servers.
"""

from enum import Enum, auto
from typing import NewType, NamedTuple


ServerNum = NewType('ServerNum', int)
IPAddress = NewType('IPAddress', str)
HostID = NewType('HostID', str)


class ServerInfo(NamedTuple):
    """Server id (test local) and IP address"""
    server_id: ServerNum
    ip_addr: IPAddress
    rpc_address: IPAddress
    datacenter: str
    rack: str

    def __str__(self):
        return f"Server({self.server_id}, {self.ip_addr}, {self.rpc_address}, {self.datacenter}, {self.rack})"
    
    def as_dict(self) -> dict[str, object]:
        return {"server_id": self.server_id, "ip_addr": self.ip_addr, "rpc_address": self.rpc_address, "datacenter": self.datacenter, "rack": self.rack}


class ServerUpState(Enum):
    PROCESS_STARTED = auto()
    HOST_ID_QUERIED = auto()
    CQL_CONNECTED = auto()
    CQL_QUERIED = auto()
