#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Internal types for handling Scylla test servers.
"""

from enum import IntEnum, auto, StrEnum
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
    pid: int

    def __str__(self):
        return f"Server({self.server_id}, {self.ip_addr}, {self.rpc_address}, {self.datacenter}, {self.rack}, {self.pid})"

    def as_dict(self) -> dict[str, object]:
        return {"server_id": self.server_id, "ip_addr": self.ip_addr, "rpc_address": self.rpc_address, "datacenter": self.datacenter, "rack": self.rack, "pid": self.pid}

    def property_file(self) -> dict[str, str]:
        return {"dc": self.datacenter, "rack": self.rack}


class ServerUpState(IntEnum):
    PROCESS_STARTED = auto()
    HOST_ID_QUERIED = auto()
    CQL_ALTERNATOR_CONNECTED = auto()
    CQL_ALTERNATOR_QUERIED = auto()
    SERVING = auto()  # Scylla sent sd_notify("serving")


class SeastarIOMetricName(StrEnum):
    """Seastar reactor IO counters, scraped at the end of each test.

    They cover all file IO issued by Scylla, counted whether or not the kernel
    serves it from the page cache (tests run with --kernel-page-cache 1, so most
    of it never reaches a disk).  They are incremented on submission to the IO
    queue and carry only a shard label; the scylla_io_queue_total_* counters
    track the same submissions broken down per IO class and device, a breakdown
    that summing for a per-test total would only throw away.
    """
    READ_BYTES = 'scylla_reactor_aio_bytes_read'
    READ_OPS = 'scylla_reactor_aio_reads'
    WRITE_BYTES = 'scylla_reactor_aio_bytes_write'
    WRITE_OPS = 'scylla_reactor_aio_writes'
