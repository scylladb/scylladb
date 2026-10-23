# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Helpers for talking to the server with raw CQL frames over a socket. Tests
# use them to send messages the Python driver can't send, e.g. malformed ones.
#############################################################################

import socket
import struct

def build_frame(*, opcode: int, stream: int, body: bytes) -> bytearray:
    frame = bytearray()
    frame += struct.pack("!B", 0x04)         # version 4
    frame += struct.pack("!B", 0x00)         # flags
    frame += struct.pack("!H", stream)       # stream
    frame += struct.pack("!B", opcode)       # opcode
    frame += struct.pack("!I", len(body))    # body length
    frame += body
    return frame

def send_frame(sock: socket.socket, *, opcode: int, stream: int, body: bytes) -> None:
    sock.send(build_frame(opcode=opcode, stream=stream, body=body))

def recv_frame(sock: socket.socket) -> bytes:
    return sock.recv(4096)
