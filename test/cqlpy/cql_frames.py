# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Helpers for talking to the server with raw CQL frames over a socket. Tests
# use them to send messages the Python driver can't be made to send, e.g.
# malformed ones, or ones with contents the driver chooses by itself.
#############################################################################

import socket
import struct
import uuid
from contextlib import contextmanager

# Opcodes.
ERROR = 0x00
STARTUP = 0x01
READY = 0x02
AUTHENTICATE = 0x03
OPTIONS = 0x05
SUPPORTED = 0x06
QUERY = 0x07
RESULT = 0x08
PREPARE = 0x09
EXECUTE = 0x0A
REGISTER = 0x0B
EVENT = 0x0C
BATCH = 0x0D
AUTH_CHALLENGE = 0x0E
AUTH_RESPONSE = 0x0F
AUTH_SUCCESS = 0x10

# Flags of a response frame.
COMPRESSION_FLAG = 0x01
TRACING_FLAG = 0x02
CUSTOM_PAYLOAD_FLAG = 0x04
WARNING_FLAG = 0x08

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
    sock.sendall(build_frame(opcode=opcode, stream=stream, body=body))

def recv_frame(sock: socket.socket) -> bytes:
    return sock.recv(4096)

def encode_byte(n: int) -> bytes:
    return struct.pack("!B", n)

def encode_short(n: int) -> bytes:
    return struct.pack("!H", n)

def encode_int(n: int) -> bytes:
    return struct.pack("!i", n)

def encode_string(s: str) -> bytes:
    encoded = s.encode()
    return encode_short(len(encoded)) + encoded

def encode_long_string(s: str) -> bytes:
    encoded = s.encode()
    return encode_int(len(encoded)) + encoded

def encode_bytes(b: bytes) -> bytes:
    return encode_int(len(b)) + b

def encode_short_bytes(b: bytes) -> bytes:
    return encode_short(len(b)) + b

def encode_string_map(m: dict[str, str]) -> bytes:
    return encode_short(len(m)) + b''.join(encode_string(k) + encode_string(v) for k, v in m.items())

# Reads the notations of the protocol from the body of a frame.
class Reader:
    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def _take(self, n: int) -> bytes:
        assert self._pos + n <= len(self._data), f"truncated body: expected {n} more bytes, got {len(self._data) - self._pos}"
        taken = self._data[self._pos:self._pos + n]
        self._pos += n
        return taken

    def read_short(self) -> int:
        return struct.unpack("!H", self._take(2))[0]

    def read_int(self) -> int:
        return struct.unpack("!i", self._take(4))[0]

    def read_long(self) -> int:
        return struct.unpack("!q", self._take(8))[0]

    def read_uuid(self) -> uuid.UUID:
        return uuid.UUID(bytes=self._take(16))

    def read_string(self) -> str:
        return self._take(self.read_short()).decode()

    def read_bytes(self) -> bytes | None:
        n = self.read_int()
        return None if n < 0 else self._take(n)

    # Reads a [bytes] holding a value, and decodes the value with `read`, a
    # method of Reader. Values of types like int, bigint or uuid are laid out
    # like the notations of the same name, e.g. Reader.read_int for an int.
    def read_value(self, read):
        value = self.read_bytes()
        assert value is not None, "unexpected null value"
        return read(Reader(value))

    # Reads a [short] n, followed by n items.
    def _read_list(self, read_item):
        n = self.read_short()
        items = []
        for _ in range(n):
            items.append(read_item())
        return items

    # Reads a [short] n, followed by n entries, each a key followed by its value.
    def _read_map(self, read_key, read_value):
        n = self.read_short()
        entries = {}
        for _ in range(n):
            key = read_key()
            value = read_value()
            entries[key] = value
        return entries

    def read_string_list(self) -> list[str]:
        return self._read_list(self.read_string)

    def read_string_multimap(self) -> dict[str, list[str]]:
        return self._read_map(self.read_string, self.read_string_list)

    def read_bytes_map(self) -> dict[str, bytes | None]:
        return self._read_map(self.read_string, self.read_bytes)

# A response frame. The optional parts at the start of its body are parsed,
# and `body` reads what follows them.
class Response:
    def __init__(self, opcode: int, flags: int, body: bytes):
        self.opcode = opcode
        self.body = Reader(body)
        if flags & TRACING_FLAG:
            self.body._take(16)
        self.warnings = self.body.read_string_list() if flags & WARNING_FLAG else []
        self.custom_payload = self.body.read_bytes_map() if flags & CUSTOM_PAYLOAD_FLAG else {}

def _recv_exactly(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError(f"connection closed after {len(data)} of {n} bytes")
        data += chunk
    return bytes(data)

# Unlike recv_frame(), reads exactly one whole frame.
def read_frame(sock: socket.socket) -> Response:
    _, flags, _, opcode, length = struct.unpack("!BBHBI", _recv_exactly(sock, 9))
    # FIXME: Support compressed frames.
    assert not flags & COMPRESSION_FLAG, "Compressed frames aren't supported"
    return Response(opcode, flags, _recv_exactly(sock, length))

# Sends a request and returns the response to it.
def request(sock: socket.socket, opcode: int, body: bytes) -> Response:
    send_frame(sock, opcode=opcode, stream=3, body=body)
    return read_frame(sock)

# A connection ready for queries. It's started with the given STARTUP options
# on top of CQL_VERSION, and authenticated with the default superuser
# credentials if the server requires it.
@contextmanager
def connection(host: str, port: int, startup_options: dict[str, str] = {}):
    sock = socket.create_connection((host, port))
    try:
        response = request(sock, STARTUP, encode_string_map({"CQL_VERSION": "3.0.0", **startup_options}))
        if response.opcode == AUTHENTICATE:
            response = request(sock, AUTH_RESPONSE, b'\x00cassandra\x00cassandra')
            assert response.opcode == AUTH_SUCCESS, f"expected AUTH_SUCCESS, got {hex(response.opcode)}"
        else:
            assert response.opcode == READY, f"expected READY or AUTHENTICATE, got {hex(response.opcode)}"
        yield sock
    finally:
        sock.close()
