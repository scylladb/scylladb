#
# Copyright (C) 2019-present ScyllaDB
#
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
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import socket
import logging

logger = logging.getLogger('redis-test')


class RedisSocket:

    def __init__(self, host='localhost', port=6379):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect(host, port)

    def connect(self, host, port):
        self.socket.connect((host, port))

    def send(self, content=''):
        self.socket.send(content)

    def recv(self, buf_len=1024):
        return self.socket.recv(buf_len)

    def shutdown(self):
        self.socket.shutdown(socket.SHUT_WR)

    def close(self):
        self.socket.close()

def verify_cmd_response(host, port, cmd, expect_ret, shutdown=False):
    rs = RedisSocket(host, port)
    rs.send(cmd.encode())
    if shutdown:
        rs.shutdown()
    ret = rs.recv().decode()
    logger.debug('Received content size: %s' % len(ret))
    logger.debug(ret)
    assert ret == expect_ret
    rs.close()

def test_ping(redis_host, redis_port):
    verify_cmd_response(redis_host, redis_port, '*1\r\n$4\r\nping\r\n', '+PONG\r\n')

def test_eof(redis_host, redis_port):
    # shutdown socket, and read nothing
    verify_cmd_response(redis_host, redis_port, "", "", shutdown=True)

    # a EOF char `\x04` should be triggered parse error
    verify_cmd_response(redis_host, redis_port, "\x04", "-ERR unknown command ''\r\n", shutdown=True)

def test_ping_and_eof(redis_host, redis_port):
    # regular ping with shutdown
    verify_cmd_response(redis_host, redis_port, '*1\r\n$4\r\nping\r\n', '+PONG\r\n', shutdown=True)

    # a EOF char `\x04` should be triggered parse error
    verify_cmd_response(redis_host, redis_port, "*1\r\n$4\r\nping\r\n\x04", "+PONG\r\n-ERR unknown command ''\r\n", shutdown=True)
