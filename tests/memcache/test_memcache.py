#!/usr/bin/env python3
from contextlib import contextmanager
import socket
import struct
import random
import argparse
import time
import re
import unittest

server_addr = None
call = None
args = None

@contextmanager
def tcp_connection():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(server_addr)
    def call(msg):
        s.send(msg.encode())
        return s.recv(16*1024)
    yield call
    s.close()

def slow(f):
    def wrapper(self):
        if args.fast:
            raise unittest.SkipTest('Slow')
        return f(self)
    return wrapper

def recv_all(s):
    m = b''
    while True:
        data = s.recv(1024)
        if not data:
            break
        m += data
    return m

def tcp_call(msg):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(server_addr)
    s.send(msg.encode())
    s.shutdown(socket.SHUT_WR)
    data = recv_all(s)
    s.close()
    return data

def udp_call(msg):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    this_req_id = random.randint(-32768, 32767)

    datagram = struct.pack(">hhhh", this_req_id, 0, 1, 0) + msg.encode()
    sock.sendto(datagram, server_addr)

    messages = {}
    n_determined = None
    while True:
        data, addr = sock.recvfrom(1500)
        req_id, seq, n, res = struct.unpack_from(">hhhh", data)
        content = data[8:]

        if n_determined and n_determined != n:
            raise Exception('Inconsitent number of total messages, %d and %d' % (n_determined, n))
        n_determined = n

        if req_id != this_req_id:
            raise Exception('Invalid request id: ' + req_id + ', expected ' + this_req_id)

        if seq in messages:
            raise Exception('Duplicate message for seq=' + seq)

        messages[seq] = content
        if len(messages) == n:
            break

    msg = b''
    for k, v in sorted(messages.items(), key=lambda e: e[0]):
        msg += v

    sock.close()
    return msg

class MemcacheTest(unittest.TestCase):
    def set(self, key, value, flags=0, expiry=0):
        self.assertEqual(call('set %s %d %d %d\r\n%s\r\n' % (key, flags, expiry, len(value), value)), b'STORED\r\n')

    def delete(self, key):
        self.assertEqual(call('delete %s\r\n' % key), b'DELETED\r\n')

    def assertHasKey(self, key):
        resp = call('get %s\r\n' % key)
        if not resp.startswith(('VALUE %s' % key).encode()):
            self.fail('Key \'%s\' should be present, but got: %s' % (key, resp.decode()))

    def assertNoKey(self, key):
        resp = call('get %s\r\n' % key)
        if resp != b'END\r\n':
            self.fail('Key \'%s\' should not be present, but got: %s' % (key, resp.decode()))

    def setKey(self, key):
        self.set(key, 'some value')

    def getItemVersion(self, key):
        m = re.match(r'VALUE %s \d+ \d+ (?P<version>\d+)' % key, call('gets %s\r\n' % key).decode())
        return int(m.group('version'))

class TcpSpecificTests(MemcacheTest):
    def test_recovers_from_errors_in_the_stream(self):
        with tcp_connection() as conn:
            self.assertEqual(conn('get\r\n'), b'ERROR\r\n')
            self.assertEqual(conn('get key\r\n'), b'END\r\n')

    def test_incomplete_command_results_in_error(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(server_addr)
        s.send(b'get')
        s.shutdown(socket.SHUT_WR)
        self.assertEqual(recv_all(s), b'ERROR\r\n')
        s.close()

    def test_stream_closed_results_in_error(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(server_addr)
        s.shutdown(socket.SHUT_WR)
        self.assertEqual(recv_all(s), b'')
        s.close()

    def test_unsuccesful_parsing_does_not_leave_data_behind(self):
        with tcp_connection() as conn:
            self.assertEqual(conn('set key 0 0 5\r\nhello\r\n'), b'STORED\r\n')
            self.assertRegexpMatches(conn('delete a b c\r\n'), b'^(CLIENT_)?ERROR.*\r\n$')
            self.assertEqual(conn('get key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')
            self.assertEqual(conn('delete key\r\n'), b'DELETED\r\n')

    def test_flush_all_no_reply(self):
        self.assertEqual(call('flush_all noreply\r\n'), b'')

    def test_set_no_reply(self):
        self.assertEqual(call('set key 0 0 5 noreply\r\nhello\r\nget key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')
        self.delete('key')

    def test_delete_no_reply(self):
        self.setKey('key')
        self.assertEqual(call('delete key noreply\r\nget key\r\n'), b'END\r\n')

    def test_add_no_reply(self):
        self.assertEqual(call('add key 0 0 1 noreply\r\na\r\nget key\r\n'), b'VALUE key 0 1\r\na\r\nEND\r\n')
        self.delete('key')

    def test_replace_no_reply(self):
        self.assertEqual(call('set key 0 0 1\r\na\r\n'), b'STORED\r\n')
        self.assertEqual(call('replace key 0 0 1 noreply\r\nb\r\nget key\r\n'), b'VALUE key 0 1\r\nb\r\nEND\r\n')
        self.delete('key')

    def test_cas_noreply(self):
        self.assertNoKey('key')
        self.assertEqual(call('cas key 0 0 1 1 noreply\r\na\r\n'), b'')
        self.assertNoKey('key')

        self.assertEqual(call('add key 0 0 5\r\nhello\r\n'), b'STORED\r\n')
        version = self.getItemVersion('key')

        self.assertEqual(call('cas key 1 0 5 %d noreply\r\naloha\r\n' % (version + 1)), b'')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')

        self.assertEqual(call('cas key 1 0 5 %d noreply\r\naloha\r\n' % (version)), b'')
        self.assertEqual(call('get key\r\n'), b'VALUE key 1 5\r\naloha\r\nEND\r\n')

        self.delete('key')


class TestCommands(MemcacheTest):
    def test_basic_commands(self):
        self.assertEqual(call('get key\r\n'), b'END\r\n')
        self.assertEqual(call('set key 0 0 5\r\nhello\r\n'), b'STORED\r\n')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')
        self.assertEqual(call('delete key\r\n'), b'DELETED\r\n')
        self.assertEqual(call('delete key\r\n'), b'NOT_FOUND\r\n')
        self.assertEqual(call('get key\r\n'), b'END\r\n')

    def test_error_handling(self):
        self.assertEqual(call('get\r\n'), b'ERROR\r\n')

    @slow
    def test_expiry(self):
        self.assertEqual(call('set key 0 1 5\r\nhello\r\n'), b'STORED\r\n')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')
        time.sleep(1)
        self.assertEqual(call('get key\r\n'), b'END\r\n')

    @slow
    def test_expiry_at_epoch_time(self):
        expiry = int(time.time()) + 1
        self.assertEqual(call('set key 0 %d 5\r\nhello\r\n' % expiry), b'STORED\r\n')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 5\r\nhello\r\nEND\r\n')
        time.sleep(2)
        self.assertEqual(call('get key\r\n'), b'END\r\n')

    def test_mutliple_keys_in_get(self):
        self.assertEqual(call('set key1 0 0 2\r\nv1\r\n'), b'STORED\r\n')
        self.assertEqual(call('set key 0 0 2\r\nv2\r\n'), b'STORED\r\n')
        self.assertEqual(call('get key1 key\r\n'), b'VALUE key1 0 2\r\nv1\r\nVALUE key 0 2\r\nv2\r\nEND\r\n')
        self.delete("key")
        self.delete("key1")

    def test_flush_all(self):
        self.set('key', 'value')
        self.assertEqual(call('flush_all\r\n'), b'OK\r\n')
        self.assertNoKey('key')

    def test_keys_set_after_flush_remain(self):
        self.assertEqual(call('flush_all\r\n'), b'OK\r\n')
        self.setKey('key')
        self.assertHasKey('key')
        self.delete('key')

    @slow
    def test_flush_all_with_timeout_flushes_all_keys_even_those_set_after_flush(self):
        self.setKey('key')
        self.assertEqual(call('flush_all 2\r\n'), b'OK\r\n')
        self.assertHasKey('key')
        self.setKey('key2')
        time.sleep(2)
        self.assertNoKey('key')
        self.assertNoKey('key2')

    @slow
    def test_subsequent_flush_is_merged(self):
        self.setKey('key')
        self.assertEqual(call('flush_all 2\r\n'), b'OK\r\n') # Can flush in anything between 1-2
        self.assertEqual(call('flush_all 4\r\n'), b'OK\r\n') # Can flush in anything between 3-4
        time.sleep(2)
        self.assertHasKey('key')
        self.setKey('key2')
        time.sleep(4)
        self.assertNoKey('key')
        self.assertNoKey('key2')

    @slow
    def test_immediate_flush_cancels_delayed_flush(self):
        self.assertEqual(call('flush_all 2\r\n'), b'OK\r\n')
        self.assertEqual(call('flush_all\r\n'), b'OK\r\n')
        self.setKey('key')
        time.sleep(1)
        self.assertHasKey('key')
        self.delete('key')

    @slow
    def test_flushing_in_the_past(self):
        self.setKey('key1')
        time.sleep(1)
        self.setKey('key2')
        key2_time = int(time.time())
        self.assertEqual(call('flush_all %d\r\n' % (key2_time - 1)), b'OK\r\n')
        self.assertNoKey("key1")
        self.assertNoKey("key2")

    @slow
    def test_memcache_does_not_crash_when_flushing_with_already_expred_items(self):
        self.assertEqual(call('set key1 0 2 5\r\nhello\r\n'), b'STORED\r\n')
        time.sleep(1)
        self.assertEqual(call('flush_all\r\n'), b'OK\r\n')

    def test_response_spanning_many_datagrams(self):
        key1_data = '1' * 1000
        key2_data = '2' * 1000
        key3_data = '3' * 1000
        self.set('key1', key1_data)
        self.set('key2', key2_data)
        self.set('key3', key3_data)
        self.assertEqual(call('get key1 key2 key3\r\n').decode(),
            'VALUE key1 0 %d\r\n%s\r\n' \
            'VALUE key2 0 %d\r\n%s\r\n' \
            'VALUE key3 0 %d\r\n%s\r\n' \
            'END\r\n' % (len(key1_data), key1_data, len(key2_data), key2_data, len(key3_data), key3_data))
        self.delete('key1')
        self.delete('key2')
        self.delete('key3')

    def test_version(self):
        self.assertRegexpMatches(call('version\r\n'), b'^VERSION .*\r\n$')

    def test_add(self):
        self.assertEqual(call('add key 0 0 1\r\na\r\n'), b'STORED\r\n')
        self.assertEqual(call('add key 0 0 1\r\na\r\n'), b'NOT_STORED\r\n')
        self.delete('key')

    def test_replace(self):
        self.assertEqual(call('add key 0 0 1\r\na\r\n'), b'STORED\r\n')
        self.assertEqual(call('replace key 0 0 1\r\na\r\n'), b'STORED\r\n')
        self.delete('key')
        self.assertEqual(call('replace key 0 0 1\r\na\r\n'), b'NOT_STORED\r\n')

    def test_cas_and_gets(self):
        self.assertEqual(call('cas key 0 0 1 1\r\na\r\n'), b'NOT_FOUND\r\n')
        self.assertEqual(call('add key 0 0 5\r\nhello\r\n'), b'STORED\r\n')
        version = self.getItemVersion('key')

        self.assertEqual(call('set key 1 0 5\r\nhello\r\n'), b'STORED\r\n')
        self.assertEqual(call('gets key\r\n').decode(), 'VALUE key 1 5 %d\r\nhello\r\nEND\r\n' % (version + 1))

        self.assertEqual(call('cas key 0 0 5 %d\r\nhello\r\n' % (version)), b'EXISTS\r\n')
        self.assertEqual(call('cas key 0 0 5 %d\r\naloha\r\n' % (version + 1)), b'STORED\r\n')
        self.assertEqual(call('gets key\r\n').decode(), 'VALUE key 0 5 %d\r\naloha\r\nEND\r\n' % (version + 2))

        self.delete('key')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="memcache protocol tests")
    parser.add_argument('--server', '-s', action="store", help="server adddress in <host>:<port> format", default="localhost:11211")
    parser.add_argument('--udp', '-U', action="store_true", help="Use UDP protocol")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    args = parser.parse_args()

    host, port = args.server.split(':')
    server_addr = (host, int(port))

    call = udp_call if args.udp else tcp_call

    runner = unittest.TextTestRunner()
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TestCommands))
    if not args.udp:
        suite.addTest(loader.loadTestsFromTestCase(TcpSpecificTests))
    runner.run(suite)
