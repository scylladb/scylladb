#!/usr/bin/env python3
from contextlib import contextmanager
import socket
import struct
import sys
import random
import argparse
import time
import re
import unittest

server_addr = None
call = None
args = None

class TimeoutError(Exception):
    pass

@contextmanager
def tcp_connection(timeout=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
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

def tcp_call(msg, timeout=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(server_addr)
    s.send(msg.encode())
    s.shutdown(socket.SHUT_WR)
    data = recv_all(s)
    s.close()
    return data

def udp_call(msg, timeout=1):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
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

    def getStat(self, name, call_fn=None):
        if not call_fn: call_fn = call
        resp = call_fn('stats\r\n').decode()
        m = re.search(r'STAT %s (?P<value>.+)' % re.escape(name), resp, re.MULTILINE)
        return m.group('value')

    def flush(self):
        self.assertEqual(call('flush_all\r\n'), b'OK\r\n')

    def tearDown(self):
        self.flush()

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

    @slow
    def test_connection_statistics(self):
        with tcp_connection() as conn:
            curr_connections = int(self.getStat('curr_connections', call_fn=conn))
            total_connections = int(self.getStat('total_connections', call_fn=conn))
            with tcp_connection() as conn2:
                self.assertEquals(curr_connections + 1, int(self.getStat('curr_connections', call_fn=conn)))
                self.assertEquals(total_connections + 1, int(self.getStat('total_connections', call_fn=conn)))
            self.assertEquals(total_connections + 1, int(self.getStat('total_connections', call_fn=conn)))
            time.sleep(0.1)
            self.assertEquals(curr_connections, int(self.getStat('curr_connections', call_fn=conn)))

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

    def test_curr_items_stat(self):
        self.assertEquals(0, int(self.getStat('curr_items')))
        self.setKey('key')
        self.assertEquals(1, int(self.getStat('curr_items')))
        self.delete('key')
        self.assertEquals(0, int(self.getStat('curr_items')))

    def test_how_stats_change_with_different_commands(self):
        get_count = int(self.getStat('cmd_get'))
        set_count = int(self.getStat('cmd_set'))
        flush_count = int(self.getStat('cmd_flush'))
        total_items = int(self.getStat('total_items'))
        get_misses = int(self.getStat('get_misses'))
        get_hits = int(self.getStat('get_hits'))
        cas_hits = int(self.getStat('cas_hits'))
        cas_badval = int(self.getStat('cas_badval'))
        cas_misses = int(self.getStat('cas_misses'))
        delete_misses = int(self.getStat('delete_misses'))
        delete_hits = int(self.getStat('delete_hits'))
        curr_connections = int(self.getStat('curr_connections'))
        incr_hits = int(self.getStat('incr_hits'))
        incr_misses = int(self.getStat('incr_misses'))
        decr_hits = int(self.getStat('decr_hits'))
        decr_misses = int(self.getStat('decr_misses'))

        call('get key\r\n')
        get_count += 1
        get_misses += 1

        call('gets key\r\n')
        get_count += 1
        get_misses += 1

        call('set key1 0 0 1\r\na\r\n')
        set_count += 1
        total_items += 1

        call('get key1\r\n')
        get_count += 1
        get_hits += 1

        call('add key1 0 0 1\r\na\r\n')
        set_count += 1

        call('add key2 0 0 1\r\na\r\n')
        set_count += 1
        total_items += 1

        call('replace key1 0 0 1\r\na\r\n')
        set_count += 1
        total_items += 1

        call('replace key3 0 0 1\r\na\r\n')
        set_count += 1

        call('cas key4 0 0 1 1\r\na\r\n')
        set_count += 1
        cas_misses += 1

        call('cas key1 0 0 1 %d\r\na\r\n' % self.getItemVersion('key1'))
        set_count += 1
        get_count += 1
        get_hits += 1
        cas_hits += 1
        total_items += 1

        call('cas key1 0 0 1 %d\r\na\r\n' % (self.getItemVersion('key1') + 1))
        set_count += 1
        get_count += 1
        get_hits += 1
        cas_badval += 1

        call('delete key1\r\n')
        delete_hits += 1

        call('delete key1\r\n')
        delete_misses += 1

        call('incr num 1\r\n')
        incr_misses += 1
        call('decr num 1\r\n')
        decr_misses += 1

        call('set num 0 0 1\r\n0\r\n')
        set_count += 1
        total_items += 1

        call('incr num 1\r\n')
        incr_hits += 1
        call('decr num 1\r\n')
        decr_hits += 1

        self.flush()
        flush_count += 1

        self.assertEquals(get_count, int(self.getStat('cmd_get')))
        self.assertEquals(set_count, int(self.getStat('cmd_set')))
        self.assertEquals(flush_count, int(self.getStat('cmd_flush')))
        self.assertEquals(total_items, int(self.getStat('total_items')))
        self.assertEquals(get_hits, int(self.getStat('get_hits')))
        self.assertEquals(get_misses, int(self.getStat('get_misses')))
        self.assertEquals(cas_misses, int(self.getStat('cas_misses')))
        self.assertEquals(cas_hits, int(self.getStat('cas_hits')))
        self.assertEquals(cas_badval, int(self.getStat('cas_badval')))
        self.assertEquals(delete_misses, int(self.getStat('delete_misses')))
        self.assertEquals(delete_hits, int(self.getStat('delete_hits')))
        self.assertEquals(0, int(self.getStat('curr_items')))
        self.assertEquals(curr_connections, int(self.getStat('curr_connections')))
        self.assertEquals(incr_misses, int(self.getStat('incr_misses')))
        self.assertEquals(incr_hits, int(self.getStat('incr_hits')))
        self.assertEquals(decr_misses, int(self.getStat('decr_misses')))
        self.assertEquals(decr_hits, int(self.getStat('decr_hits')))

    def test_incr(self):
        self.assertEqual(call('incr key 0\r\n'), b'NOT_FOUND\r\n')

        self.assertEqual(call('set key 0 0 1\r\n0\r\n'), b'STORED\r\n')
        self.assertEqual(call('incr key 0\r\n'), b'0\r\n')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 1\r\n0\r\nEND\r\n')

        self.assertEqual(call('incr key 1\r\n'), b'1\r\n')
        self.assertEqual(call('incr key 2\r\n'), b'3\r\n')
        self.assertEqual(call('incr key %d\r\n' % (pow(2, 64) - 1)), b'2\r\n')
        self.assertEqual(call('incr key %d\r\n' % (pow(2, 64) - 3)), b'18446744073709551615\r\n')
        self.assertRegexpMatches(call('incr key 1\r\n').decode(), r'0(\w*)?\r\n')

        self.assertEqual(call('set key 0 0 2\r\n1 \r\n'), b'STORED\r\n')
        self.assertEqual(call('incr key 1\r\n'), b'2\r\n')

        self.assertEqual(call('set key 0 0 2\r\n09\r\n'), b'STORED\r\n')
        self.assertEqual(call('incr key 1\r\n'), b'10\r\n')

    def test_decr(self):
        self.assertEqual(call('decr key 0\r\n'), b'NOT_FOUND\r\n')

        self.assertEqual(call('set key 0 0 1\r\n7\r\n'), b'STORED\r\n')
        self.assertEqual(call('decr key 1\r\n'), b'6\r\n')
        self.assertEqual(call('get key\r\n'), b'VALUE key 0 1\r\n6\r\nEND\r\n')

        self.assertEqual(call('decr key 6\r\n'), b'0\r\n')
        self.assertEqual(call('decr key 2\r\n'), b'0\r\n')

        self.assertEqual(call('set key 0 0 2\r\n20\r\n'), b'STORED\r\n')
        self.assertRegexpMatches(call('decr key 11\r\n').decode(), r'^9( )?\r\n$')

        self.assertEqual(call('set key 0 0 3\r\n100\r\n'), b'STORED\r\n')
        self.assertRegexpMatches(call('decr key 91\r\n').decode(), r'^9(  )?\r\n$')

        self.assertEqual(call('set key 0 0 2\r\n1 \r\n'), b'STORED\r\n')
        self.assertEqual(call('decr key 1\r\n'), b'0\r\n')

        self.assertEqual(call('set key 0 0 2\r\n09\r\n'), b'STORED\r\n')
        self.assertEqual(call('decr key 1\r\n'), b'8\r\n')

    def test_incr_and_decr_on_invalid_input(self):
        error_msg = b'CLIENT_ERROR cannot increment or decrement non-numeric value\r\n'
        for cmd in ['incr', 'decr']:
            for value in ['', '-1', 'a', '0x1', '18446744073709551616']:
                self.assertEqual(call('set key 0 0 %d\r\n%s\r\n' % (len(value), value)), b'STORED\r\n')
                prev = call('get key\r\n')
                self.assertEqual(call(cmd + ' key 1\r\n'), error_msg, "cmd=%s, value=%s" % (cmd, value))
                self.assertEqual(call('get key\r\n'), prev)
                self.delete('key')

def wait_for_memcache_tcp(timeout=4):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    timeout_at = time.time() + timeout
    while True:
        if time.time() >= timeout_at:
            raise TimeoutError()
        try:
            s.connect(server_addr)
            s.close()
            break
        except ConnectionRefusedError:
            time.sleep(0.1)


def wait_for_memcache_udp(timeout=4):
    timeout_at = time.time() + timeout
    while True:
        if time.time() >= timeout_at:
            raise TimeoutError()
        try:
            udp_call('version\r\n', timeout=0.2)
            break
        except socket.timeout:
            pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="memcache protocol tests")
    parser.add_argument('--server', '-s', action="store", help="server adddress in <host>:<port> format", default="localhost:11211")
    parser.add_argument('--udp', '-U', action="store_true", help="Use UDP protocol")
    parser.add_argument('--fast',  action="store_true", help="Run only fast tests")
    args = parser.parse_args()

    host, port = args.server.split(':')
    server_addr = (host, int(port))

    if args.udp:
        call = udp_call
        wait_for_memcache_udp()
    else:
        call = tcp_call
        wait_for_memcache_tcp()

    runner = unittest.TextTestRunner()
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTest(loader.loadTestsFromTestCase(TestCommands))
    if not args.udp:
        suite.addTest(loader.loadTestsFromTestCase(TcpSpecificTests))
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)
