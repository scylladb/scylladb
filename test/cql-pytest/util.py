# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
##################################################################

# Various utility functions which are useful for multiple tests.
# Note that fixtures aren't here - they are in conftest.py.

import string
import random
import time
import socket
import os
import requests
import collections
import ssl
from contextlib import contextmanager

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

def random_bytes(length=10):
    return bytearray(random.getrandbits(8) for _ in range(length))

# A function for picking a unique name for test keyspace or table.
# This name doesn't need to be quoted in CQL - it only contains
# lowercase letters, numbers, and underscores, and starts with a letter.
unique_name_prefix = 'cql_test_'
def unique_name():
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)
unique_name.last_ms = 0

# Functions for picking a unique key to use when multiple tests want to use
# the same shared table and need to pick different keys so as not to collide.
# Because different runs do not share the same table (unique_name() above
# is used to pick the table name), the uniqueness of the keys we generate
# here does not need to be global - we can just use a simple counter to
# guarantee uniqueness.
def unique_key_string():
    unique_key_string.i += 1
    return 's' + str(unique_key_string.i)
unique_key_string.i = 0

def unique_key_int():
    unique_key_int.i += 1
    return unique_key_int.i
unique_key_int.i = 0


def is_scylla(cql):
    """ Check whether we are running against Scylla or not """
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    return any('scylla' in name for name in names)

def keyspace_has_tablets(cql, keyspace):
    """ Return true if the keyspace was created with tablets.

    We support running cql-pytest against an older version of scylla, so we do
    the detection in a way that accounts for scylla possibly not even knowing
    what tablets is.
    For cassandra, this will always return no.

    If the keyspace was created with tablets, it will have an entry in
    `system_schema.scylla_keyspaces`, with `initial_tablets` set.
    So here, we simply query this table, looking for a partition for the
    appropriate keyspace. If the result has the `initial_tablets` column and it
    is set, the keyspace has tablets.
    """
    if not is_scylla(cql):
        return False

    try:
        res = list(cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name='{keyspace}'"))
    except:
        # Antique versions of Scylla are is_scylla() but did not have
        # the scylla_keyspaces table. They didn't have tablets either, so
        # we should just return False.
        return False
    # The row might exist due to storage related options, but the tablets
    # related fields are null.
    # So we check that:
    # * the row exists
    # * `initial_tablets` has a value
    if not res:
        return False
    return getattr(res[0], "initial_tablets", None) is not None

# A utility function for creating a new temporary keyspace with given options.
# It can be used in a "with", as:
#   with new_test_keyspace(cql, '...') as keyspace:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_keyspace(cql, opts):
    keyspace = unique_name()
    cql.execute("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        cql.execute("DROP KEYSPACE " + keyspace)

# A utility function for creating a new temporary table with a given schema.
# Because Scylla becomes slower when a huge number of uniquely-named tables
# are created and deleted (see https://github.com/scylladb/scylla/issues/7620)
# we keep here a list of previously used but now deleted table names, and
# reuse one of these names when possible.
# This function can be used in a "with", as:
#   with create_table(cql, test_keyspace, '...') as table:
previously_used_table_names = []
@contextmanager
def new_test_table(cql, keyspace, schema, extra=""):
    global previously_used_table_names
    if not previously_used_table_names:
        previously_used_table_names.append(unique_name())
    table_name = previously_used_table_names.pop()
    table = keyspace + "." + table_name
    cql.execute("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        cql.execute("DROP TABLE " + table)
        previously_used_table_names.append(table_name)

# A utility function for creating a new temporary user-defined type.
@contextmanager
def new_type(cql, keyspace, cmd, name=None):
    type_name = keyspace + "." + (name or unique_name())
    cql.execute("CREATE TYPE " + type_name + " " + cmd)
    try:
        yield type_name
    finally:
        cql.execute("DROP TYPE " + type_name)

# A utility function for creating a new temporary user-defined function.
@contextmanager
def new_function(cql, keyspace, body, name=None, args=None):
    fun = name if name else unique_name()
    cql.execute(f"CREATE FUNCTION {keyspace}.{fun} {body}")
    try:
        yield fun
    finally:
        if args:
            cql.execute(f"DROP FUNCTION {keyspace}.{fun}({args})")
        else:
            cql.execute(f"DROP FUNCTION {keyspace}.{fun}")

# A utility function for creating a new temporary user-defined aggregate.
@contextmanager
def new_aggregate(cql, keyspace, body):
    aggr = unique_name()
    cql.execute(f"CREATE AGGREGATE {keyspace}.{aggr} {body}")
    try:
        yield aggr
    finally:
        cql.execute(f"DROP AGGREGATE {keyspace}.{aggr}")

# A utility function for creating a new temporary materialized view in
# an existing table.
@contextmanager
def new_materialized_view(cql, table, select, pk, where, extra=""):
    keyspace = table.split('.')[0]
    mv = keyspace + "." + unique_name()
    cql.execute(f"CREATE MATERIALIZED VIEW {mv} AS SELECT {select} FROM {table} WHERE {where} PRIMARY KEY ({pk}) {extra}")
    try:
        yield mv
    finally:
        cql.execute(f"DROP MATERIALIZED VIEW {mv}")

# A utility function for creating a new temporary secondary index of
# an existing table.
@contextmanager
def new_secondary_index(cql, table, column, name='', extra=''):
    keyspace = table.split('.')[0]
    if not name:
        name = unique_name()
    cql.execute(f"CREATE INDEX {name} ON {table} ({column}) {extra}")
    try:
        yield f"{keyspace}.{name}"
    finally:
        cql.execute(f"DROP INDEX {keyspace}.{name}")

def index_table_name(index_name : str):
    return f"{index_name}_index"

# Helper function for establishing a connection with given username and password
@contextmanager
def cql_session(host, port, is_ssl, username, password, request_timeout=120):
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout=request_timeout)
    if is_ssl:
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[host],
        port=int(port),
        # TODO: make the protocol version an option, to allow testing with
        # different versions. If we drop this setting completely, it will
        # mean pick the latest version supported by the client and the server.
        protocol_version=4,
        auth_provider=PlainTextAuthProvider(username=username, password=password),
        ssl_context=ssl_context,
        # The default timeout for new connections is 5 seconds, and for
        # requests made by the control connection is 2 seconds. These should
        # have been more than enough, but in some extreme cases with a very
        # slow debug build running on a very busy machine, they may not be.
        # so let's increase them to 60 seconds. See issue #11289.
        connect_timeout = 60,
        control_connection_timeout = 60,
    )
    yield cluster.connect()
    cluster.shutdown()

@contextmanager
def new_user(cql, username='', with_superuser_privileges=False):
    if not username:
        username = unique_name()
    cql.execute(f"CREATE ROLE {username} WITH PASSWORD = '{username}' AND SUPERUSER = {with_superuser_privileges} AND LOGIN = true")
    try:
        yield username
    finally:
        cql.execute(f"DROP ROLE {username}")

@contextmanager
def new_session(cql, username):
    endpoint = cql.hosts[0].endpoint
    with cql_session(host=endpoint.address, port=endpoint.port, is_ssl=(cql.cluster.ssl_context is not None), username=username, password=username) as session:
        yield session
        session.shutdown()

# new_cql() returns a new object similar to the given cql fixture,
# connected to the same endpoint but with a separate connection.
# This can be useful for tests which require a separate connection -
# for example for testing the "USE" statement (which, after used once
# on a connection, cannot be undone).
@contextmanager
def new_cql(cql):
    session = cql.cluster.connect()
    try:
        yield session
    finally:
        session.shutdown()

def project(column_name_string, rows):
    """Returns a list of column values from each of the rows."""
    return [getattr(r, column_name_string) for r in rows]

# Utility function for trying to find a local process which is listening to
# the address and port to which our our CQL connection is connected. If such a
# process exists, return its process id (as a string). Otherwise, return None.
# Note that the local process needs to belong to the same user running this
# test, or it cannot be found.
def local_process_id(cql):
    ip = socket.gethostbyname(cql.cluster.contact_points[0])
    port = cql.cluster.port
    # Implement something like the shell "lsof -Pni @{ip}:{port}", just
    # using /proc without any external shell command.
    # First, we look in /proc/net/tcp for a LISTEN socket (state 0x0A) at the
    # desired local address. The address is specially-formatted hex of the ip
    # and port, with 0100007F:2352 for 127.0.0.1:9042. We check for two
    # listening addresses: one is the specific IP address given, and the
    # other is listening on address 0 (INADDR_ANY).
    ip2hex = lambda ip: ''.join([f'{int(x):02X}' for x in reversed(ip.split('.'))])
    port2hex = lambda port: f'{int(port):04X}'
    addr1 = ip2hex(ip) + ':' + port2hex(port)
    addr2 = ip2hex('0.0.0.0') + ':' + port2hex(port)
    LISTEN = '0A'
    with open('/proc/net/tcp', 'r') as f:
        for line in f:
            cols = line.split()
            if cols[3] == LISTEN and (cols[1] == addr1 or cols[1] == addr2):
                inode = cols[9]
                break
        else:
            # Didn't find a process listening on the given address
            return None
    # Now look in /proc/*/fd/* for processes that have this socket "inode"
    # as one of its open files. We can only find a process that belongs to
    # the same user.
    target = f'socket:[{inode}]'
    for proc in os.listdir('/proc'):
        if not proc.isnumeric():
            continue
        dir = f'/proc/{proc}/fd/'
        try:
            for fd in os.listdir(dir):
                if os.readlink(dir + fd) == target:
                    # Found the process!
                    return proc
        except:
            # Ignore errors. We can't check processes we don't own.
            pass
    return None

# user_type("a", 1, "b", 2) creates a named tuple with component names "a", "b"
# and values 1, 2. The return of this function can be used to bind to a UDT.
# The number of arguments is assumed to be even.
def user_type(*args):
    return collections.namedtuple('user_type', args[::2])(*args[1::2])

class config_value_context:
    """Change the value of a config item while the context is active.

    The config item has to be live-updatable.
    """
    def __init__(self, cql, key, value):
        self._cql = cql
        self._key = key
        self._value = value
        self._original_value = None
        self._select = cql.prepare("SELECT value FROM system.config WHERE name=?")
        self._update = cql.prepare("UPDATE system.config SET value=? WHERE name=?")

    def __enter__(self):
        self._original_value = self._cql.execute(self._select, (self._key,)).one().value
        self._cql.execute(self._update, (self._value, self._key))

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._cql.execute(self._update, (self._original_value, self._key))

class ScyllaMetrics:
    def __init__(self, lines):
        self._lines = lines
    @staticmethod
    def query(cql):
        url = f'http://{cql.cluster.contact_points[0]}:9180/metrics'
        return ScyllaMetrics(requests.get(url).text.split('\n'))
    def get(self, name, labels = None, shard='total'):
        result = None
        for l in self._lines:
            if not l.startswith(name):
                continue
            labels_start = l.find('{')
            labels_finish = l.find('}')
            if labels_start == -1 or labels_finish == -1:
                raise ValueError(f'invalid metric format [{l}]')
            def match_kv(kv):
                key, val = kv.split('=')
                val = val.strip('"')
                return shard == 'total' or val == shard if key == 'shard' \
                    else labels is None or labels.get(key, None) == val
            match = all(match_kv(kv) for kv in l[labels_start + 1:labels_finish].split(','))
            if match:
                value = float(l[labels_finish + 2:])
                if result is None:
                    result = value
                else:
                    result += value
                if shard != 'total':
                    break
        return result
