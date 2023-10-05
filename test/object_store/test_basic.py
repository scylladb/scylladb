#!/usr/bin/env python3
# Use the run.py library from ../cql-pytest:
import sys
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
import run
from util import format_tuples

import os
import requests
import signal
import yaml
import pytest
import xml.etree.ElementTree as ET

from contextlib import contextmanager
from test.pylib.rest_client import ScyllaRESTAPIClient
from cassandra.protocol import ConfigurationException


def get_scylla_with_s3_cmd(ssl, s3_server):
    '''return a function which in turn returns the command for running scylla'''
    scylla = run.find_scylla()
    print('Scylla under test:', scylla)

    def make_run_cmd(pid, d):
        '''return the command args and environmental variables for running scylla'''
        if ssl:
            cmd, env = run.run_scylla_ssl_cql_cmd(pid, d)
        else:
            cmd, env = run.run_scylla_cmd(pid, d)

        cmd += ['--object-storage-config-file', s3_server.config_file]
        return cmd, env
    return make_run_cmd


def check_with_cql(ip, ssl):
    '''return a checker which checks the readiness of scylla'''
    def checker():
        if ssl:
            return run.check_ssl_cql(ip)
        else:
            return run.check_cql(ip)
    return checker


def run_with_dir(run_cmd_gen, run_dir):
    print(f'Start scylla (dir={run_dir}')
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, {})
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGQUIT, signal.SIGTERM})
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # child
        cmd, env = run_cmd_gen(os.getpid(), run_dir)
        log = os.path.join(run_dir, 'log')
        log_fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, mode=0o666)
        # redirect stdout and stderr to the log file
        # close and dup2 the original fds associated with stdout and stderr,
        # pytest changes sys.stdout and sys.stderr to the its output buffers
        # to capture them. so, if we intent to redirect the child process's
        # stdout and stderr to the specified fd, we have to use the fd numbers
        # *used* by the child process, not the ones used by the test.
        outputs = [(sys.stdout, 1),
                   (sys.stderr, 2)]
        for output, output_fd in outputs:
            output.flush()
            os.close(output_fd)
            os.dup2(log_fd, output_fd)
        os.setsid()
        os.execve(cmd[0], cmd, dict(os.environ, **env))
    # parent
    signal.pthread_sigmask(signal.SIG_SETMASK, mask)
    return pid


def kill_with_dir(old_pid, run_dir):
    try:
        print('Kill scylla')
        os.killpg(old_pid, 2)
        os.waitpid(old_pid, 0)
    except ProcessLookupError:
        pass
    scylla_link = os.path.join(run_dir, 'test_scylla')
    os.unlink(scylla_link)


class Cluster:
    def __init__(self, cql, ip: str, pid):
        self.cql = cql
        self.ip = ip
        self.api = ScyllaRESTAPIClient()
        self.pid = pid

    def reload_config(self):
        os.kill(self.pid, signal.SIGHUP)


@contextmanager
def managed_cluster(run_dir, ssl, s3_server):
    # launch a one-node scylla cluster which uses the give s3_server as its
    # object storage backend, it yields an instance of Cluster
    # before this function returns, the cluster is teared down.
    run_scylla_cmd = get_scylla_with_s3_cmd(ssl, s3_server)
    pid = run_with_dir(run_scylla_cmd, run_dir)
    ip = run.pid_to_ip(pid)
    run.wait_for_services(pid, [check_with_cql(ip, ssl)])
    cluster = run.get_cql_cluster(ip)
    try:
        yield Cluster(cluster, ip, pid)
    finally:
        cluster.shutdown()
        kill_with_dir(pid, run_dir)


def create_ks_and_cf(cql, s3_server):
    ks = 'test_ks'
    cf = 'test_cf'

    replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                      'replication_factor': '1'})
    storage_opts = format_tuples(type='S3',
                                 endpoint=s3_server.address,
                                 bucket=s3_server.bucket_name)

    cql.execute((f"CREATE KEYSPACE {ks} WITH"
                  f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))
    cql.execute(f"CREATE TABLE {ks}.{cf} ( name text primary key, value text );")

    rows = [('0', 'zero'),
            ('1', 'one'),
            ('2', 'two')]
    for row in rows:
        cql_fmt = "INSERT INTO {}.{} ( name, value ) VALUES ('{}', '{}');"
        cql.execute(cql_fmt.format(ks, cf, *row))

    return ks, cf


@pytest.mark.asyncio
async def test_basic(test_tempdir, s3_server, ssl):
    '''verify ownership table is updated, and tables written to S3 can be read after scylla restarts'''

    with managed_cluster(test_tempdir, ssl, s3_server) as cluster:
        print(f'Create keyspace (minio listening at {s3_server.address})')
        conn = cluster.cql.connect()
        ks, cf = create_ks_and_cf(conn, s3_server)

        assert not os.path.exists(os.path.join(test_tempdir, f'data/{ks}')), "S3-backed keyspace has local directory created"
        # Sanity check that the path is constructed correctly
        assert os.path.exists(os.path.join(test_tempdir, 'data/system')), "Datadir is elsewhere"

        desc = conn.execute(f"DESCRIBE KEYSPACE {ks}").one().create_statement
        # The storage_opts wraps options with '{ <options> }' while the DESCRIBE
        # does it like '{<options>}' so strip the corner brances and spaces for check
        assert f"{{'type': 'S3', 'bucket': '{s3_server.bucket_name}', 'endpoint': '{s3_server.address}'}}" in desc, "DESCRIBE generates unexpected storage options"

        res = conn.execute(f"SELECT * FROM {ks}.{cf};")
        rows = {x.name: x.value for x in res}
        assert len(rows) > 0, 'Test table is empty'

        await cluster.api.flush_keyspace(cluster.ip, ks)

        # Check that the ownership table is populated properly
        res = conn.execute("SELECT * FROM system.sstables;")
        for row in res:
            assert row.location.startswith(test_tempdir), \
                f'Unexpected entry location in registry: {row.location}'
            assert row.status == 'sealed', f'Unexpected entry status in registry: {row.status}'

    print('Restart scylla')
    with managed_cluster(test_tempdir, ssl, s3_server) as cluster:
        # Shouldn't be recreated by populator code
        assert not os.path.exists(os.path.join(test_tempdir, f'data/{ks}')), "S3-backed keyspace has local directory resurrected"

        conn = cluster.cql.connect()
        res = conn.execute(f"SELECT * FROM {ks}.{cf};")
        have_res = {x.name: x.value for x in res}
        assert have_res == rows, f'Unexpected table content: {have_res}'

        print('Drop table')
        conn.execute(f"DROP TABLE {ks}.{cf};")
        # Check that the ownership table is de-populated
        res = conn.execute("SELECT * FROM system.sstables;")
        rows = "\n".join(f"{row.location} {row.status}" for row in res)
        assert not rows, 'Unexpected entries in registry'


@pytest.mark.asyncio
async def test_garbage_collect(test_tempdir, s3_server, ssl):
    '''verify ownership table is garbage-collected on boot'''

    def list_bucket(s3_server):
        r = requests.get(f'http://{s3_server.address}:{s3_server.port}/{s3_server.bucket_name}')
        bucket_list_res = ET.fromstring(r.content)
        objects = []
        for elem in bucket_list_res:
            if elem.tag.endswith('Contents'):
                for opt in elem:
                    if opt.tag.endswith('Key'):
                        objects.append(opt.text)
        return objects

    sstable_entries = []

    with managed_cluster(test_tempdir, ssl, s3_server) as cluster:
        print(f'Create keyspace (minio listening at {s3_server.address})')
        conn = cluster.cql.connect()
        ks, cf = create_ks_and_cf(conn, s3_server)

        await cluster.api.flush_keyspace(cluster.ip, ks)
        # Mark the sstables as "removing" to simulate the problem
        res = conn.execute("SELECT * FROM system.sstables;")
        for row in res:
            sstable_entries.append((row.location, row.generation))
        print(f'Found entries: {[ str(ent[1]) for ent in sstable_entries ]}')
        for loc, gen in sstable_entries:
            conn.execute("UPDATE system.sstables SET status = 'removing'"
                         f" WHERE location = '{loc}' AND generation = {gen};")

    print('Restart scylla')
    with managed_cluster(test_tempdir, ssl, s3_server) as cluster:
        conn = cluster.cql.connect()
        res = conn.execute(f"SELECT * FROM {ks}.{cf};")
        have_res = {x.name: x.value for x in res}
        # Must be empty as no sstables should have been picked up
        assert not have_res, f'Sstables not cleaned, got {have_res}'
        # Make sure objects also disappeared
        objects = list_bucket(s3_server)
        print(f'Found objects: {[ objects ]}')
        for o in objects:
            for ent in sstable_entries:
                assert not o.startswith(str(ent[1])), f'Sstable object not cleaned, found {o}'

@pytest.mark.asyncio
async def test_misconfigured_storage(test_tempdir, s3_server, ssl):
    '''creating keyspace with unknown endpoint is not allowed'''
    # scylladb/scylladb#15074
    with managed_cluster(test_tempdir, ssl, s3_server) as cluster:
        print(f'Create keyspace (minio listening at {s3_server.address})')
        replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                          'replication_factor': '1'})
        storage_opts = format_tuples(type='S3',
                                     endpoint='unknown_endpoint',
                                     bucket=s3_server.bucket_name)

        conn = cluster.cql.connect()
        with pytest.raises(ConfigurationException):
            conn.execute((f"CREATE KEYSPACE test_ks WITH"
                          f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))
