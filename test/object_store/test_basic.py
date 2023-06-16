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

from contextlib import contextmanager

def get_scylla_with_s3_cmd(tempdir, ssl, s3_server):
    '''return the command args and environmental variables for running scylla'''
    scylla = run.find_scylla()
    print('Scylla under test:', scylla)
    def make_run_cmd(pid, d):
        if ssl:
            cmd, env = run.run_scylla_ssl_cql_cmd(pid, d)
        else:
            cmd, env = run.run_scylla_cmd(pid, d)

        os_conf_path = os.path.join(tempdir, 'object_storage.yaml')
        with open(os_conf_path, 'w', encoding='utf-8') as os_config_file:
            s3_endpoint = {'name': s3_server.address,
                           'port': s3_server.port}
            yaml.dump({'endpoints': [s3_endpoint]}, os_config_file)
        cmd += ['--object-storage-config-file', os_conf_path]
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
        # redirect stdout and stderr to log file
        for output in [sys.stdout, sys.stderr]:
            output.flush()
            output_fd = output.fileno()
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


@contextmanager
def managed_cluster(run_dir, ssl, s3_server):
    '''return scylla's pid, IP and a connection to it'''
    run_scylla_cmd = get_scylla_with_s3_cmd(run_dir, ssl, s3_server)
    pid = run_with_dir(run_scylla_cmd, run_dir)
    ip = run.pid_to_ip(pid)
    run.wait_for_services(pid, [check_with_cql(ip, ssl)])
    cluster = run.get_cql_cluster(ip)
    try:
        yield ip, cluster
    finally:
        cluster.shutdown()
        kill_with_dir(pid, run_dir)


@pytest.mark.asyncio
async def test_basic(test_tempdir, s3_server, ssl):
    '''verify ownership table is updated, and tables written to S3 can be read after scylla restarts'''
    ks = 'test_ks'
    cf = 'test_cf'
    rows = [('0', 'zero'),
            ('1', 'one'),
            ('2', 'two')]

    with managed_cluster(test_tempdir, ssl, s3_server) as (ip, cluster):
        print(f'Create keyspace (minio listening at {s3_server.address})')
        replication_opts = format_tuples({'class': 'NetworkTopologyStrategy',
                                          'replication_factor': '1'})
        storage_opts = format_tuples(type='S3',
                                     endpoint=s3_server.address,
                                     bucket=s3_server.bucket_name)

        conn = cluster.connect()
        conn.execute((f"CREATE KEYSPACE {ks} WITH"
                      f" REPLICATION = {replication_opts} AND STORAGE = {storage_opts};"))
        conn.execute(f"CREATE TABLE {ks}.{cf} ( name text primary key, value text );")
        for row in rows:
            cql_fmt = "INSERT INTO {}.{} ( name, value ) VALUES ('{}', '{}');"
            conn.execute(cql_fmt.format(ks, cf, *row))
        res = conn.execute(f"SELECT * FROM {ks}.{cf};")

        r = requests.post(f'http://{ip}:10000/storage_service/keyspace_flush/{ks}', timeout=60)
        if r.status_code != 200:
            print(f'Error flushing keyspace: {r}')
            success = False

        # Check that the ownership table is populated properly
        res = conn.execute("SELECT * FROM system.sstables;")
        for row in res:
            if not row.location.startswith(test_tempdir):
                print(f'Unexpected entry location in registry: {row.location}')
                success = False
            if row.status != 'sealed':
                print(f'Unexpected entry status in registry: {row.status}')
                success = False

    print('Restart scylla')
    with managed_cluster(test_tempdir, ssl, s3_server) as (ip, cluster):
        conn = cluster.connect()
        res = conn.execute(f"SELECT * FROM {ks}.{cf};")
        have_res = { x.name: x.value for x in res }
        if have_res != dict(rows):
            print(f'Unexpected table content: {have_res}')
            success = False

        print('Drop table')
        conn.execute(f"DROP TABLE {ks}.{cf};")
        # Check that the ownership table is de-populated
        res = conn.execute("SELECT * FROM system.sstables;")
        for row in res:
            print(f'Unexpected entry in registry: {row.location} {row.status}')
            success = False

    assert success
