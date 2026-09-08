#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path
from subprocess import CalledProcessError
from contextlib import ExitStack, suppress
from tempfile import TemporaryDirectory

import psutil

from test import HOST_ID, TOP_SRC_DIR
from test.pylib.host_registry import Host

LDAP_SERVER_CONFIGURATION_FILE = Path('test', 'resource', 'slapd.conf')
DEFAULT_ENTRIES = ["""dn: dc=example,dc=com
objectClass: dcObject
objectClass: organization
dc: example
o: Example
description: Example directory.
""", """dn: cn=root,dc=example,dc=com
objectClass: organizationalRole
cn: root
description: Directory manager.
""", """dn: ou=People,dc=example,dc=com
objectClass: organizationalUnit
ou: People
description: Our people.
""", """# Default superuser for Scylla
dn: uid=cassandra,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: cassandra
ou: People
sn: cassandra
userid: cassandra
userPassword: cassandra
""", """dn: uid=jsmith,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: Joe Smith
ou: People
sn: Smith
userid: jsmith
userPassword: joeisgreat
""", """dn: uid=jdoe,ou=People,dc=example,dc=com
objectClass: organizationalPerson
objectClass: uidObject
cn: John Doe
ou: People
sn: Doe
userid: jdoe
userPassword: pa55w0rd
""", """dn: cn=role1,dc=example,dc=com
objectClass: groupOfUniqueNames
objectClass: extensibleObject
cn: role1
uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
memberUid: jsmith
memberUid: cassandra
""", """dn: cn=role2,dc=example,dc=com
objectClass: groupOfUniqueNames
objectClass: extensibleObject
cn: role2
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
memberUid: cassandra
""", """dn: cn=role3,dc=example,dc=com
objectClass: groupOfUniqueNames
objectClass: extensibleObject
cn: role3
uniqueMember: uid=jdoe,ou=People,dc=example,dc=com
memberUid: jdoe
""", ]


def can_connect(address, family=socket.AF_INET):
    s = socket.socket(family)
    try:
        s.connect(address)
        return True
    except OSError as e:
        if 'AF_UNIX path too long' in str(e):
            raise OSError(e.errno, f"{str(e)} ({address})") from None
        else:
            return False
    except:
        return False


def try_something_backoff(something):
    sleep_time = 0.05
    while not something():
        if sleep_time > 30:
            return False
        time.sleep(sleep_time)
        sleep_time *= 2
    return True


def make_saslauthd_conf(ip, port, instance_path):
    """Creates saslauthd.conf with appropriate contents under instance_path.  Returns the path to the new file."""
    saslauthd_conf_path = os.path.join(instance_path, 'saslauthd.conf')
    with open(saslauthd_conf_path, 'w') as f:
        f.write(f'ldap_servers: ldap://{ip}:{port}\nldap_search_base: dc=example,dc=com')
    return saslauthd_conf_path


class ToxiproxyPortInUse(Exception):
    """The fixed toxiproxy port is held by a process that is not a leftover we can reclaim."""


def kill_leaked_toxiproxy(host: Host, tp_port: int) -> None:
    """Kills toxiproxy-servers left behind on this subnet by a previous run.

    One still holds the port and its proxies, so ours would fail to bind and 'toxiproxy-cli create'
    would fail with a confusing "proxy already exists". A run holds its subnet lock for as long as
    it runs tests, so whatever we find on our subnet belongs to a run that is dead or already
    tearing down: HostRegistry.cleanup() unlinks the lock file concurrently with the ldap
    finalizer, so the subnet can be re-leased while an old server is still going away. Either way
    it serves nobody, and reclaiming the port beats failing the whole session.
    """
    cmdline = ['toxiproxy-server', '-host', str(host), '-port', str(tp_port)]
    leaked = [proc for proc in psutil.process_iter(['cmdline']) if proc.info['cmdline'] == cmdline]
    for proc in leaked:
        logging.warning('Killing toxiproxy-server %s leaked by a previous run on %s:%s',
                        proc.pid, host, tp_port)
        with suppress(psutil.NoSuchProcess):
            proc.kill()
    psutil.wait_procs(leaked, timeout=10)


def start_ldap(host: Host, port: int, instance_root: Path, toxiproxy_byte_limit: int):
    tp_port = 8474

    if can_connect((host, tp_port)):
        kill_leaked_toxiproxy(host, tp_port)
        if can_connect((host, tp_port)):
            raise ToxiproxyPortInUse(f'{host}:{tp_port} is in use by something other than a '
                                     f'toxiproxy-server we can reclaim')

    tp_log_file = open(instance_root.parent / f'toxiproxy_server_{HOST_ID}.log', 'w')
    tp_server = subprocess.Popen(
        ['toxiproxy-server', '-host', host, '-port', str(tp_port)],
        stdout=tp_log_file,
        stderr=subprocess.STDOUT
    )

    def can_connect_to_toxiproxy():
        if tp_server.poll() is not None:
            raise Exception(f'toxiproxy-server exited with {tp_server.returncode}, see {tp_log_file.name}')
        return can_connect((host, tp_port))

    def stop_toxiproxy():
        tp_server.terminate()
        tp_server.wait()
        tp_log_file.close()

    # Anything failing below must not leave the server behind for the next run to trip over.
    with ExitStack() as stack:
        stack.callback(stop_toxiproxy)
        if not try_something_backoff(can_connect_to_toxiproxy):
            raise Exception('Could not connect to toxiproxy')

        instance_path = instance_root / str(port)
        slapd_pid_file = instance_path / 'slapd.pid'
        saslauthd_socket_path = TemporaryDirectory()
        stack.callback(saslauthd_socket_path.cleanup)
        os.makedirs(instance_path)
        stack.callback(shutil.rmtree, instance_path)
        # This will always fail because it lacks the permissions to read the default slapd data
        # folder but it does create the instance folder so we don't want to fail here.
        subprocess.run(['slaptest', '-f', TOP_SRC_DIR / LDAP_SERVER_CONFIGURATION_FILE, '-F', instance_path],
                       check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Set up failure injection.
        try:
            proxy_name = f'p{port}'
            subprocess.check_output(
                ['toxiproxy-cli', '--host', f'{host}:{tp_port}', 'create', '--listen', f'{host}:{port + 2}', '--upstream',
                 f'{host}:{port}', proxy_name], stderr=subprocess.STDOUT)
            subprocess.check_output(
                ['toxiproxy-cli', '--host', f'{host}:{tp_port}', 'toxic', 'add', '-t', 'limit_data', '-n', 'limiter', '-a',
                 f'bytes={toxiproxy_byte_limit}', proxy_name], stderr=subprocess.STDOUT)
            # Change the data folder in the default config.
            replace_expression = f"s/olcDbDirectory:.*/olcDbDirectory: {str(instance_path).replace('/', r'\/')}/g"
            subprocess.check_output(
                ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'],
                stderr=subprocess.STDOUT
            )
            # Change the pid file to be kept with the instance.
            replace_expression = f"s/olcPidFile:.*/olcPidFile: {str(slapd_pid_file).replace('/', r'\/')}/g"
            subprocess.check_output(
                ['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'],
                stderr=subprocess.STDOUT
            )
            # Put the test data in.
            cmd = ['slapadd', '-F', instance_path]
            subprocess.check_output(cmd, input='\n\n'.join(DEFAULT_ENTRIES).encode('ascii'), stderr=subprocess.STDOUT)
        except CalledProcessError as e:
            logging.critical("toxiproxy-cli failed: %s: %s", e, e.stdout)
            raise 
        # Set up the server.
        SLAPD_URLS = f'ldap://:{port}/ ldaps://:{port + 1}/'

        def can_connect_to_slapd():
            return can_connect((host, port)) and can_connect((host, port + 1)) and can_connect(
                (host, port + 2))

        def can_connect_to_saslauthd():
            return can_connect(os.path.join(saslauthd_socket_path.name, 'mux'), socket.AF_UNIX)

        slapd_proc = subprocess.Popen(['prlimit', '-n1024', 'slapd', '-F', instance_path, '-h', SLAPD_URLS, '-d', '0'])

        def stop_slapd():
            slapd_proc.terminate()
            slapd_proc.wait()  # Wait for slapd to remove slapd.pid, so it doesn't race with rmtree.

        stack.callback(stop_slapd)
        saslauthd_conf_path = make_saslauthd_conf(host, port, instance_path)
        os.environ.update(SEASTAR_LDAP_PORT=str(port), SEASTAR_LDAP_HOST=str(host),
                          SASLAUTHD_MUX_PATH=os.path.join(saslauthd_socket_path.name, 'mux'))

        saslauthd_proc = subprocess.Popen(
            ['saslauthd', '-d', '-n', '1', '-a', 'ldap', '-O', saslauthd_conf_path, '-m', saslauthd_socket_path.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        stack.callback(saslauthd_proc.kill)  # Somehow, invoking terminate() here also terminates toxiproxy-server. o_O

        if not try_something_backoff(can_connect_to_slapd):
            raise Exception('Unable to connect to slapd')
        if not try_something_backoff(can_connect_to_saslauthd):
            raise Exception('Unable to connect to saslauthd')
        return stack.pop_all().close  # started: ownership passes to the caller
