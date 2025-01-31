#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Any, Generator

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
cn: role1
uniqueMember: uid=jsmith,ou=People,dc=example,dc=com
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""", """dn: cn=role2,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role2
uniqueMember: uid=cassandra,ou=People,dc=example,dc=com
""", """dn: cn=role3,dc=example,dc=com
objectClass: groupOfUniqueNames
cn: role3
uniqueMember: uid=jdoe,ou=People,dc=example,dc=com
""", ]


class PrepareChildProcessEnv:
    """
    Class responsible to get environment variables from the main thread through the shared file and set them for the process
    """

    def __init__(self, root_dir, temp_dir: Path, modes: list[str], env_file: Path, byte_limit: int, worker_id):
        self.id = int(worker_id[2:]) + 1
        self.temp_dir = temp_dir
        self.modes = modes
        self.root_dir = root_dir
        self.byte_limit = byte_limit
        self.env_file = env_file
        self.finalize = None

    def prepare(self) -> None:
        """
        Setup LDAP proxy and set environment variables
        """
        ldap_port = 5000 + (self.id * 3) % 55000

        timeout = 10
        sleep_for = 0.01
        start_time = time.time()
        while True:
            if os.path.exists(self.env_file):
                (self.finalize, _, test_env) = setup(self.root_dir, ldap_port, self.temp_dir / 'ldap_instances',
                                                     self.byte_limit)

                for key, value in test_env.items():
                    os.environ[key] = value
                break

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for file {self.env_file}")
            # Sleep needed to wait when the controller will create a file with environment variables.
            # Without sleep checking of the file existence will be too fast,
            # so it will finish before the file is created
            time.sleep(sleep_for)
            sleep_for *= 2


    def cleanup(self) -> None:
        """
        Stop LDAP
        """
        if self.finalize:
            self.finalize()

    def __enter__(self):
        try:
            self.prepare()
        except Exception:
            self.cleanup()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


class PrepareMainProcessEnv:
    """
    A class responsible for starting additional services needed by tests.

    It starts up a Minio server and an S3 mock server.
    The environment settings are saved to a file for later consumption by child processes.
    """

    def __init__(self, root_dir, temp_dir: Path, modes: list[str], env_file: Path, byte_limit: int):
        self.temp_dir = temp_dir
        self.modes = modes
        self.root_dir = root_dir
        pytest_dirs = [self.temp_dir / mode / 'pytest' for mode in modes]
        for directory in [self.temp_dir, *pytest_dirs]:
            if not directory.exists():
                os.makedirs(directory, exist_ok=True)
        self.env_file = env_file
        self.tp_server = subprocess.Popen('toxiproxy-server', stderr=subprocess.DEVNULL)

        def can_connect_to_toxiproxy():
            return can_connect(('127.0.0.1', 8474))

        if not try_something_backoff(can_connect_to_toxiproxy):
            raise Exception('Could not connect to toxiproxy')
        self.ldap_port = 5000
        self.byte_limit = byte_limit
        self.finalize = None

    def prepare(self) -> None:
        """
        Start the LDAP.
        Create a file with environment variables for connecting to them.
        """
        (self.finalize, _, test_env) = setup(self.root_dir, self.ldap_port, self.temp_dir / 'ldap_instances',
                                             self.byte_limit)

        for key, value in test_env.items():
            os.environ[key] = value

        with open(self.env_file, 'w') as file:
            for key, value in test_env.items():
                file.write(f"{key}={value}\n")

    def cleanup(self) -> None:
        """
        Stop LDAP.
        Remove the file with environment variables to not mess for consecutive runs.
        """
        if os.path.exists(self.env_file):
            self.env_file.unlink()
        if self.finalize:
            self.finalize()
        if self.tp_server is not None:
            self.tp_server.terminate()

    def __enter__(self):
        try:
            self.prepare()
        except Exception:
            self.cleanup()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


@contextmanager
def get_env_manager(root_dir: Path, temp_dir: Path, worker_id: str, modes: list[str],
                    byte_limit: int) -> Generator[None, Any, None]:
    """
    xdist helps to execute test in parallel.
    For that purpose it creates one main controller and workers.
    Pytest itself doesn't know if it's a worker or controller, so it will execute all fixtures and methods.
    Tests need S3 mock server and minio to start only once for the whole run, since they can share the one instance and
    share the environment variables with workers.

    So the part of starting the servers executes on non-workers' machines.
    That means when xdist isn't used, servers start as intended in the main process.
    Tests on workers should know the endpoints of the servers, so the controller prepares this information.
    According classes responsible for configuration controller and workers.
    """
    env_file = Path(f"{temp_dir}/test_env").absolute()
    if worker_id != 'master':
        with PrepareChildProcessEnv(root_dir, temp_dir, modes, env_file, byte_limit, worker_id):
            yield
    else:
        with PrepareMainProcessEnv(root_dir, temp_dir, modes, env_file, byte_limit):
            yield


def can_connect(address, family=socket.AF_INET):
    s = socket.socket(family)
    try:
        s.connect(address)
        return True
    except OSError as e:
        if 'AF_UNIX path too long' in str(e):
            raise OSError(e.errno, "{} ({})".format(str(e), address)) from None
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


def make_saslauthd_conf(port, instance_path):
    """Creates saslauthd.conf with appropriate contents under instance_path.  Returns the path to the new file."""
    saslauthd_conf_path = os.path.join(instance_path, 'saslauthd.conf')
    with open(saslauthd_conf_path, 'w') as f:
        f.write('ldap_servers: ldap://localhost:{}\nldap_search_base: dc=example,dc=com'.format(port))
    return saslauthd_conf_path


def setup(project_root: Path, port: int, instance_root: Path, byte_limit: int):
    instance_path = instance_root / str(port)
    slapd_pid_file = instance_path / 'slapd.pid'
    saslauthd_socket_path = TemporaryDirectory()
    os.makedirs(instance_path, exist_ok=True)
    # This will always fail because it lacks the permissions to read the default slapd data
    # folder but it does create the instance folder so we don't want to fail here.
    try:
        subprocess.check_output(['slaptest', '-f', project_root / LDAP_SERVER_CONFIGURATION_FILE, '-F', instance_path],
                                stderr=subprocess.DEVNULL)
    except:
        pass
    # Set up failure injection.
    proxy_name = 'p{}'.format(port)
    subprocess.check_output(
        ['toxiproxy-cli', 'c', proxy_name, '--listen', 'localhost:{}'.format(port + 2), '--upstream',
            'localhost:{}'.format(port)])
    subprocess.check_output(['toxiproxy-cli', 't', 'a', proxy_name, '-t', 'limit_data', '-n', 'limiter', '-a',
                             'bytes={}'.format(byte_limit)])
    # Change the data folder in the default config.
    replace_expression = 's/olcDbDirectory:.*/olcDbDirectory: {}/g'.format(str(instance_path).replace('/', r'\/'))
    subprocess.check_output(['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
    # Change the pid file to be kept with the instance.
    replace_expression = 's/olcPidFile:.*/olcPidFile: {}/g'.format(str(slapd_pid_file).replace('/', r'\/'))
    subprocess.check_output(['find', instance_path, '-type', 'f', '-exec', 'sed', '-i', replace_expression, '{}', ';'])
    # Put the test data in.
    cmd = ['slapadd', '-F', instance_path]
    subprocess.check_output(cmd, input='\n\n'.join(DEFAULT_ENTRIES).encode('ascii'), stderr=subprocess.STDOUT)
    # Set up the server.
    SLAPD_URLS = 'ldap://:{}/ ldaps://:{}/'.format(port, port + 1)

    def can_connect_to_slapd():
        return can_connect(('127.0.0.1', port)) and can_connect(('127.0.0.1', port + 1)) and can_connect(
            ('127.0.0.1', port + 2))

    def can_connect_to_saslauthd():
        return can_connect(os.path.join(saslauthd_socket_path.name, 'mux'), socket.AF_UNIX)

    slapd_proc = subprocess.Popen(['prlimit', '-n1024', 'slapd', '-F', instance_path, '-h', SLAPD_URLS, '-d', '0'])
    saslauthd_conf_path = make_saslauthd_conf(port, instance_path)
    test_env = {"SEASTAR_LDAP_PORT": str(port), "SASLAUTHD_MUX_PATH": os.path.join(saslauthd_socket_path.name, "mux")}

    saslauthd_proc = subprocess.Popen(
        ['saslauthd', '-d', '-n', '1', '-a', 'ldap', '-O', saslauthd_conf_path, '-m', saslauthd_socket_path.name],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    def finalize():
        slapd_proc.terminate()
        slapd_proc.wait()  # Wait for slapd to remove slapd.pid, so it doesn't race with rmtree below.
        saslauthd_proc.kill()  # Somehow, invoking terminate() here also terminates toxiproxy-server. o_O
        shutil.rmtree(instance_path)
        saslauthd_socket_path.cleanup()
        subprocess.check_output(['toxiproxy-cli', 'd', proxy_name])

    try:
        if not try_something_backoff(can_connect_to_slapd):
            raise Exception('Unable to connect to slapd')
        if not try_something_backoff(can_connect_to_saslauthd):
            raise Exception('Unable to connect to saslauthd')
    except:
        finalize()
        raise
    return finalize, '--byte-limit={}'.format(byte_limit), test_env
