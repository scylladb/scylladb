#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import errno
import functools
import hashlib
import logging
import os
import random
import re
import string
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any
from concurrent.futures import ThreadPoolExecutor

from test.cluster.dtest.tools.context import disable_autocompaction

if TYPE_CHECKING:
    from collections.abc import Callable

    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)
lock = threading.Lock()

colors = {
    "yellow": "\033[93m",
    "reset": "\033[0m",
}


def retry_till_success[T, **P](fun: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    timeout = kwargs.pop("timeout", 60)
    bypassed_exception = kwargs.pop("bypassed_exception", Exception)
    should_retry = kwargs.pop("should_retry", None)

    deadline = time.perf_counter() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception as e:
            if (should_retry and not should_retry(e)) or time.perf_counter() > deadline:
                raise

        # Brief pause before next attempt.
        time.sleep(0.1)


def list_to_hashed_dict(query_response_list: list) -> dict:
    """
    takes a list and hashes the contents and puts them into a dict so the contents can be compared
    without order. unfortunately, we need to do a little massaging of our input; the result from
    the driver can return a OrderedMapSerializedKey (e.g. [0, 9, OrderedMapSerializedKey([(10, 11)])])
    but our "expected" list is simply a list of elements (or list of list). this means if we
    hash the values as is we'll get different results. to avoid this, when we see a dict,
    convert the raw values (key, value) into a list and insert that list into a new list
    :param query_response_list the list to convert
    :return: dict containing the contents fo the list with the hashed contents
    """
    hashed_dict = dict()
    for item_lst in query_response_list:
        normalized_list = []
        for item in item_lst:
            if hasattr(item, "items"):
                tmp_list = []
                for a, b in item.items():
                    tmp_list.append(a)
                    tmp_list.append(b)
                normalized_list.append(tmp_list)
            else:
                normalized_list.append(item)
        list_str = str(normalized_list)
        utf8 = list_str.encode("utf-8", "ignore")
        list_digest = hashlib.sha256(utf8).hexdigest()
        hashed_dict[list_digest] = normalized_list
    return hashed_dict


def num_tokens_per_node(session) -> int:
    """How many vnode tokens each node of this cluster actually has.

    scylla-dtest could assume 256: nothing there overrode the num_tokens that
    the setup writes into scylla.yaml.  In this tree every node is started with
    `--num-tokens 16` on the command line (see ccmlib/scylla_node.py, added by
    9280a039ee so that a 768-range bootstrap does not run out of file
    descriptors), and a command line option wins over scylla.yaml.  Ask the
    server what it ended up with rather than hard-coding upstream's number.
    """
    return int(session.execute("SELECT value FROM system.config WHERE name = 'num_tokens'").one().value)


def set_trace_probability(nodes: list[ScyllaNode], probability_value: float) -> None:
    def _set_trace_probability_for_node(_node: ScyllaNode) -> None:
        logger.debug(f'{"Enable" if probability_value else "Disable"} trace for {_node.name} with {probability_value=}')
        _node.cluster.manager.api.set_trace_probability(node_ip=_node.address(), probability=probability_value)

    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        threads = [executor.submit(_set_trace_probability_for_node, node) for node in nodes]
        [thread.result() for thread in threads]


class ImmutableMapping(Mapping):
    """
    Convenience class for when you want an immutable-ish map.

    Useful at class level to prevent mutability problems (such as a method altering the class level mutable)
    """

    def __init__(self, init_dict):
        self._data = init_dict.copy()

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._data})"


# NOTE: the functions below are restored verbatim (imports aside) from
# scylla-dtest's tools/misc.py; they were trimmed when this module was first
# ported in-tree, but not-yet-adapted dtest/unported test modules still import
# them.


def log_subprocess_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except subprocess.CalledProcessError as exp:
            logger.error(str(exp))
            logger.error("stdout:\n%s", exp.stdout)
            logger.error("stderr:\n%s", exp.stderr)
            raise

    return wrapper


@log_subprocess_exceptions
def generate_ssl_stores(base_dir, passphrase="cassandra", ip_addresses: list[str] | None = None, dns_names: list[str] | None = None):
    """
    Util for generating ssl stores using java keytool -- nondestructive method if stores already exist this method is
    a no-op.

    @param base_dir (str) directory where keystore.jks, truststore.jks and ccm_node.cer will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can be
            overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool fails during any step
    """

    if os.path.exists(os.path.join(base_dir, "keystore.jks")):
        logger.debug("keystores already exists - skipping generation of ssl keystores")
        return

    legacy = ["-legacy"] if "-legacy" in subprocess.run(["openssl", "pkcs12", "--help"], text=True, stderr=subprocess.PIPE, check=False).stderr else []
    ext = []
    ext_list = []
    if dns_names:
        ext_list += [f"dns:{name}" for name in dns_names]
    if ip_addresses:
        ext_list += [f"ip:{ip}" for ip in ip_addresses]
    if ext_list:
        ext = ["-ext", f"san={','.join(ext_list)}"]

    logger.debug(f"generating keystore.jks in [{base_dir}]")
    subprocess.check_output(
        [
            "keytool",
            "-genkeypair",
            "-alias",
            "ccm_node",
            "-keyalg",
            "RSA",
            "-validity",
            "365",
            "-keystore",
            os.path.join(base_dir, "keystore.jks"),
            "-storepass",
            passphrase,
            "-dname",
            "cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US",
            "-keypass",
            passphrase,
            *ext,
        ]
    )
    logger.debug(f"exporting cert from keystore.jks in [{base_dir}]")
    subprocess.check_output(["keytool", "-export", "-rfc", "-alias", "ccm_node", "-keystore", os.path.join(base_dir, "keystore.jks"), "-file", os.path.join(base_dir, "ccm_node.cer"), "-storepass", passphrase])
    logger.debug(f"importing cert into truststore.jks in [{base_dir}]")
    subprocess.check_output(["keytool", "-import", "-file", os.path.join(base_dir, "ccm_node.cer"), "-alias", "ccm_node", "-keystore", os.path.join(base_dir, "truststore.jks"), "-storepass", passphrase, "-noprompt"])
    # Added for scylla: Generate pem format cert/key
    logger.debug(f"exporting cert to pks12 from keystore.jks in [{base_dir}]")
    subprocess.check_output(
        [
            "keytool",
            "-importkeystore",
            "-srckeystore",
            os.path.join(base_dir, "keystore.jks"),
            "-srcstorepass",
            passphrase,
            "-srckeypass",
            passphrase,
            "-destkeystore",
            os.path.join(base_dir, "ccm_node.p12"),
            "-deststoretype",
            "PKCS12",
            "-srcalias",
            "ccm_node",
            "-deststorepass",
            passphrase,
            "-destkeypass",
            passphrase,
        ]
    )
    logger.debug(f"Using openssl to split pks12 in [{base_dir}] to pem format")
    subprocess.check_output(["openssl", "pkcs12", "-in", os.path.join(base_dir, "ccm_node.p12"), "-passin", f"pass:{passphrase}", "-nokeys", "-out", os.path.join(base_dir, "ccm_node.pem"), *legacy])
    # Key with password. We want without...
    subprocess.check_output(
        ["openssl", "pkcs12", "-in", os.path.join(base_dir, "ccm_node.p12"), "-passin", f"pass:{passphrase}", "-passout", f"pass:{passphrase}", "-nocerts", "-out", os.path.join(base_dir, "ccm_node.tmp"), *legacy],
    )
    subprocess.check_output(["openssl", "rsa", "-in", os.path.join(base_dir, "ccm_node.tmp"), "-passin", f"pass:{passphrase}", "-out", os.path.join(base_dir, "ccm_node.key")])
    # And create the trust chain
    logger.debug(f"exporting cert to pks12 from truststore.jks in [{base_dir}]")
    subprocess.check_output(
        [
            "keytool",
            "-importkeystore",
            "-srckeystore",
            os.path.join(base_dir, "truststore.jks"),
            "-srcstorepass",
            passphrase,
            "-destkeystore",
            os.path.join(base_dir, "trust.p12"),
            "-deststoretype",
            "PKCS12",
            "-srcalias",
            "ccm_node",
            "-deststorepass",
            passphrase,
        ]
    )
    subprocess.check_output(["openssl", "pkcs12", "-in", os.path.join(base_dir, "trust.p12"), "-passin", f"pass:{passphrase}", "-out", os.path.join(base_dir, "trust.pem"), *legacy])
    # generate a revokation list (crl) for the same cert
    index_txt = os.path.join(base_dir, "index.txt")
    pulp_crl_number = os.path.join(base_dir, "pulp_crl_number")
    openssl_ca_conf = os.path.join(base_dir, "openssl_ca.conf")
    with open(index_txt, "w") as f:
        pass
    with open(pulp_crl_number, "w") as f:
        f.write("00")
    with open(openssl_ca_conf, "w") as conf_file:
        conf_file.write(
            f"""
# OpenSSL configuration for CRL generation
#
####################################################################
[ ca ]
default_ca     = CA_default            # The default ca section

####################################################################
[ CA_default ]
database = {index_txt}
crlnumber = {pulp_crl_number}

default_days   = 365                   # how long to certify for
default_crl_days= 30                   # how long before next CRL
default_md     = default               # use public key default MD
preserve       = no                    # keep passed DN ordering

####################################################################
[ crl_ext ]
# CRL extensions.
# Only issuerAltName and authorityKeyIdentifier make any sense in a CRL.
# issuerAltName=issuer:copy
authorityKeyIdentifier=keyid:always,issuer:always
        """
        )
    crl_file = os.path.join(base_dir, "ccm_node.crl")
    subprocess.check_output(["openssl", "ca", "-gencrl", "-cert", os.path.join(base_dir, "ccm_node.pem"), "-keyfile", os.path.join(base_dir, "ccm_node.key"), "-out", crl_file, "-config", openssl_ca_conf])

    logger.debug(f"removing temporary certificates in [{base_dir}]")
    for filename in ("ccm_node.p12", "ccm_node.tmp", "trust.p12", "index.txt.attr", "index.txt.old", "pulp_crl_number.old"):
        try:
            os.remove(os.path.join(base_dir, filename))
        except OSError as e:
            if e.errno != errno.ENOENT:  # ENOENT = no such file or directory
                raise


@log_subprocess_exceptions
def revoke_certificate(base_dir):
    crl_file = os.path.join(base_dir, "ccm_node.crl")
    openssl_ca_conf = os.path.join(base_dir, "openssl_ca.conf")
    subprocess.check_output(["openssl", "ca", "-revoke", os.path.join(base_dir, "ccm_node.pem"), "-cert", os.path.join(base_dir, "ccm_node.pem"), "-keyfile", os.path.join(base_dir, "ccm_node.key"), "-config", openssl_ca_conf])
    subprocess.check_output(["openssl", "ca", "-gencrl", "-cert", os.path.join(base_dir, "ccm_node.pem"), "-keyfile", os.path.join(base_dir, "ccm_node.key"), "-out", crl_file, "-config", openssl_ca_conf])


def is_port_used(port: int, service_name: str) -> bool:
    """
    Path to `ss' is /usr/sbin/ss for RHEL-like distros and /bin/ss for Debian-based.  Unfortunately,
    /usr/sbin is not always in $PATH, so need to set it explicitly.

    Output of `ss -ln' command in case of used port:
      $ ss -ln '( sport = :8000 )'
      Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port

    And if there are no processes listening on the port:
      $ ss -ln '( sport = :8001 )'
      Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port

    Can't avoid the header by using `-H' option because of ss' core on Ubuntu 18.04.
    """
    try:
        cmd = f"PATH=/bin:/usr/sbin ss -ln"
        out = f"{subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False).stdout}"
        cmd = f"PATH=/bin:/usr/sbin ss -ln '( sport = :{port} )'"
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False).stdout.splitlines()
        if len(res) <= 1:
            logger.debug(f"Checking for '{service_name}' on port {port} not found:\n{out}")
        return len(res) > 1
    except Exception as details:  # noqa: BLE001
        logger.debug(f"Error checking for '{service_name}' on port {port}: {details}")
        return False


def get_current_test_name():
    """
    See https://docs.pytest.org/en/latest/example/simple.html#pytest-current-test-environment-variable
    :return: returns just the name of the current running test name
    """
    pytest_current_test = os.environ.get("PYTEST_CURRENT_TEST")
    test_splits = pytest_current_test.split("::")
    current_test_name = test_splits[len(test_splits) - 1]
    current_test_name = current_test_name.replace(" (call)", "")
    current_test_name = current_test_name.replace(" (setup)", "")
    current_test_name = current_test_name.replace(" (teardown)", "")
    return current_test_name


def safe_mkdtemp():
    lock.acquire()
    tmpdir = tempfile.mkdtemp()
    # \ on Windows is interpreted as an escape character and doesn't do anyone any favors
    lock.release()
    return tmpdir.replace("\\", "/")


def generate_random_text(length=10):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def flush_by_node(cluster):
    for node in cluster.nodelist():
        node.flush()


def remove_node(cluster, node, wait_other_notice=True, other_nodes=None, gently=False):
    hostid = node.hostid()
    logger.debug(f"Stopping node {node.name} (hostid {hostid}) gently={gently}")
    node.stop(gently=gently, wait_other_notice=True)
    logger.debug(f"Remove node {node.name} (hostid {hostid})")
    cluster.remove(node, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
    remove_using_node = cluster.nodelist()[0]
    remove_using_node.nodetool(f"removenode {hostid}")


def get_free_memory_size_in_mb():
    """
    Get current free memory from /proc/meminfo
    """
    proc = subprocess.Popen(["cat", "/proc/meminfo"], stdout=subprocess.PIPE)
    out, err = proc.communicate()
    out = out.decode()
    assert proc.returncode == 0 and "MemFree:" in out, err
    pattern = re.compile("MemFree: (.*) ")
    for line in out.split("\n"):
        if pattern.match(line):
            return int(pattern.match(line)[1]) / 1024  # unit: mb
    raise Exception("Failed to get the valid free memory size")


def seconds_to_micros(seconds):
    return seconds * 1000 * 1000


def micros_to_seconds(micros):
    return micros // (1000 * 1000)


def get_manager_version(install_dir):
    return subprocess.run([Path(install_dir) / "sctool", "version"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False).stdout.split(":")[-1].strip()


def dump_sstables(node: ScyllaNode, keyspace: str, table: str, datafiles: list[str] | None = None) -> list[dict[str, Any]]:
    with disable_autocompaction(node, "system_schema"), disable_autocompaction(node, keyspace, table):
        return node.dump_sstables(keyspace, table, datafiles)


def is_coverage(cassandra_dir):
    command = ["bash", "-c", f"strings {cassandra_dir}/libexec/scylla | grep -i GCOV_"]
    res = subprocess.run(command, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, check=False)
    return res.returncode == 0


def colored_text(text: str, color: str) -> str:
    if sys.stdout.isatty():  # Check if the output is a terminal
        return f"{colors[color]}{text}{colors['reset']}"
    return text
