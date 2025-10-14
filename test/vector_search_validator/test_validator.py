#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import subprocess

DNS_IP = "127.0.1.1"
BASE_IP = "127.0.2"

def build_resolv_conf_path(tmp_path):
    """Return the path to the custom resolv.conf file in the temporary directory."""

    return tmp_path / "resolv.conf"


def build_unshare_sh_path(tmp_path):
    """Return the path to the custom resolv.conf file in the temporary directory."""

    return tmp_path / "unshare.sh"


def build_stdout_log_path(tmp_path):
    """Return the path to the stdout.log in the temporary directory."""

    return tmp_path / "stdout.log"


def create_resolv_conf(tmp_path):
    """Create a custom resolv.conf file in the temporary directory."""

    with open(build_resolv_conf_path(tmp_path), "w") as f:
        f.write(f"nameserver {DNS_IP}\n")


def create_unshare_sh(tmp_path, validator_path, vector_store_path, scylla_path, filters):
    """Create a custom resolv.conf file in the temporary directory."""

    resolv_conf = build_resolv_conf_path(tmp_path)

    with open(build_unshare_sh_path(tmp_path), "w") as f:
        f.write(f"""
            mount --bind {resolv_conf} /etc/resolv.conf
            ip link set lo up
            ip addr add {DNS_IP}/32 dev lo
            for i in {{1..10}}; do
                ip addr add {BASE_IP}.$i/32 dev lo
            done
            {validator_path} --dns-ip {DNS_IP} --base-ip {BASE_IP}.1 --repeat-errors --verbose {scylla_path} {vector_store_path} {filters}
            """)


def test_validator(logdir_path, validator_path, vector_store_path, scylla_path, filters):
    create_resolv_conf(logdir_path)
    create_unshare_sh(logdir_path, validator_path, vector_store_path, scylla_path, filters)
    stdout_log = build_stdout_log_path(logdir_path)
    with open(stdout_log, "w") as f:
        result = subprocess.run(
                ["sudo", "unshare", "-n", "-m", "/bin/bash", build_unshare_sh_path(logdir_path)],
                text=True, stdout=f, stderr=subprocess.PIPE)
    assert result.returncode == 0, f"""
vector-search-validator tests failed:
{result.stderr}
See {stdout_log} for details.
"""

