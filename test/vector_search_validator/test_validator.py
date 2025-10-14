#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
from pathlib import Path
import pytest
import subprocess
from test import BUILD_DIR

# IP address for custom DNS server inside linux namespace
DNS_IP = "127.0.1.1"
# 3 tuple IP address as a base for scylla or vector-store nodes
BASE_IP = "127.0.2"

VALIDATOR_PATH = BUILD_DIR / "vector-search-validator/bin/vector-search-validator"
VECTOR_STORE_PATH = BUILD_DIR / "vector-search-validator/bin/vector-store"


def create_resolv_conf(tmp_path: Path) -> Path:
    """Create a custom resolv.conf file in the temporary directory."""

    path = tmp_path / "resolv.conf"

    with path.open("w") as f:
        f.write(f"nameserver {DNS_IP}\n")

    return path


def create_unshare_sh(tmp_path: Path, resolv_conf_path: Path, scylla_path: str, filters: str) -> Path:
    """Create a custom resolv.conf file in the temporary directory."""

    path = tmp_path / "unshare.sh"

    with path.open("w") as f:
        f.write(f"""
mount --bind {resolv_conf_path} /etc/resolv.conf
ip link set lo up
ip addr add {DNS_IP}/32 dev lo
for i in {{1..10}}; do
    ip addr add {BASE_IP}.$i/32 dev lo
done
{VALIDATOR_PATH} run --dns-ip {DNS_IP} --base-ip {BASE_IP}.1 --duplicate-errors --verbose --disable-colors {scylla_path} {VECTOR_STORE_PATH} {filters}
""")

    return path


def list_test_case_names() -> list[str]:
    result = subprocess.run([VALIDATOR_PATH, "list"], text=True, capture_output=True, check=True)
    return sorted(list(set(map(lambda name: name.split("::", 1)[0], result.stdout.splitlines()))))


@pytest.mark.skipif(os.getenv("DBUILD_TOOLCHAIN") != "1", reason="Requires running test inside dbuild toolchain container")
@pytest.mark.parametrize("test_case_name", list_test_case_names())
def test_validator(logdir_path: Path, run_id: int, scylla_path: str, test_case_name: str, filters: str):
    logdir_path = logdir_path / f"{test_case_name}-{run_id}"
    logdir_path.mkdir(parents=True, exist_ok=True)
    resolve_conf_path = create_resolv_conf(logdir_path)
    unshare_sh_path = create_unshare_sh(
            logdir_path,
            resolve_conf_path,
            scylla_path,
            filters if filters else test_case_name + "::"
            )

    stdout_log = logdir_path / "stdout.log"

    with open(stdout_log, "w") as f:
        result = subprocess.run(
                ["sudo", "unshare", "-n", "-m", "/bin/bash", unshare_sh_path],
                text=True, stdout=f, stderr=subprocess.PIPE)

    assert result.returncode == 0, f"""
vector-search-validator tests failed:
{result.stderr}
See {stdout_log} for details.
"""

