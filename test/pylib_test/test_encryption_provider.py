#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os
import subprocess

from test.pylib.encryption_provider import KeyProvider, has_kmip_support, key_providers
from test.pylib.skip_types import skip_env

KMIP = KeyProvider.kmip.value
ALL_PROVIDERS = {kp.value for kp in KeyProvider}
ALWAYS_BUILT_IN = ALL_PROVIDERS - {KMIP}


def test_binary_lists_its_key_providers(request):
    """The real option: exits 0 without starting a server and prints one known factory name per line."""
    if request.config.getoption("--exe-path"):
        skip_env("--exe-path may point at a binary older than --list-key-providers")
    # The framework stage has no build and no mode; resolving the binary would raise there.
    if not request.getfixturevalue("build_mode"):
        skip_env("no build mode, so no scylla binary")
    scylla_binary = request.getfixturevalue("scylla_binary")
    # A configured but unbuilt mode still resolves to a path.
    if not os.path.exists(scylla_binary):
        skip_env(f"{scylla_binary} is not built")
    res = subprocess.run([scylla_binary, "--list-key-providers"], capture_output=True, text=True, timeout=120,
                         env=os.environ | {"LLVM_PROFILE_FILE": os.devnull})
    assert res.returncode == 0, res.stdout + res.stderr
    names = res.stdout.splitlines()
    assert len(names) == len(set(names))
    assert ALWAYS_BUILT_IN <= set(names) <= ALL_PROVIDERS
    assert key_providers(scylla_binary) == frozenset(names)
    assert has_kmip_support(scylla_binary) == (KMIP in names)
