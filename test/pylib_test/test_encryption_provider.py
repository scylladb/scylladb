#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os
import subprocess

import pytest

from test.pylib.encryption_provider import (
    REQUIRE_KMIP_ENV, KeyProvider, has_kmip_support, key_providers, skip_or_fail_without_kmip)
from test.pylib.skip_types import skip_env

KMIP = KeyProvider.kmip.value
ALL_PROVIDERS = {kp.value for kp in KeyProvider}
ALWAYS_BUILT_IN = ALL_PROVIDERS - {KMIP}


@pytest.fixture
def fake_scylla(tmp_path):
    """Build a stand-in `scylla` whose --list-key-providers behaves as the given shell body."""
    key_providers.cache_clear()

    def make(body: str):
        exe = tmp_path / f"scylla{len(list(tmp_path.iterdir()))}"
        exe.write_text("#!/bin/sh\n" + body)
        exe.chmod(0o755)
        return exe

    yield make
    key_providers.cache_clear()


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
    res = subprocess.run([scylla_binary, "--list-key-providers"], capture_output=True, text=True, timeout=30)
    assert res.returncode == 0, res.stdout + res.stderr
    names = res.stdout.splitlines()
    assert len(names) == len(set(names))
    assert ALWAYS_BUILT_IN <= set(names) <= ALL_PROVIDERS
    assert key_providers(scylla_binary) == frozenset(names)
    assert has_kmip_support(scylla_binary) == (KMIP in names)


@pytest.mark.parametrize("providers, kmip", [
    (sorted(ALWAYS_BUILT_IN | {KMIP}), True),
    (sorted(ALWAYS_BUILT_IN), False),
])
def test_kmip_support_follows_listing(fake_scylla, providers, kmip):
    exe = fake_scylla("printf '%s\\n' " + " ".join(providers))
    assert key_providers(exe) == frozenset(providers)
    assert has_kmip_support(exe) is kmip


@pytest.mark.parametrize("body", [
    "echo 'error: unrecognised option' >&2; exit 2",  # older binary rejecting the option
    "",
])
def test_older_binary_is_assumed_to_have_kmip(fake_scylla, body):
    exe = fake_scylla(body)
    assert key_providers(exe) is None
    assert has_kmip_support(exe)


@pytest.mark.parametrize("required, outcome", [(None, pytest.skip.Exception), ("1", RuntimeError)])
def test_missing_kmip_skips_or_fails(fake_scylla, monkeypatch, required, outcome):
    monkeypatch.delenv(REQUIRE_KMIP_ENV, raising=False)
    if required:
        monkeypatch.setenv(REQUIRE_KMIP_ENV, required)
    with pytest.raises(outcome, match=REQUIRE_KMIP_ENV):
        skip_or_fail_without_kmip(fake_scylla("echo " + KeyProvider.local.value))


def test_kmip_present_neither_skips_nor_fails(fake_scylla, monkeypatch):
    monkeypatch.setenv(REQUIRE_KMIP_ENV, "1")
    skip_or_fail_without_kmip(fake_scylla("echo " + KMIP))
