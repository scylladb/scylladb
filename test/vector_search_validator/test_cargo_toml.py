#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import subprocess

def test_cargo_toml(dir_path):
    with open(f"{dir_path}/Cargo.toml", "r") as f:
        cargo_toml = f.read()
    result = subprocess.run([f"{dir_path}/cargo-toml-template"], text=True, capture_output=True, check=True)
    assert result.stdout == cargo_toml, "Cargo.toml does not match the template output"

