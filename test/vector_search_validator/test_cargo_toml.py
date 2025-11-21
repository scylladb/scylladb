#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import subprocess
from test import TEST_DIR

def test_cargo_toml():
    dir_path = TEST_DIR / "vector_search_validator"
    cargo_toml_path = dir_path / "Cargo.toml"

    with cargo_toml_path.open("r") as f:
        cargo_toml = f.read()

    result = subprocess.run([f"{dir_path}/cargo-toml-template"], text=True, capture_output=True, check=True)

    assert result.stdout == cargo_toml, "Cargo.toml does not match the template output"
