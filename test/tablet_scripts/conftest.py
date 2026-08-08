#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import importlib.util
import sys
from types import ModuleType

from test import TOP_SRC_DIR


SCRIPTS_DIR = str(TOP_SRC_DIR / "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)


def load_tablet_script(module_name: str) -> ModuleType:
    script_path = TOP_SRC_DIR / "scripts" / "tablets" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(f"tablet_scripts_{module_name.replace('-', '_')}", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load tablet script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
