#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ruamel.yaml import YAML

if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger("ccm")


BIN_DIR = "bin"
SCYLLA_CONF_DIR = "conf"
SCYLLA_CONF = "scylla.yaml"


class CCMError(Exception):
    ...


class ArgumentError(CCMError):
    ...


def wait_for(func: Callable, timeout: int, first: float = 0.0, step: float = 1.0) -> bool:
    """Wait until func() evaluates to True.

    If func() evaluates to True before timeout expires, return True.  Otherwise, return False.
    """
    deadline = time.perf_counter() + timeout

    time.sleep(first)

    while time.perf_counter() < deadline:
        if func():
            return True
        time.sleep(step)

    return False


# NOTE: the functions below are restored verbatim (imports aside) from ccm's
# ccmlib/common.py, only because a handful of not-yet-adapted dtest/unported
# test modules import them at module load time. They are not otherwise used
# by the in-tree port.


def is_win():
    return sys.platform in ("cygwin", "win32")


def platform_binary(input):
    return input + ".bat" if is_win() else input


def join_bin(root, dir, executable):
    return os.path.join(root, dir, platform_binary(executable))


def isScylla(install_dir):
    if install_dir is None:
        scylla_version = os.environ.get('SCYLLA_VERSION', None)
        if scylla_version:
            from test.cluster.dtest.ccmlib.scylla_repository import setup
            cdir, _ = setup(scylla_version)
            return cdir is not None

        scylla_docker_image = os.environ.get('SCYLLA_DOCKER_IMAGE', None)
        if scylla_docker_image:
            return True

        raise ArgumentError('Undefined installation directory')

    if os.path.exists(os.path.join(install_dir, 'scylla')):
        return True

    scylla_build_modes = ['debug', 'dev', 'release']
    cmake_build_types = ['Debug', 'Dev', 'RelWithDebInfo']
    for mode in scylla_build_modes + cmake_build_types:
        if os.path.exists(os.path.join(install_dir, 'build', mode, 'scylla')):
            return True

    return os.path.exists(os.path.join(install_dir, 'bin', 'scylla'))


def get_dse_version(install_dir):
    for root, dirs, files in os.walk(install_dir):
        for file in files:
            match = re.search(r'^dse(?:-core)?-([0-9.]+)(?:-SNAPSHOT)?\.jar', file)
            if match:
                return match.group(1)
    return None


def get_install_dir_from_cluster_conf(node_path):
    file = os.path.join(os.path.dirname(node_path), "cluster.conf")
    with open(file) as f:
        for line in f:
            match = re.search('install_dir: (.*?)$', line)
            if match:
                return match.group(1)
    return None


def get_default_scylla_yaml(install_dir):
    scylla_yaml_path = Path(install_dir) / SCYLLA_CONF_DIR / SCYLLA_CONF
    with scylla_yaml_path.open() as f:
        return YAML().load(f)


def _get_scylla_version(install_dir):
    scylla_version_files = [
        os.path.join(install_dir, 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, 'build', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, '..', '..', 'build', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'SCYLLA-VERSION-FILE'),
        os.path.join(install_dir, 'scylla-core-package', 'scylla', 'SCYLLA-VERSION-FILE'),
    ]

    # Track if we found any version file (even with invalid content)
    version_file_exists = False

    for version_file in scylla_version_files:
        if os.path.exists(version_file):
            version_file_exists = True
            with open(version_file) as file:
                v = file.read().strip()
            # return only version strings (loosly) conforming to PEP-440
            # See https://www.python.org/dev/peps/pep-0440/
            # 'i.j(.|-)dev[N]' < 'i.j.rc[N]' < 'i.j.k' < i.j(.|-)post[N]
            if re.fullmatch(r'(\d+!)?\d+([.-]\d+)*([a-z]+\d*)?([.-](post|dev|rc)\d*)*', v):
                return v

    # If we found a version file but it had invalid content, return default
    if version_file_exists:
        return '3.0'

    # No version file found at all - check if scylla binary exists to provide better error message
    scylla_bin_paths = [
        os.path.join(install_dir, 'scylla'),
        os.path.join(install_dir, 'bin', 'scylla'),
    ]
    scylla_build_modes = ['debug', 'dev', 'release']
    cmake_build_types = ['Debug', 'Dev', 'RelWithDebInfo']
    for mode in scylla_build_modes + cmake_build_types:
        scylla_bin_paths.append(os.path.join(install_dir, 'build', mode, 'scylla'))

    scylla_bin_exists = any(os.path.exists(path) for path in scylla_bin_paths)

    if scylla_bin_exists:
        # Scylla binary exists but no version file found
        raise CCMError(
            f"Could not find SCYLLA-VERSION-FILE in the Scylla installation directory.\n"
            f"Searched in: {install_dir}\n"
            f"Expected locations:\n" + "\n".join(f"  - {vf}" for vf in scylla_version_files) + "\n"
            f"Please ensure you have built Scylla completely or use a relocatable package."
        )
    else:
        # No scylla binary found either
        raise CCMError(
            f"Could not find Scylla binary in the installation directory: {install_dir}\n"
            f"Expected to find 'scylla' binary in one of:\n"
            f"{chr(10).join(f'  - {path}' for path in scylla_bin_paths)}\n"
            f"Please ensure --install-dir points to a valid Scylla installation or build directory."
        )


def get_version_from_build(install_dir=None, node_path=None):
    if install_dir is None and node_path is not None:
        install_dir = get_install_dir_from_cluster_conf(node_path)
    if install_dir is not None:
        if isScylla(install_dir):
            return _get_scylla_version(install_dir)
        # Binary cassandra installs will have a 0.version.txt file
        version_file = os.path.join(install_dir, '0.version.txt')
        if os.path.exists(version_file):
            with open(version_file) as f:
                return f.read().strip()
        # For DSE look for a dse*.jar and extract the version number
        dse_version = get_dse_version(install_dir)
        if (dse_version is not None):
            return dse_version
        # Source cassandra installs we can read from build.xml
        build = os.path.join(install_dir, 'build.xml')
        if not os.path.exists(build):
            raise CCMError(
                f"Cannot find version information in {install_dir}.\n"
                f"Expected to find one of:\n"
                f"  - {version_file} (for binary Cassandra installs)\n"
                f"  - {build} (for source Cassandra installs)\n"
                f"  - dse*.jar (for DSE installs)\n"
                f"Please ensure --install-dir points to a valid Cassandra/DSE installation."
            )
        with open(build) as f:
            for line in f:
                match = re.search(r'name="base\.version" value="([0-9.]+)[^"]*"', line)
                if match:
                    return match.group(1)
    raise CCMError("Cannot find version")


def get_java_home_path(parent_path: Path, hardcode_java_version: list[str]) -> Path | None:
    for java in Path(parent_path).rglob('*/bin/java'):
        if get_jvm_spec_version(java) in hardcode_java_version:
            return java.parent.parent
    return None


java_version_property_regexp = re.compile(r'\s*java\.specification\.version\s*=\s*([0-9.]+)\s*')


def get_jvm_spec_version(java_bin_path: Path) -> str | None:
    if not java_bin_path.is_file() or not os.access(java_bin_path, os.X_OK):
        return None
    try:
        properties = subprocess.check_output([java_bin_path.as_posix(), '-XshowSettings:properties', '-version'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        return None

    match = java_version_property_regexp.search(properties.decode('utf-8'))
    if match is None:
        return None
    for version in match.groups():
        return version
    return None
