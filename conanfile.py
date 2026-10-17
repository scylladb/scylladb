# Copyright 2026-present ScyllaDB

# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from conan import ConanFile
from conan.tools.cmake import CMakeDeps
from conan.tools.gnu import PkgConfigDeps


class ScyllaBuildDependencies(ConanFile):
    """Build-only C and C++ dependencies for Scylla."""

    settings = "os", "arch", "compiler", "build_type"

    requires = (
        "libdeflate/1.25",
        "xxhash/0.8.3",
    )

    default_options = {
        "libdeflate/*:shared": False,
    }

    def generate(self) -> None:
        """Generate metadata for Scylla's selected build system."""
        generator = self.conf.get("user.scylla:generator", default="pkg-config")
        if generator == "cmake":
            cmake_deps = CMakeDeps(self)
            cmake_deps.configuration = self.conf.get(
                "user.scylla:cmake-configuration",
                default=str(self.settings.build_type),
            )
            cmake_deps.generate()
        else:
            pkg_config = PkgConfigDeps(self)
            pkg_config.generate()
