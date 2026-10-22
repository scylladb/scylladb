# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""ScyllaDB Conan 2 binary compatibility plugin.

Install as $CONAN_HOME/extensions/plugins/compatibility/compatibility.py.

A binary is accepted in place of the requested one when it was built with
  * GCC   >= 13, or
  * Clang >= 20,
and, for C++ packages, with C++17 or newer (ISO or GNU dialect).
Everything else (os, arch, build_type, options, ...) must match exactly.
"""

from conan.tools.build import supported_cppstd
from conan.tools.scm import Version

# Minimum accepted compiler versions.
MIN_COMPILER_VERSION = {
    'gcc': 13,
    'clang': 20,
}
MIN_CPPSTD = 17

# GCC and Clang binaries are interchangeable only when they share the C++ runtime.
# None means the recipe dropped the setting (a pure C package).
_SHARED_LIBCXX = (None, 'libstdc++', 'libstdc++11')


def _cppstd_number(cppstd: str) -> int:
    """Order '17'/'gnu17' style values chronologically; C++98 sorts before C++11."""
    number = int(cppstd.removeprefix('gnu'))
    return number - 100 if number >= 98 else number


def _accepted_versions(conanfile, compiler: str) -> list[str]:
    """Major versions of `compiler` known to settings.yml that satisfy the minimum."""
    known = conanfile.settings.compiler.possible_values().get(compiler, {}).get('version', [])
    # Only major versions: this is what Conan's profile detection and ConanCenter use
    # for these compilers, and it keeps the number of candidates small.
    accepted = [v for v in known
                if v and str(v).isdigit() and int(v) >= MIN_COMPILER_VERSION[compiler]]
    return sorted(accepted, key=int)


def _accepted_cppstds(conanfile, compiler: str, version: str) -> list[str]:
    supported = supported_cppstd(conanfile, compiler, version) or []
    return [v for v in supported if _cppstd_number(v) >= MIN_CPPSTD]


def compatibility(conanfile) -> list[dict]:
    settings = conanfile.settings
    compiler = settings.get_safe('compiler')
    version = settings.get_safe('compiler.version')
    cppstd = settings.get_safe('compiler.cppstd')
    libcxx = settings.get_safe('compiler.libcxx')
    cstd = settings.get_safe('compiler.cstd')

    # Only consumers that satisfy the same requirements take part.
    if compiler not in MIN_COMPILER_VERSION or not version:
        return []
    if Version(version) < MIN_COMPILER_VERSION[compiler]:
        return []
    if cppstd is not None and _cppstd_number(cppstd) < MIN_CPPSTD:
        return []

    compilers = [compiler]
    if libcxx in _SHARED_LIBCXX:
        compilers += [c for c in MIN_COMPILER_VERSION if c != compiler]

    candidates = []
    for other_compiler in compilers:
        versions = _accepted_versions(conanfile, other_compiler)
        if other_compiler == compiler:
            # Closest version first, preferring older (the binary runs on a newer runtime).
            current = Version(version)
            versions.sort(key=lambda v: (abs(int(v) - int(str(current.major))), -int(v)))
        else:
            versions.reverse()
        for other_version in versions:
            # Setting the compiler resets its subsettings, so they are all listed again.
            base = [('compiler', other_compiler), ('compiler.version', other_version)]
            if libcxx is not None:
                base.append(('compiler.libcxx', libcxx))
            if cstd is not None:
                base.append(('compiler.cstd', cstd))
            if cppstd is None:
                candidates.append(base)
                continue
            cppstds = _accepted_cppstds(conanfile, other_compiler, other_version)
            # Same standard first, then the nearest ones.
            cppstds.sort(key=lambda v: (v != cppstd,
                                        abs(_cppstd_number(v) - _cppstd_number(cppstd)),
                                        v.startswith('gnu') != cppstd.startswith('gnu')))
            candidates.extend(base + [('compiler.cppstd', v)] for v in cppstds)

    return [{'settings': candidate} for candidate in candidates]
