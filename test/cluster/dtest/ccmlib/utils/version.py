#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Restored verbatim (imports aside) from ccm's ccmlib/utils/version.py, needed by
# not-yet-adapted dtest/unported test modules that import ComparableScyllaVersion.

from __future__ import annotations

import re
from packaging.version import parse, Version


# NOTE: following regex is taken from the 'semver' package as is:
#       https://python-semver.readthedocs.io/en/2.10.0/readme.html
SEMVER_REGEX = re.compile(
    r"""
        ^
        (?P<major>0|[1-9]\d*)
        \.
        (?P<minor>0|[1-9]\d*)
        \.
        (?P<patch>0|[1-9]\d*)
        (?:-(?P<prerelease>
            (?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)
            (?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*
        ))?
        (?:\+(?P<build>
            [0-9a-zA-Z-]+
            (?:\.[0-9a-zA-Z-]+)*
        ))?
        $
    """,
    re.VERBOSE,
)

class ComparableScyllaVersion:
    """Accepts and compares known 'non-semver' and 'semver'-like Scylla versions."""

    def __init__(self, version_string: str) -> None:
        parsed_version = self.parse(version_string)
        self.v_major = int(parsed_version["major"])
        self.v_minor = int(parsed_version["minor"])
        self.v_patch = int(parsed_version["patch"])
        self.v_pre_release = parsed_version["prerelease"] or ''
        self.v_build = parsed_version["build"] or ''

    @staticmethod
    def parse(version_string: str) -> re.Match:
        """Parse scylla-binary and scylla-docker-tag versions into a proper semver structure."""
        # NOTE: remove 'with build-id' part if exists and other possible non-semver parts
        _scylla_version = (version_string or '').split(" ")[0]

        # NOTE: replace '~' which gets returned by the scylla binary
        _scylla_version = _scylla_version.replace('~', '-')

        # NOTE: remove docker-specific parts if version is taken from a docker tag
        _scylla_version = _scylla_version.replace('-aarch64', '')
        _scylla_version = _scylla_version.replace('-x86_64', '')

        # NOTE: transform gce-image version like '2024.2.0.dev.0.20231219.c7cdb16538f2.1'
        if gce_image_v_match := re.search(r"(\d+\.\d+\.\d+\.)([a-z0-9]+\.)(.*)", _scylla_version):
            _scylla_version = f"{gce_image_v_match[1][:-1]}-{gce_image_v_match[2][:-1]}-{gce_image_v_match[3]}"

        # NOTE: make short scylla version like '5.2' be correct semver string
        _scylla_version_parts = re.split(r'\.|-', _scylla_version)
        if len(_scylla_version_parts) == 1:
            _scylla_version = f"{_scylla_version}.0.0"
        elif len(_scylla_version_parts) == 2:
            _scylla_version = f"{_scylla_version}.0"
        elif len(_scylla_version_parts) > 2 and re.search(
                r"\D+", _scylla_version_parts[2].split("-")[0]):
            _scylla_version = f"{_scylla_version_parts[0]}.{_scylla_version_parts[1]}.0-{_scylla_version_parts[2]}"
            for part in _scylla_version_parts[3:]:
                _scylla_version += f".{part}"

        # NOTE: replace '-0' with 'dev-0', '-1' with 'dev-1' and so on
        #       to match docker and scylla binary version structures correctly.
        if no_dev_match := re.search(r"(\d+\.\d+\.\d+)(\-\d+)(\.20[0-9]{6}.*)", _scylla_version):
            _scylla_version = f"{no_dev_match[1]}-dev{no_dev_match[2]}{no_dev_match[3]}"

        # NOTE: replace '.' with '+' symbol between build date and build commit
        #       to satisfy semver structure
        if dotted_build_id_match := re.search(r"(.*\.20[0-9]{6})(\.)([\.\d\w]+)", _scylla_version):
            _scylla_version = f"{dotted_build_id_match[1]}+{dotted_build_id_match[3]}"

        if match := SEMVER_REGEX.match(_scylla_version):
            return match
        raise ValueError(
            f"Cannot parse provided '{version_string}' scylla_version for the comparison. "
            f"Transformed scylla_version: {_scylla_version}")

    def major(self) -> 'ComparableScyllaVersion':
        return ComparableScyllaVersion(f"{self.v_major}")

    def minor(self) -> 'ComparableScyllaVersion':
        return ComparableScyllaVersion(f"{self.v_major}.{self.v_minor}")

    def patch(self) -> 'ComparableScyllaVersion':
        return ComparableScyllaVersion(f"{self.v_major}.{self.v_minor}.{self.v_patch}")

    def __str__(self):
        result = f"{self.v_major}.{self.v_minor}.{self.v_patch}"
        if self.v_pre_release:
            result += f"-{self.v_pre_release}"
        if self.v_build:
            result += f"+{self.v_build}"
        return result

    def _transform_to_comparable(self, other):
        if isinstance(other, str):
            return self.__class__(other)
        elif isinstance(other, self.__class__):
            return other
        raise ValueError("Got unexpected type for the comparison: %s" % type(other))

    def as_comparable(self):
        # NOTE: absence of the 'pre-release' part means we have 'GA' version which is newer than
        #       any of the 'pre-release' ones.
        #       So, make empty 'pre-release' prevail over any defined one.
        return (self.v_major, self.v_minor, self.v_patch, self.v_pre_release or 'xyz')

    def __lt__(self, other):
        return self.as_comparable() < self._transform_to_comparable(other).as_comparable()

    def __le__(self, other):
        return self.as_comparable() <= self._transform_to_comparable(other).as_comparable()

    def __eq__(self, other):
        return self.as_comparable() == self._transform_to_comparable(other).as_comparable()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ge__(self, other):
        return not self.__lt__(other)

    def __gt__(self, other):
        return not self.__le__(other)


class ComparableCassandraVersion(ComparableScyllaVersion):
    pass

def parse_version(v: str) -> Version:
    v = v.replace('~', '-')
    return parse(v)
