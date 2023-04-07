#!/bin/bash
#
# Copyright (C) 2023-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

set -eu

SCRIPT_PATH="$(realpath "$0")"
PROJECT_BASE="$(realpath "$(dirname "$0")"/..)"
WORKING_DIR="$(realpath "$PWD")"
if [ "$PROJECT_BASE" != "$WORKING_DIR" ]; then
    echo "Error: $SCRIPT_PATH should be ran with $PROJECT_BASE instead of $WORKING_DIR as the working directory" >&2
    exit 1
fi

BUILD_PATH=build/release-cs-pgo/profiles/merged.profdata
TARGET_PATH=pgo/profiles/$(uname -m)/profile.profdata.xz
./configure.py --mode=release --pgo --cspgo --use-profile=

# ninja "$BUILD_PATH" would avoid a build step, but let's do it voluntarily
# to check that the profile doesn't cause any compilation problems.
ninja build/release/scylla

# Profiles are stored in version control, so we want very strong compression.
mkdir -p "$(dirname "$TARGET_PATH")"
xz --compress -9 --stdout "$BUILD_PATH" >"$TARGET_PATH"

echo "Profile $TARGET_PATH regenerated. You can now stage, commit, and push it."
