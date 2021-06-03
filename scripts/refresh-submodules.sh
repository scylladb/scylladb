#!/bin/bash
#
# Refresh git submodules by fast-forward merging them to the tip of the
# master branch of their respective repositories and committing the
# update with a default commit message of "git submodule summary".
#
# Copyright (C) 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

set -euo pipefail

# The following is the default list of submodules to refresh. To only refreh
# some of them, pass the list of modules to refresh as arguments. For example,
# "scripts/refresh-submodules.sh seastar tools/java" only refreshes the
# two submodules seastar and tools/java.
submodules=(
    seastar
    tools/jmx
    tools/java
    tools/python3
)

for submodule in "${@:-${submodules[@]}}"; do
    GIT_DIR="$submodule/.git" git pull --ff-only origin master
    SUMMARY=$(git submodule summary $submodule)
    if grep '^ *<' <<< "$SUMMARY"; then
        echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
        exit 1
    fi
    if [ ! -z "$SUMMARY" ]; then
        git commit --edit -m "Update $submodule submodule" -m "$SUMMARY" $submodule
    fi
done
