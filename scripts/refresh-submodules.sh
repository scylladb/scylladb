#!/bin/bash
#
# Refresh git submodules by fast-forward merging them to the tip of the
# master branch of their respective repositories and committing the
# update with a default commit message of "git submodule summary".
#
# Copyright (C) 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

set -euo pipefail

# The following is the default list of submodules to refresh. To only refresh
# some of them, pass the list of modules to refresh as arguments. For example,
# "scripts/refresh-submodules.sh seastar tools/java" only refreshes the
# two submodules seastar and tools/java.
submodules=(
    seastar
    tools/jmx
    tools/java
    tools/python3
)

for ent in "${@:-${submodules[@]}}"; do
    submodule=${ent%%:*}
    [ ${submodule} == ${ent} ] && branch="master" || branch=${ent#*:}
    bump_to=origin/${branch}

    pushd .
    cd $submodule
    git fetch origin ${branch}
    if ! git merge-base --is-ancestor HEAD ${bump_to}; then
        echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
        exit 1
    fi
    # collect the summary
    head_ref=$(git rev-parse --short=8 HEAD)
    branch_ref=$(git rev-parse --short=8 origin/${branch})
    count=$(git rev-list --no-merges --count HEAD..${bump_to})
    # create a summary using the output format of "git submodule summary"
    SUMMARY="
* ${submodule} ${head_ref}...${branch_ref} (${count}):
$(git log --pretty='format:  > %s' --no-merges HEAD..${bump_to})"

    # fast-forward to origin/master
    git merge --ff-only ${bump_to}
    popd

    if [ ! -z "$SUMMARY" ]; then
        git commit --edit -m "Update $submodule submodule" -m "$SUMMARY" $submodule
    fi
done
