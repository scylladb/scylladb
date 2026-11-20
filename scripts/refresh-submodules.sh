#!/bin/bash
#
# Refresh git submodules by fast-forward merging them to the tip of the
# respective branch of their repositories and committing the update with
# a default commit message of "git submodule summary".
#
# The branch is derived from the superproject next-X or branch-X branch
# (local or remote) that HEAD points at, i.e. the current branch or the one
# it was just forked from, and is branch-X. If there's no such branch, or
# the submodule origin doesn't have the branch-X, master is used. The branch
# can also be set explicitly per submodule as "submodule:branch".
#
# Copyright (C) 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

set -euo pipefail

trap 'echo "error $? in $0 line $LINENO"' ERR

# The following is the default list of submodules to refresh. To only refresh
# some of them, pass the list of modules to refresh as arguments. For example,
# "scripts/refresh-submodules.sh seastar tools/python3" only refreshes the
# two submodules seastar and tools/python3.
submodules=(
    seastar
    tools/python3
)

default_branch=master
for ref in $(git for-each-ref --points-at HEAD --format='%(refname:lstrip=-1)' refs/heads refs/remotes); do
    case "${ref}" in
        next-*)   default_branch=branch-${ref#next-}; break ;;
        branch-*) default_branch=${ref}; break ;;
    esac
done
echo "Default submodule branch: ${default_branch}"

for ent in "${@:-${submodules[@]}}"; do
    submodule=${ent%%:*}

    pushd .
    cd $submodule
    # explicit branch, or the default one if origin has it (not every
    # submodule has release branches), or master
    if [ "${submodule}" != "${ent}" ]; then
        branch=${ent#*:}
    else
        # exit code 2 means no such branch, anything else is an error
        rc=0
        git ls-remote --exit-code origin refs/heads/${default_branch} > /dev/null || rc=$?
        case ${rc} in
            0)  branch=${default_branch} ;;
            2)  echo "No ${default_branch} branch in ${submodule} origin, falling back to master"
                branch=master ;;
            *)  echo "Failed to look up ${default_branch} branch in ${submodule} origin"
                exit ${rc} ;;
        esac
    fi
    bump_to=origin/${branch}
    git fetch origin ${branch}
    if ! git merge-base --is-ancestor HEAD ${bump_to}; then
        echo "Non fast-forward changes detected! Fire three red flares from your flare pistol."
        exit 1
    fi
    # collect the summary
    head_ref=$(git rev-parse --short=8 HEAD)
    branch_ref=$(git rev-parse --short=8 origin/${branch})
    count=$(git rev-list --first-parent --count HEAD..${bump_to})
    # create a summary using the output format of "git submodule summary"
    SUMMARY="
* ${submodule} ${head_ref}...${branch_ref} (${count}):
$(git log --pretty='tformat: %H %s' --first-parent HEAD..${bump_to} | while IFS= read -r line; do
        SUBJECT=$(echo $line | cut -d' ' -f2-)
        echo "  > ${SUBJECT}"
        if [[ "$SUBJECT" == "Merge "* ]]; then
            COMMIT=$(echo $line | cut -d' ' -f1)
            git log --pretty='format:    %s' --no-merges ${COMMIT}^1..${COMMIT}
            echo
        fi
    done)
"

    # fast-forward to origin/master
    git merge --ff-only ${bump_to}
    popd

    if [ ! -z "$SUMMARY" ]; then
        git commit --edit -m "Update $submodule submodule" -m "$SUMMARY" $submodule
    fi
done
