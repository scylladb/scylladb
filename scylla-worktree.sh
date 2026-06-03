#!/usr/bin/env bash
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  ./scylla-worktree.sh add <branch>
  ./scylla-worktree.sh <branch>
  ./scylla-worktree.sh setup <branch>
  ./scylla-worktree.sh remove <branch>

Adds worktrees as siblings of this checkout, named scylla_<branch>.
Branch names containing '/' are mapped to '_' in the directory name.

  add / <branch>   Create a new worktree, copy kmipc, and run configure.py.
  setup <branch>   Fix up an existing worktree (copy kmipc if missing,
                   update git exclude, run configure.py).
  remove <branch>  Remove a worktree.
EOF
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

quote_cmd() {
    printf '%q ' "$@"
}

run() {
    printf '+ '
    quote_cmd "$@"
    printf '\n'
    "$@"
}

run_in() {
    local dir=$1
    shift
    printf '+ cd %q && ' "$dir"
    quote_cmd "$@"
    printf '\n'
    (cd "$dir" && "$@")
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel)
repo_root=$(cd "$repo_root" && pwd -P)
parent_dir=$(dirname -- "$repo_root")

validate_branch() {
    local branch=$1
    [[ -n $branch ]] || die "branch name is required"
    git check-ref-format --branch "$branch" >/dev/null || die "invalid branch name: $branch"
}

worktree_basename() {
    local branch=$1
    local name=${branch//\//_}
    name=${name//[!A-Za-z0-9._-]/_}
    [[ -n $name ]] || die "branch name produced an empty directory name"
    printf 'scylla_%s' "$name"
}

worktree_path() {
    local branch=$1
    printf '%s/%s' "$parent_dir" "$(worktree_basename "$branch")"
}

ensure_safe_target() {
    local target=$1
    local base
    base=$(basename -- "$target")
    [[ $target == "$parent_dir"/scylla_* ]] || die "refusing to operate outside $parent_dir/scylla_*: $target"
    [[ $base == scylla_* ]] || die "refusing to operate on non-scylla worktree: $target"
}

copy_kmipc() {
    local target=$1
    local source=$repo_root/kmipc

    [[ -d $source ]] || die "missing $source; clone or copy cryptsoft-kmipc there first"
    if [[ -e $target/kmipc ]]; then
        printf 'kmipc already exists in %s, skipping copy\n' "$target"
        return
    fi

    run cp -a "$source" "$target/kmipc"
}

ignore_kmipc() {
    local target=$1
    local exclude

    exclude=$(git -C "$target" rev-parse --git-path info/exclude)
    mkdir -p -- "$(dirname -- "$exclude")"
    if [[ ! -f $exclude ]] || ! grep -qxF '/kmipc/' "$exclude"; then
        printf '\n/kmipc/\n' >> "$exclude"
    fi
}

add_worktree() {
    local branch=$1
    local target

    validate_branch "$branch"
    target=$(worktree_path "$branch")
    ensure_safe_target "$target"

    [[ ! -e $target && ! -L $target ]] || die "target already exists: $target"

    if git -C "$repo_root" show-ref --verify --quiet "refs/heads/$branch"; then
        run git -C "$repo_root" worktree add "$target" "$branch"
    elif git -C "$repo_root" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        run git -C "$repo_root" worktree add --track -b "$branch" "$target" "origin/$branch"
    else
        run git -C "$repo_root" worktree add -b "$branch" "$target" HEAD
    fi

    run git -C "$target" submodule sync --recursive
    run git -C "$target" submodule update --init --recursive
    copy_kmipc "$target"
    ignore_kmipc "$target"
    run_in "$target" ./configure.py --mode dev

    printf 'Created worktree: %s\n' "$target"
}

setup_worktree() {
    local branch=$1
    local target

    validate_branch "$branch"
    target=$(worktree_path "$branch")
    ensure_safe_target "$target"

    [[ -d $target ]] || die "worktree does not exist: $target (use 'add' to create it)"

    copy_kmipc "$target"
    ignore_kmipc "$target"

    # Always reconfigure: the typical reason for running setup is that kmipc
    # was missing when configure.py first ran, so HAVE_KMIP is absent from
    # the existing build.ninja.
    run_in "$target" ./configure.py --mode dev

    printf 'Setup complete: %s\n' "$target"
}

branch_from_dir() {
    local dir=$1
    dir=$(cd -- "$dir" 2>/dev/null && pwd -P) || dir=$(realpath -- "$dir" 2>/dev/null) || dir=${dir%/}
    local base
    base=$(basename -- "$dir")
    [[ $base == scylla_* ]] || die "directory name does not start with scylla_: $base"
    printf '%s' "${base#scylla_}"
}

remove_worktree() {
    local arg=$1
    local target

    if [[ -d $arg || $arg == */* ]]; then
        local branch
        branch=$(branch_from_dir "$arg")
        target=$(cd -- "$arg" 2>/dev/null && pwd -P) || target=$(realpath -- "$arg" 2>/dev/null) || target=${arg%/}
    else
        validate_branch "$arg"
        target=$(worktree_path "$arg")
    fi
    ensure_safe_target "$target"

    [[ -e $target || -L $target ]] || die "target does not exist: $target"

    if ! git -C "$repo_root" worktree remove --force "$target"; then
        printf 'warning: git worktree remove failed; removing %s directly\n' "$target" >&2
        run rm -rf --one-file-system -- "$target"
        run git -C "$repo_root" worktree prune
    fi

    printf 'Removed worktree: %s\n' "$target"
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

case ${1:-} in
    -h|--help|help)
        usage
        ;;
    add)
        [[ $# -eq 2 ]] || die "usage: ./scylla-worktree.sh add <branch>"
        add_worktree "$2"
        ;;
    setup|fix)
        [[ $# -eq 2 ]] || die "usage: ./scylla-worktree.sh setup <branch>"
        setup_worktree "$2"
        ;;
    remove|rm|delete)
        [[ $# -eq 2 ]] || die "usage: ./scylla-worktree.sh remove <branch>"
        remove_worktree "$2"
        ;;
    *)
        [[ $# -eq 1 ]] || die "usage: ./scylla-worktree.sh <branch>"
        add_worktree "$1"
        ;;
esac
