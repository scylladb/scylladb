#!/bin/bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

#
# Requirements:
# * eu-unstrip
# * curl
# * jq
# * git

set -e

SCRIPT_NAME=$(basename $0)
SCYLLA_S3_RELOC_SERVER_DEFAULT_URL=http://backtrace.scylladb.com

function print_usage {
cat << EOF
Usage: ./${SCRIPT_NAME} [options] COREFILE

Script for "one-click" opening of coredumps.

It extracts the build-id from the coredump, retrieves metadata for that
build, downloads the binary package, the source code and finally
launches the dbuild container, with everything ready to load the
coredump.
Sometimes the metadata retrieved from the build lacks the package URL.
In this case, the scylla package can be downloaded manually and extracted
into the artifact directory (see below) using the "scylla.package" name.
When this directory exists, the script will not attempt to extract the
package URL from the metadata and download the package itself, instead it
will use the provided package.

The script is idempotent: running it after the prepartory steps will
reuse what is already downloaded. It is *strongly* recommended to run
this from an empty directory, with nothing but the core-file present.

Options:

--help,-h
    Print this help message and exit.

--quiet,-q
    Only print errors (if any).

--verbose,-v
    Print progress information when downloading the package and cloning
    the git repo.

--ci
    Designate the coredump as one coming from CI. These coredumps are
    produced by a merge commit between the main branch (master or
    enterprise) and the tested branch and this commit will not be
    present in origin. Using this flag will force the main branch to
    be checked out, instead of the commit hash obtained from the
    ScyllaDB vesrsion.

--artifact-dir,-d ARTIFACT_DIR
    Directory where the script will store all downloaded artifacts. If
    the directory doesn't exist, it will be created.
    It is strongly recommended that this is an empty directory, not used
    for anything else, to avoid interference with the script.
    To reset the state of the script, just delete this directory.
    This can be the same as the directory the coredump is already in, or
    some other path.
    By default, a directory called ${SCRIPT_NAME}.dir will be created in
    the script's working directory.
    Can be also provided via env variable ARTIFACT_DIR.
    If both are provided, command line has precedence.

--scylla-package-url,-p SCYLLA_PACKAGE_URL
    Instead of querying the s3-reloc-server (see below) and downloading the
    package from the obtained URL, download it directly from the provided URL
    and extract the scylla version metadata from the package.
    If the package is already downloaded, it is not downloaded again.

--scylla-s3-reloc-server-url,-u SCYLLA_S3_RELOC_SERVER_URL
    The URL of the s3 reloc server to connect to. Needed to fetch the
    build info based on the build-id.
    Defaults to ${SCYLLA_S3_RELOC_SERVER_DEFAULT_URL}
    Can be also provided via env variable SCYLLA_S3_RELOC_SERVER_URL.
    If both are provided, command line has precedence.

--scylla-repo-path,-r SCYLLA_REPO_PATH
    Path to an existing scylla repository to use. The repo is expected to have
    the branch and commit the packages were built from.
    If not provided, the appropriate git repository (either scylladb.git or
    scylla-enterprise.git) will be cloned from https://github.com/scylladb.
    The script assumes you have the appropriate access rights set up.
    Can be also provided via env variable SCYLLA_REPO_PATH.
    If both are provided, command line has precedence.

--scylla-gdb-py-source,-s repo|package|none
    Where to get the scylla-gdb.py script from:
    * repo - download the latest one from the appropriate main branch;
    * package - use the one that comes with the relocatable package;
    * none - don't try to obtain scylla-gdb.py;
    Defaults to repo.
    Unlike other artifacts, scylla-gdb.py is placed in the same directory
    where the coredump is located. If one is already present, it is not
    overwritten.
    Can be also provided via env variable SCYLLA_GDB_PY_SOURCE.
    If both are provided, command line has precedence.
EOF
}

function log {
    if [[ $VERBOSE_LEVEL -ge 1 ]]
    then
        echo $@
    fi
}

function get_json_field {
    local json_obj=$1
    local field_name=$2
    local optional=${3:-0}
    local field_val=$(jq -r ".${field_name}" <<< "$json_obj")

    if [[ -z $field_val ]] && [[ $optional -eq 0 ]]
    then
        echo "error: failed to get field '$field_name' from: $json_obj" >&2
        exit 1
    fi

    echo $field_val
}

for required in eu-unstrip jq curl git; do
    if ! type $required >& /dev/null; then
        echo "error: missing required program $required, please install first" >&2
        exit 1
    fi
done

VERBOSE_LEVEL=1
CORE_FROM_CI=0
PACKAGE_URL="${SCYLLA_PACKAGE_URL}"
SCYLLA_S3_RELOC_SERVER_URL="${SCYLLA_S3_RELOC_SERVER_URL:-$SCYLLA_S3_RELOC_SERVER_DEFAULT_URL}"
SCYLLA_REPO_PATH="${SCYLLA_REPO_PATH}"
SCYLLA_BUILD_ID="${SCYLLA_BUILD_ID}"
SCYLLA_GDB_PY_SOURCE="${SCYLLA_GDB_PY_SOURCE:-repo}"
ARTIFACT_DIR=${ARTIFACT_DIR:-${SCRIPT_NAME}.dir}

if [[ $# -ge 1 && ( "$1" == "--help" || "$1" == "-h" ) ]]; then
    print_usage
    exit 0
fi

while [[ $# -gt 1 ]]
do
    case $1 in
        "--verbose"|"-v")
            VERBOSE_LEVEL=2
            shift 1
            ;;
        "--quiet"|"-q")
            VERBOSE_LEVEL=0
            shift 1
            ;;
        "--ci")
            CORE_FROM_CI=1
            shift 1
            ;;
        "--artifact-dir"|"-d")
            ARTIFACT_DIR=$2
            shift 2
            ;;
        "--scylla-package-url"|"-p")
            PACKAGE_URL=$2
            shift 2
            ;;
        "--scylla-s3-reloc-server-url"|"-u")
            SCYLLA_S3_RELOC_SERVER_URL=$2
            shift 2
            ;;
        "--scylla-repo-path"|"-r")
            SCYLLA_REPO_PATH=$2
            shift 2
            ;;
        "--scylla-gdb-py-source"|"-s")
            SCYLLA_GDB_PY_SOURCE=$2
            shift 2
            ;;
        "--scylla-build-id"|"-b")
            SCYLLA_BUILD_ID=$2
            shift 2
            ;;
        *)
            echo "error: unrecognized option: $1, see $0 -h for usage" >&2
            exit 1
            ;;
    esac
done

COREFILE=$1
shift 1
if ! [[ -f $COREFILE ]]
then
    echo "error: ${COREFILE} is not a valid path to a core file" >&2
    exit 1
fi
COREDIR=$(dirname ${COREFILE})

ARTIFACT_DIR=$(realpath ${ARTIFACT_DIR})
mkdir -p $ARTIFACT_DIR

if [[ -z "${SCYLLA_REPO_PATH}" ]]
then
    SCYLLA_REPO_PATH="${ARTIFACT_DIR}/scylla.repo"
fi

SCYLLA_REPO_PATH=$(realpath ${SCYLLA_REPO_PATH})

if [[ ${VERBOSE_LEVEL} -lt 2 ]]
then
    CURL_QUIET_FLAG="-s"
    GIT_QUIET_FLAG="-q"
else
    CURL_QUIET_FLAG=""
    GIT_QUIET_FLAG=""
fi

case "${SCYLLA_GDB_PY_SOURCE}" in
    ("repo"|"package"|"none")
        ;;
    *)
        echo "error: invalid value for option SCYLLA_GDB_PY_SOURCE, has to be one of repo, package or none, got: ${SCYLLA_GDB_PY_SOURCE}" >&2
        exit 1
        ;;
esac

if [[ -z "${PACKAGE_URL}" ]]
then
    BUILD_ID="${SCYLLA_BUILD_ID}"
    if [[ -z "${BUILD_ID}" ]]; then
        BUILD_ID=$(eu-unstrip -n --core ${COREFILE} | grep 'scylla$' | cut -f2 -d' ' | cut -f1 -d@)
    fi

    log "Build id: ${BUILD_ID}"

    BUILD=$(curl -s -X GET "${SCYLLA_S3_RELOC_SERVER_URL}/build.json?build_id=${BUILD_ID}")

    if [[ -z "$BUILD" ]]
    then
        echo "error: failed to retrieve build information from ${SCYLLA_S3_RELOC_SERVER_URL}" >&2
        exit 1
    fi

    RESPONSE_BUILD_ID=$(get_json_field "$BUILD" "build_id")
    VERSION=$(get_json_field "$BUILD" "version")
    PRODUCT=$(get_json_field "$BUILD" "product")
    RELEASE=$(get_json_field "$BUILD" "release")
    ARCH=$(get_json_field "$BUILD" "arch")
    BUILD_MODE=$(get_json_field "$BUILD" "build_mode")
    PACKAGE_URL=$(get_json_field "$BUILD" "package_url" 1)

    if [[ "$RESPONSE_BUILD_ID" != "$BUILD_ID" ]]
    then
        echo "error: mismatching build id: requested ${BUILD_ID} but got ${RESPONSE_BUILD_ID}" >&2
        exit 1
    fi

    log "Matching build is ${PRODUCT}-${VERSION} ${RELEASE} ${BUILD_MODE}-${ARCH}"
fi

if ! [[ -d ${ARTIFACT_DIR}/scylla.package ]]
then
    if [[ -z $PACKAGE_URL ]]
    then
        echo "error: no package_url in build object: ${BUILD}" >&2
        echo "" >&2
        echo "The package can be provided manually by placing it (unpacked) to "${ARTIFACT_DIR}/scylla.package" to work around this problem." >&2
        exit 1
    fi
    PACKAGE_FILE=$(basename ${PACKAGE_URL})
    if ! [[ -f ${ARTIFACT_DIR}/${PACKAGE_FILE} ]]
    then
        log "Downloading relocatable package from ${PACKAGE_URL}"
        curl -L ${CURL_QUIET_FLAG} --output ${ARTIFACT_DIR}/${PACKAGE_FILE} ${PACKAGE_URL}
    else
        log "Relocatable package ${PACKAGE_URL} already downloaded"
    fi

    log "Extracting package ${PACKAGE_FILE}"
    pushd ${ARTIFACT_DIR} > /dev/null
    tar -zxf $PACKAGE_FILE
    mv scylla scylla.package
    popd > /dev/null
else
    log "Relocatable package ${PACKAGE_URL} already downloaded and extracted"
fi

# If the package was provided directly we bypassed talking to the S3 server and
# the version metadata has to be loaded from the package itself.
if [[ -z "$VERSION" ]]
then
    VERSION=$(cat ${ARTIFACT_DIR}/scylla.package/SCYLLA-VERSION-FILE)
    PRODUCT=$(cat ${ARTIFACT_DIR}/scylla.package/SCYLLA-PRODUCT-FILE)
    RELEASE=$(cat ${ARTIFACT_DIR}/scylla.package/SCYLLA-RELEASE-FILE)
fi

if [[ "${PRODUCT}" == "scylla-enterprise" ]]
then
    MAIN_BRANCH=enterprise
else
    MAIN_BRANCH=master
fi

COMMIT_HASH=$(cut -f3 -d. <<< $RELEASE)
if [ $CORE_FROM_CI -eq 1 ]
then
    COMMIT_HASH=${MAIN_BRANCH}
fi
if [ "$(grep -o ~dev <<< $VERSION)" == "~dev" ]
then
    BRANCH=${MAIN_BRANCH}
else
    BASE_VERSION=$(grep -o "^[0-9]\+\.[0-9]\+" <<< $VERSION)
    BRANCH=branch-${BASE_VERSION}
fi

if ! [[ -d ${SCYLLA_REPO_PATH} ]]
then
    log "Cloning ${PRODUCT}.git"
    git clone ${GIT_QUIET_FLAG} -b ${BRANCH} git@github.com:scylladb/${PRODUCT}.git ${SCYLLA_REPO_PATH}
    REMOTE_REPO_NAME=origin
else
    log "${PRODUCT}.git already cloned"
    REMOTE_REPO_NAME=$(cd ${SCYLLA_REPO_PATH}; git remote -v | awk "/scylladb\/${PRODUCT}.*fetch/{ print \$1 }")
fi

# We do the checkout unconditionally, it is cheap anyway.
pushd ${SCYLLA_REPO_PATH} > /dev/null
git fetch -q ${REMOTE_REPO_NAME} ${BRANCH}
git checkout -q ${COMMIT_HASH}
# Skip the other submodules, they are not needed for debugging
git submodule -q sync
git submodule update -q --depth=1 --init seastar
popd > /dev/null

if ! [[ -f ${COREDIR}/scylla-gdb.py ]]
then
    if [[ "${SCYLLA_GDB_PY_SOURCE}" == "repo" ]]
    then
        WORKDIR=$(pwd)
        cd ${SCYLLA_REPO_PATH}
        git checkout -q $MAIN_BRANCH
        git pull -q --no-recurse-submodules ${REMOTE_REPO_NAME} $MAIN_BRANCH
        log "Copying scylla-gdb.py from ${SCYLLA_REPO_PATH}"
        cp scylla-gdb.py ${WORKDIR}/scylla-gdb.py
        git checkout -q ${COMMIT_HASH}
        cd $WORKDIR
    elif [[ "${SCYLLA_GDB_PY_SOURCE}" == "package" ]]
    then
        log "Copying scylla-gdb.py from package"
        cp -n ${ARTIFACT_DIR}/scylla.package/scylla-gdb.py .
    else
        log "scylla-gdb.py was not requested"
    fi
else
    log "scylla-gdb.py already present"
fi

if [[ ${VERBOSE_LEVEL} -ge 1 ]]
then
cat << EOF
Launching dbuild container.

To examine the coredump with gdb:

    $ gdb -x scylla-gdb.py -ex 'set directories /src/scylla' -iex 'set solib-search-path /opt/scylladb/libreloc/' --core ${COREFILE} /opt/scylladb/libexec/scylla

See https://github.com/scylladb/scylladb/blob/master/docs/dev/debugging.md for more information on how to debug scylla.

Good luck!
EOF
fi

TOP_SRCDIR="$(dirname "$(dirname "$(readlink -f "$0")")")"
IMAGE="${SCYLLA_REPO_PATH}/tools/toolchain/image"
exec ${TOP_SRCDIR}/tools/toolchain/dbuild               \
    --image "$(<"$IMAGE")"                              \
    -it                                                 \
    -v $(pwd):/workdir                                  \
    -v ${SCYLLA_REPO_PATH}:/src/scylla                  \
    -v ${ARTIFACT_DIR}/scylla.package:/opt/scylladb     \
    -w /workdir -- bash -l
