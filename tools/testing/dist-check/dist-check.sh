#!/bin/bash -e

#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

PROGRAM=$(basename $0)

print_usage() {
    echo "Usage: $PROGRAM [OPTION]..."
    echo ""
    echo "  --mode MODE  The build mode of 'scylla' to verify (options: 'release', 'dev', and 'debug')."
    exit 1
}

if which podman > /dev/null 2>&1 ; then
	contool=podman
elif which docker > /dev/null 2>&1 ; then
	contool=docker
else
	echo "Please make sure you have either podman or docker installed on this host in order to use dist-chec"
	exit 1
fi

while [ $# -gt 0 ]; do
    case "$1" in
        "--mode")
            MODE=$2
            shift 2
            ;;
        "--help")
            print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ -z "$MODE" ]; then
    print_usage
fi

if [ -f /.dockerenv ]; then
    echo "error: running $PROGRAM in container is not supported, please run on host."
    exit 1
fi

container_images=(
    docker.io/rockylinux:9
)

for container_image in "${container_images=[@]}"
do
    container_script="${container_image//:/-}"
    install_sh="$(pwd)/tools/testing/dist-check/$container_script.sh"
    if [ -f "$install_sh" ]; then
        $contool run -i --rm -v $(pwd):$(pwd):Z $container_image /bin/bash -c "cd $(pwd) && $install_sh --mode $MODE"
    else
        echo "internal error: $install_sh does not exist, please create one to verify packages on $container_image."
        exit 1
    fi
done
