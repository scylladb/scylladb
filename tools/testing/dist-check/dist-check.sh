#!/bin/bash -e

#
# Copyright (C) 2020-present ScyllaDB
#

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
    docker.io/centos:7
)

for container_image in "${container_images=[@]}"
do
    container_script="${container_image//:/-}"
    install_sh="$(pwd)/tools/testing/dist-check/$container_script.sh"
    if [ -f "$install_sh" ]; then
        $contool run -i --rm -v $(pwd):$(pwd) $container_image /bin/bash -c "cd $(pwd) && $install_sh --mode $MODE"
    else
        echo "internal error: $install_sh does not exist, please create one to verify packages on $container_image."
        exit 1
    fi
done
