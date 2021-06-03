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
    echo "error: running $PROGRAM in Docker is not supported, please run on host."
    exit 1
fi

docker_images=(
    docker.io/centos:7
)

for docker_image in "${docker_images[@]}"
do
    docker_script="${docker_image//:/-}"
    install_sh="$(pwd)/tools/testing/dist-check/$docker_script.sh"
    if [ -f "$install_sh" ]; then
        docker run -i --rm -v $(pwd):$(pwd) $docker_image /bin/bash -c "cd $(pwd) && $install_sh --mode $MODE"
    else
        echo "internal error: $install_sh does not exist, please create one to verify packages on $docker_image."
        exit 1
    fi
done
