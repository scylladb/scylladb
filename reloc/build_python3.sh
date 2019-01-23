#!/bin/bash -e
#
# Copyright (C) 2019 ScyllaDB
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

TARGET=build/release/scylla-python3.tar.gz

if [ -f "$TARGET" ]; then
    rm "$TARGET"
fi

PACKAGES="python3-PyYAML python3-urwid python3-pyparsing python3-requests python3-pyudev python3-setuptools"
./scripts/create-relocatable-python.py --output "$TARGET" $PACKAGES
