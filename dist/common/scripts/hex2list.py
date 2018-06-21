#!/usr/bin/python3
#
# Copyright 2016 ScyllaDB
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
#
# hex2list.py is cpuset format converter (see cpuset(7)).
# It reads "Mask Format" parameter from stdin,
# outputs "List Format" parameter to stdout.
#
# Here's example to convert '00000000,000e3862' to List Format:
# $ echo 00000000,000e3862 | hex2list.py
# 1,5-6,11-13,17-19
#

from scylla_util import hex2list

print(hex2list(input()))
