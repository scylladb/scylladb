#!/usr/bin/python3

#
# Copyright (C) 2018 ScyllaDB
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

import argparse
import io
import os
import subprocess
import tarfile


def ldd(executable):
    '''Given an executable file, return a dictionary with the keys
    containing its shared library dependencies and the values pointing
    at the files they resolve to. A fake key ld.so points at the
    dynamic loader.'''
    libraries = {}
    for ldd_line in subprocess.check_output(
            ['ldd', executable],
            universal_newlines=True).splitlines():
        elements = ldd_line.split()
        if ldd_line.endswith('not found'):
            raise Exception('ldd could not resolve {}'.format(elements[0]))
        if elements[1] != '=>':
            if elements[0].startswith('linux-vdso.so'):
                # provided by kernel
                continue
            libraries['ld.so'] = os.path.realpath(elements[0])
        else:
            libraries[elements[0]] = os.path.realpath(elements[2])
    return libraries


ap = argparse.ArgumentParser(description='Create a relocatable scylla package.')
ap.add_argument('dest',
                help='Destination file (tar format)')
ap.add_argument('--mode', dest='mode', default='release',
                help='Build mode (debug/release) to use')

args = ap.parse_args()

executables = ['build/{}/scylla'.format(args.mode),
               'build/{}/iotune'.format(args.mode)]

output = args.dest

libs = {}
for exe in executables:
    libs.update(ldd(exe))

ld_so = libs['ld.so']

ar = tarfile.open(output, mode='w')

# This thunk is a shell script that arranges for the executable to be invoked,
# under the following conditions:
#
#  - the same argument vector is passed to the executable, including argv[0]
#  - the executable name (/proc/pid/comm, shown in top(1)) is the same
#  - the dynamic linker is taken from this package rather than the executable's
#    default (which is hardcoded to point to /lib64/ld-linux-x86_64.so or similar)
#  - LD_LIBRARY_PATH points to the lib/ directory so shared library dependencies
#    are satisified from there rather than the system default (e.g. /lib64)

# To do that, the dynamic linker is invoked using a symbolic link named after the
# executable, not its standard name. We use "bash -a" to set argv[0].

# The full tangled web looks like:
#
# foobar/bin/scylla               a shell script invoking everything
# foobar/libexec/scylla.bin       the real binary
# foobar/libexec/scylla           a symlink to ../lib/ld.so
# foobar/lib/ld.so                the dynamic linker
# foobar/lib/lib...               all the other libraries

# the transformations (done by the thunk and symlinks) are:
#
#    bin/scylla args -> libexec/scylla libexec/scylla.bin args -> lib/ld.so libexec/scylla.bin args

thunk = b'''\
#!/bin/bash

x="$(readlink -f "$0")"
b="$(basename "$x")"
d="$(dirname "$x")/.."
ldso="$d/libexec/$b"
realexe="$d/libexec/$b.bin"
LD_LIBRARY_PATH="$d/lib" exec -a "$0" "$ldso" "$realexe" "$@"
'''

for exe in executables:
    basename = os.path.basename(exe)
    ar.add(exe, arcname='libexec/' + basename + '.bin')
    ti = tarfile.TarInfo(name='bin/' + basename)
    ti.size = len(thunk)
    ti.mode = 0o755
    ti.mtime = os.stat(exe).st_mtime
    ar.addfile(ti, fileobj=io.BytesIO(thunk))
    ti = tarfile.TarInfo(name='libexec/' + basename)
    ti.type = tarfile.SYMTYPE
    ti.linkname = '../lib/ld.so'
    ti.mtime = os.stat(exe).st_mtime
    ar.addfile(ti)
for lib, libfile in libs.items():
    ar.add(libfile, arcname='lib/' + lib)
ar.add('conf')
ar.add('dist')
