#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
import pathlib


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
               'build/{}/iotune'.format(args.mode),
               '/usr/bin/lscpu',
               '/usr/bin/gawk',
               '/usr/bin/gzip',
               '/usr/sbin/ifconfig',
               '/usr/sbin/ethtool',
               '/usr/bin/netstat',
               '/usr/bin/hwloc-distrib',
               '/usr/bin/hwloc-calc']

output = args.dest

libs = {}
for exe in executables:
    libs.update(ldd(exe))

ld_so = libs['ld.so']

have_gnutls = any([lib.startswith('libgnutls.so')
                   for lib in libs.keys()])

# Although tarfile.open() can write directly to a compressed tar by using
# the "w|gz" mode, it does so using a slow Python implementation. It is as
# much as 3 times faster (!) to output to a pipe running the external gzip
# command. We can complete the compression even faster by using the pigz
# command - a parallel implementation of gzip utilizing all processors
# instead of just one.
gzip_process = subprocess.Popen("pigz > "+output, shell=True, stdin=subprocess.PIPE)

ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
pathlib.Path('build/SCYLLA-RELOCATABLE-FILE').touch()
ar.add('build/SCYLLA-RELOCATABLE-FILE', arcname='SCYLLA-RELOCATABLE-FILE')

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
# foobar/libreloc/ld.so                the dynamic linker
# foobar/libreloc/lib...               all the other libraries

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
export GNUTLS_SYSTEM_PRIORITY_FILE="${GNUTLS_SYSTEM_PRIORITY_FILE-$d/libreloc/gnutls.config}"
LD_LIBRARY_PATH="$d/libreloc" exec -a "$0" "$ldso" "$realexe" "$@"
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
    ti.linkname = '../libreloc/ld.so'
    ti.mtime = os.stat(exe).st_mtime
    ar.addfile(ti)
for lib, libfile in libs.items():
    ar.add(libfile, arcname='libreloc/' + lib)
if have_gnutls:
    gnutls_config_nolink = os.path.realpath('/etc/crypto-policies/back-ends/gnutls.config')
    ar.add(gnutls_config_nolink, arcname='libreloc/gnutls.config')
ar.add('conf')
ar.add('dist')
ar.add('build/SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.add('build/SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.add('build/SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.add('seastar/scripts')
ar.add('seastar/dpdk/usertools')
ar.add('install.sh')
ar.add('scripts/relocate_python_scripts.py', arcname='relocate_python_scripts.py')
ar.add('README.md')
ar.add('README-DPDK.md')
ar.add('NOTICE.txt')
ar.add('ORIGIN')
ar.add('licenses')
ar.add('swagger-ui')
ar.add('api')
ar.add('tools')
ar.add('scylla-gdb.py')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
