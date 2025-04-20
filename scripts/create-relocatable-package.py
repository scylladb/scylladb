#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import argparse
import os
import subprocess
import tarfile
import pathlib
import shutil
import sys
import tempfile
import magic
from tempfile import mkstemp


RELOC_PREFIX='scylla'
def reloc_add(self, name, arcname=None, recursive=True, *, filter=None):
    if arcname:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, arcname),
                        filter=filter)
    else:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, name),
                        filter=filter)

tarfile.TarFile.reloc_add = reloc_add

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
            raise Exception('ldd {}: could not resolve {}'.format(executable, elements[0]))
        if elements[1] != '=>':
            if elements[0].startswith('linux-vdso.so'):
                # provided by kernel
                continue
            libraries['ld.so'] = os.path.realpath(elements[0])
        elif '//' in elements[0]:
            # We know that the only DSO with a // in the path is the
            # dynamic linker used by scylla, which is the same ld.so
            # above.
            pass
        else:
            libraries[elements[0]] = os.path.realpath(elements[2])
            hmacfile = elements[2].replace('/lib64/', '/lib64/.') + '.hmac'
            if os.path.exists(hmacfile):
                arcname = os.path.basename(hmacfile)
                libraries[arcname] = os.path.realpath(hmacfile)
            fc_hmacfile = elements[2].replace('/lib64/', '/lib64/fipscheck/') + '.hmac'
            if os.path.exists(fc_hmacfile):
                arcname = 'fipscheck/' + os.path.basename(fc_hmacfile)
                libraries[arcname] = os.path.realpath(fc_hmacfile)
    return libraries

def filter_dist(info):
    for x in ['dist/ami/files/', 'dist/ami/packer', 'dist/ami/variables.json']:
        if info.name.startswith(x):
            return None
    return info

SCYLLA_DIR='scylla-package'
def reloc_add(ar, name, arcname=None):
    ar.add(name, arcname="{}/{}".format(SCYLLA_DIR, arcname if arcname else name))

def fipshmac(f):
    DIRECTORY='build'
    bn = os.path.basename(f)
    subprocess.run(['fipshmac', '-d', DIRECTORY, f], check=True)
    return f'{DIRECTORY}/{bn}.hmac'

def fix_hmac(ar, binpath, targetpath, patched_binary):
    bn = os.path.basename(binpath)
    dn = os.path.dirname(binpath)
    targetpath_bn = os.path.basename(targetpath)
    targetpath_dn = os.path.dirname(targetpath)
    hmac = f'{dn}/.{bn}.hmac'
    if os.path.exists(hmac):
        hmac = fipshmac(patched_binary)
        hmac_arcname = f'{targetpath_dn}/.{targetpath_bn}.hmac'
        ar.reloc_add(hmac, arcname=hmac_arcname)
    fc_hmac = f'{dn}/fipscheck/{bn}.hmac'
    if os.path.exists(fc_hmac):
        fc_hmac = fipshmac(patched_binary)
        fc_hmac_arcname = f'{targetpath_dn}/fipscheck/{targetpath_bn}.hmac'
        ar.reloc_add(fc_hmac, arcname=fc_hmac_arcname)

def fix_binary(ar, path):
    # it's a pity patchelf have to patch an actual binary.
    patched_elf = mkstemp()[1]
    shutil.copy2(path, patched_elf)

    subprocess.check_call(['patchelf',
                           '--remove-rpath',
                           patched_elf])
    return patched_elf

def fix_executable(ar, binpath, targetpath):
    patched_binary = fix_binary(ar, binpath)
    ar.reloc_add(patched_binary, arcname=targetpath)
    os.remove(patched_binary)

def fix_sharedlib(ar, binpath, targetpath):
    patched_binary = fix_binary(ar, binpath)
    ar.reloc_add(patched_binary, arcname=targetpath)
    fix_hmac(ar, binpath, targetpath, patched_binary)
    os.remove(patched_binary)

ap = argparse.ArgumentParser(description='Create a relocatable scylla package.')
ap.add_argument('dest',
                help='Destination file (tar format)')
ap.add_argument('--build-dir', default='build/release',
                help='Build dir ("build/debug" or "build/release") to use')
ap.add_argument('--node-exporter-dir', default='build/node_exporter',
                help='the directory where node_exporter is located')
ap.add_argument('--debian-dir', default='build/debian/debian',
                help='the directory where debian packaging is located')
ap.add_argument('--stripped', action='store_true',
                help='use stripped binaries')
ap.add_argument('--print-libexec', action='store_true',
                help='print libexec executables and exit script')

args = ap.parse_args()

executables_scylla = [
                '{}/scylla'.format(args.build_dir),
                '{}/iotune'.format(args.build_dir)]
executables_distrocmd = [
                '/usr/bin/patchelf',
                '/usr/bin/lscpu',
                '/usr/bin/gawk',
                '/usr/bin/gzip',
                '/usr/sbin/ifconfig',
                '/usr/sbin/ethtool',
                '/usr/bin/netstat',
                '/usr/bin/hwloc-distrib',
                '/usr/bin/hwloc-calc',
                '/usr/bin/lsblk']

executables = executables_scylla + executables_distrocmd

if args.print_libexec:
    for exec in executables:
        print(f'libexec/{os.path.basename(exec)}')
    sys.exit(0)

output = args.dest

libs = {}
for exe in executables:
    libs.update(ldd(exe))

# manually add libthread_db for debugging thread
libs.update({'libthread_db.so.1': os.path.realpath('/lib64/libthread_db.so')})
# manually add p11-kit-trust.so since it will dynamically load
libs.update({'pkcs11/p11-kit-trust.so': '/lib64/pkcs11/p11-kit-trust.so'})

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
# relocatable package format version = 3.0
with tempfile.NamedTemporaryFile('w+t') as version_file:
    version_file.write('3.0\n')
    version_file.flush()
    ar.add(version_file.name, arcname='.relocatable_package_version')

with tempfile.TemporaryDirectory() as tmpdir:
    os.symlink('./pkcs11/p11-kit-trust.so', f'{tmpdir}/libnssckbi.so')
    ar.reloc_add(f'{tmpdir}/libnssckbi.so', arcname='libreloc/libnssckbi.so')

for exe in executables_scylla:
    basename = os.path.basename(exe)
    if not args.stripped:
        fix_executable(ar, exe, f'libexec/{basename}')
    else:
        fix_executable(ar, f'{exe}.stripped', f'libexec/{basename}')
for exe in executables_distrocmd:
    basename = os.path.basename(exe)
    fix_executable(ar, exe, f'libexec/{basename}')

for lib, libfile in libs.items():
    m = magic.detect_from_filename(libfile)
    if m and (m.mime_type.startswith('application/x-sharedlib') or m.mime_type.startswith('application/x-pie-executable')):
        fix_sharedlib(ar, libfile, f'libreloc/{lib}')
    else:
        ar.reloc_add(libfile, arcname=lib, recursive=False)
if have_gnutls:
    gnutls_config_nolink = os.path.realpath('/etc/crypto-policies/back-ends/gnutls.config')
    ar.reloc_add(gnutls_config_nolink, arcname='libreloc/gnutls.config')
    ar.reloc_add('conf')
ar.reloc_add('dist', filter=filter_dist)
with tempfile.NamedTemporaryFile('w') as relocatable_file:
    ar.reloc_add(relocatable_file.name, arcname='SCYLLA-RELOCATABLE-FILE')
version_dir = pathlib.Path(args.build_dir)
if not (version_dir / 'SCYLLA-RELEASE-FILE').exists():
    version_dir = version_dir.parent
ar.reloc_add(version_dir / 'SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.reloc_add(version_dir / 'SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.reloc_add(version_dir / 'SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.reloc_add('seastar/scripts')
ar.reloc_add('seastar/dpdk/usertools')
ar.reloc_add('install.sh')
# scylla_post_install.sh lives at the top level together with install.sh in the src tree, but while install.sh is
# not distributed in the .rpm and .deb packages, scylla_post_install is, so we'll add it in the package
# together with the other scripts that will end up in /usr/lib/scylla
ar.reloc_add('scylla_post_install.sh', arcname="dist/common/scripts/scylla_post_install.sh")
ar.reloc_add('README.md')
ar.reloc_add('NOTICE.txt')
ar.reloc_add('ORIGIN')
ar.reloc_add('licenses')
ar.reloc_add('swagger-ui')
ar.reloc_add('api')
ar.reloc_add('tools/scyllatop')
ar.reloc_add('scylla-gdb.py')
ar.reloc_add('bin/nodetool')
ar.reloc_add(args.debian_dir, arcname='debian')
node_exporter_dir = args.node_exporter_dir
if args.stripped:
    ar.reloc_add(f'{node_exporter_dir}', arcname='node_exporter')
    ar.reloc_add(f'{node_exporter_dir}/node_exporter.stripped', arcname='node_exporter/node_exporter')
else:
    ar.reloc_add(f'{node_exporter_dir}/node_exporter', arcname='node_exporter/node_exporter')
ar.reloc_add(f'{node_exporter_dir}/LICENSE', arcname='node_exporter/LICENSE')
ar.reloc_add(f'{node_exporter_dir}/NOTICE', arcname='node_exporter/NOTICE')
ar.reloc_add('ubsan-suppressions.supp')
ar.reloc_add('fix_system_distributed_tables.py')

# Complete the tar output, and wait for the gzip process to complete
ar.close()
gzip_process.communicate()
if gzip_process.returncode != 0:
    print(f'pigz returned {gzip_process.returncode}!', file=sys.stderr)
    sys.exit(1)
