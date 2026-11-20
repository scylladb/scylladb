#!/usr/bin/env python3

#
# Copyright (C) 2024 ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import argparse
import tarfile
import pathlib


def erase_uid(tarinfo):
    tarinfo.uid = tarinfo.gid = 0
    tarinfo.uname = tarinfo.gname = 'root'
    return tarinfo

RELOC_PREFIX='scylla-cqlsh'
def reloc_add(self, name, arcname=None):
    if arcname:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, arcname), filter=erase_uid)
    else:
        return self.add(name, arcname="{}/{}".format(RELOC_PREFIX, name), filter=erase_uid)

tarfile.TarFile.reloc_add = reloc_add


ap = argparse.ArgumentParser(description='Create a relocatable scylla-cqlsh package.')
ap.add_argument('--version', required=True,
                help='Tools version')
ap.add_argument('--binary', required=True,
                help='Path to the cqlsh-rs binary')
ap.add_argument('dest',
                help='Destination file (tar format)')

args = ap.parse_args()

output = args.dest

ar = tarfile.open(output, mode='w|gz')
# relocatable package format version = 2
with open('build/.relocatable_package_version', 'w') as f:
    f.write('2\n')
ar.add('build/.relocatable_package_version', arcname='.relocatable_package_version', filter=erase_uid)

pathlib.Path('build/SCYLLA-RELOCATABLE-FILE').touch()
ar.reloc_add('build/SCYLLA-RELOCATABLE-FILE', arcname='SCYLLA-RELOCATABLE-FILE')
ar.reloc_add('build/SCYLLA-RELEASE-FILE', arcname='SCYLLA-RELEASE-FILE')
ar.reloc_add('build/SCYLLA-VERSION-FILE', arcname='SCYLLA-VERSION-FILE')
ar.reloc_add('build/SCYLLA-PRODUCT-FILE', arcname='SCYLLA-PRODUCT-FILE')
ar.reloc_add('dist/debian')
ar.reloc_add('dist/redhat')
ar.reloc_add(args.binary, arcname='bin/cqlsh-rs')
ar.reloc_add('install.sh')
ar.reloc_add('build/debian/debian', arcname='debian')
