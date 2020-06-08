#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 ScyllaDB
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

import string
import os
import glob
import shutil

class DebianFilesTemplate(string.Template):
    delimiter = '%'

scriptdir = os.path.dirname(__file__)

with open(os.path.join(scriptdir, 'changelog.template')) as f:
    changelog_template = f.read()

with open(os.path.join(scriptdir, 'control.template')) as f:
    control_template = f.read()

with open('build/SCYLLA-PRODUCT-FILE') as f:
    product = f.read().strip()

with open('build/SCYLLA-VERSION-FILE') as f:
    version = f.read().strip().replace('.rc', '~rc').replace('_', '-')

with open('build/SCYLLA-RELEASE-FILE') as f:
    release = f.read().strip()

shutil.rmtree('build/debian/debian', ignore_errors=True)
shutil.copytree('dist/debian/debian', 'build/debian/debian')

if product != 'scylla':
    for p in glob.glob('build/debian/debian/scylla-*'):
        shutil.move(p, p.replace('scylla-', '{}-'.format(product)))

shutil.copy('dist/common/sysconfig/scylla-server', 'build/debian/debian/{}-server.default'.format(product))
if product != 'scylla':
    shutil.copy('dist/common/systemd/scylla-server.service', 'build/debian/debian/{}-server.scylla-server.service'.format(product))
else:
    shutil.copy('dist/common/systemd/scylla-server.service', 'build/debian/debian/scylla-server.service')
shutil.copy('dist/common/systemd/scylla-housekeeping-daily.service', 'build/debian/debian/{}-server.scylla-housekeeping-daily.service'.format(product))
shutil.copy('dist/common/systemd/scylla-housekeeping-restart.service', 'build/debian/debian/{}-server.scylla-housekeeping-restart.service'.format(product))
shutil.copy('dist/common/systemd/scylla-fstrim.service', 'build/debian/debian/{}-server.scylla-fstrim.service'.format(product))
shutil.copy('dist/common/systemd/node-exporter.service', 'build/debian/debian/{}-server.node-exporter.service'.format(product))

s = DebianFilesTemplate(changelog_template)
changelog_applied = s.substitute(product=product, version=version, release=release, revision='1', codename='stable')

s = DebianFilesTemplate(control_template)
control_applied = s.substitute(product=product)

with open('build/debian/debian/changelog', 'w') as f:
    f.write(changelog_applied)

with open('build/debian/debian/control', 'w') as f:
    f.write(control_applied)

