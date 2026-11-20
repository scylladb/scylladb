#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import datetime
import string
import os
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
    version = f.read().strip()

with open('build/SCYLLA-RELEASE-FILE') as f:
    release = f.read().strip()

if os.path.exists('build/debian/debian'):
    shutil.rmtree('build/debian/debian')
shutil.copytree('dist/debian/debian', 'build/debian/debian')

s = DebianFilesTemplate(changelog_template)
now = datetime.datetime.now(tz=datetime.timezone.utc)
changelog_applied = s.substitute(product=product,
                                 version=version,
                                 release=release,
                                 revision='1',
                                 codename='stable',
                                 timestamp=now.strftime("%a, %d %b %Y %H:%M:%S %z"))

s = DebianFilesTemplate(control_template)
control_applied = s.substitute(product=product)

with open('build/debian/debian/changelog', 'w') as f:
    f.write(changelog_applied)

with open('build/debian/debian/control', 'w') as f:
    f.write(control_applied)
