#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2016-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import os

from scylla_util import parse_scylla_dirs_with_default


# try to write data to a sysfs path, expect problems
def try_write(path, data):
    try:
        open(path, 'w').write(data)
    except Exception:
        print("warning: unable to tune {} to {}".format(path, data))


# update a sysfs path if it does not satisfy a check
# function (default = check that the data is already there)
def tune_path(path, data, check=None):
    def default_check(current):
        return current == data
    if check is None:
        check = default_check
    if not os.path.exists(path):
        return
    if check(open(path).read().strip()):
        print('already tuned: {}'.format(path))
        return
    print('tuning: {} {}'.format(path, data))
    try_write(path, data + '\n')


tuned_blockdevs = set()


# tune a blockdevice (sysfs node); updates I/O scheduler
# and merge behavior.  Tunes dependent devices
def tune_blockdev(path, nomerges):
    from os.path import join, exists, dirname, realpath
    path = realpath(path)
    print('tuning {}'.format(path))
    if path in tuned_blockdevs:
        return
    tuned_blockdevs.add(path)

    def check_sched(current):
        return current == 'none' or '[noop]' in current

    if not nomerges:
        tune_path(join(path, 'queue', 'scheduler'), 'noop', check_sched)
        tune_path(join(path, 'queue', 'nomerges'), '2')
    else:
        tune_path(join(path, 'queue', 'nomerges'), nomerges)
    slaves = join(path, 'slaves')
    if exists(slaves):
        for slave in os.listdir(slaves):
            tune_blockdev(join(slaves, slave), nomerges)
    if exists(join(path, 'partition')):
        tune_blockdev(dirname(path), nomerges)


# tunes a /dev/foo blockdev
def tune_dev(path, nomerges):
    dev = os.stat(path).st_rdev
    devfile = '/sys/dev/block/{}:{}'.format(dev // 256, dev % 256)
    tune_blockdev(devfile, nomerges)


# tunes a filesystem
def tune_fs(path, nomerges):
    dev = os.stat(path).st_dev
    devfile = '/sys/dev/block/{}:{}'.format(dev // 256, dev % 256)
    tune_blockdev(devfile, nomerges)


# tunes all filesystems referenced from a scylla.yaml
def tune_yaml(path, nomerges):
    y = parse_scylla_dirs_with_default(conf=path)
    for fs in y['data_file_directories']:
        tune_fs(fs, nomerges)
    tune_fs(y['commitlog_directory'], nomerges)
    tune_fs(y['schema_commitlog_directory'], nomerges)
