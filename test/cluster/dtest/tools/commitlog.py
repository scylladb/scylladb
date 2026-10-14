#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os
import shutil


def cleanup(commitlog_dir):
    for f in os.listdir(commitlog_dir):
        file_path = os.path.join(commitlog_dir, f)
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            shutil.rmtree(file_path)


def list_files(commitlog_dir):
    return [f for f in os.listdir(commitlog_dir) if os.path.isfile(os.path.join(commitlog_dir, f))]
