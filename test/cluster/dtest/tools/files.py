#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
import random


def corrupt_file(file_path):
    """
    Corrupt a file by writing 1024 bytes of random data in 10 random locations in the file
    """
    with open(file_path, "r+b") as f:
        file_size = os.path.getsize(file_path)
        random_data = os.urandom(1024)
        for _ in range(10):
            random_position = random.randint(0, max(file_size - 1024, 0))
            f.seek(random_position)
            f.write(random_data)
