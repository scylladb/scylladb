#
# Copyright (C) 2019 pengjian.uestc @ gmail.com
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import string
import random
import redis

def connect(host='localhost', port=6379):
    return redis.Redis(host, port, decode_responses=True)

def random_string(size=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(size))
