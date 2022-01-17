/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

class clustering_key_prefix {
    std::vector<bytes> explode();
};

class partition_key {
    std::vector<bytes> explode();
};
