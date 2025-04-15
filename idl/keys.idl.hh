/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

class clustering_key_prefix {
    std::vector<bytes> explode();
};

class partition_key {
    std::vector<bytes> explode();
};
