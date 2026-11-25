/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "keys/partition_key_serializer.hh"

class clustering_key_prefix {
    std::vector<bytes> explode();
};

