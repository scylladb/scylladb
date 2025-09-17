/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "dht/decorated_key.hh"
#include "keys/keys.hh"

namespace cql3 {

namespace statements {

/// Encapsulates a partition key and clustering key prefix as a primary key.
struct primary_key {
    dht::decorated_key partition;
    clustering_key_prefix clustering;
};

} // namespace statements

} // namespace cql3
