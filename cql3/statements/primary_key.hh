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
class primary_key {
public:
    dht::decorated_key partition;
    clustering_key_prefix clustering;

    primary_key(const dht::decorated_key& partition, const clustering_key_prefix& clustering)
        : partition(partition), clustering(clustering) {}

    bool operator<(const primary_key& other) const {
        auto partition_key = partition.key().explode();
        auto other_partition_key = other.partition.key().explode();

        if (partition_key < other_partition_key) {
            return true;
        }
        if (other_partition_key < partition_key) {
            return false;
        }
        return clustering.explode() < other.clustering.explode();
    }
};

} // namespace statements

} // namespace cql3
