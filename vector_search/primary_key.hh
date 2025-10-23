/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "dht/decorated_key.hh"
#include "keys/keys.hh"

namespace vector_search {

/// Encapsulates a partition key and clustering key prefix as a primary key.
struct primary_key {
    dht::decorated_key partition;
    clustering_key_prefix clustering;

    struct hashing {
        const schema& _schema;
        hashing(const schema& s)
            : _schema(s) {
        }

        size_t operator()(const primary_key& pk) const {
            size_t h1 = partition_key::hashing(_schema)(pk.partition.key());
            size_t h2 = clustering_key_prefix::hashing(_schema)(pk.clustering);
            return h1 ^ (h2 << 1);
        }
    };

    struct equality {
        const schema& _schema;
        equality(const schema& s)
            : _schema(s) {
        }

        bool operator()(const primary_key& pk1, const primary_key& pk2) const {
            return partition_key::equality(_schema)(pk1.partition.key(), pk2.partition.key()) &&
                   clustering_key_prefix::equality(_schema)(pk1.clustering, pk2.clustering);
        }
    };
};

} // namespace vector_search
