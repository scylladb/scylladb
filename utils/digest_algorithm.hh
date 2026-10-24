/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

namespace query {

enum class digest_algorithm : uint8_t {
    none = 0,  // digest not required
    xxHash = 3, // default algorithm
    // Like xxHash, but the digest covers only the partitions of the result.
    // xxHash also covers the key of a partition which the result omits, if
    // the replica read some of its tombstones. When the query selects no
    // static column, a replica which returns a partition's static-only row
    // and one which returns nothing from the partition then have the same
    // digest, so the coordinator does not notice that they disagree. The
    // READ_FRONTIERS cluster feature gates it, because a digest is only
    // comparable with digests of the same algorithm.
    xxHash_without_empty_partitions = 4,
};

// Whether the digest of `algo` omits the partitions which the result omits.
inline bool digest_omits_empty_partitions(digest_algorithm algo) {
    return algo == digest_algorithm::xxHash_without_empty_partitions;
}

}
