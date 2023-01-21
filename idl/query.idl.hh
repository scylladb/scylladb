/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "idl/keys.idl.hh"

namespace query {

class qr_cell stub [[writable]] {
    std::optional<api::timestamp_type> timestamp; // present when send_timestamp option set in partition_slice
    std::optional<gc_clock::time_point> expiry; // present when send_expiry option set in partition_slice

    // Specified by CQL binary protocol, according to cql_serialization_format in read_command.
    bytes value;

    std::optional<gc_clock::duration> ttl [[version 1.3]]; // present when send_ttl option set in partition_slice
};

class qr_row stub [[writable]] {
    std::vector<std::optional<qr_cell>> cells; // ordered as requested in partition_slice
};

class qr_clustered_row stub [[writable]] {
    std::optional<clustering_key> key; // present when send_clustering_key option set in partition_slice
    qr_row cells; // ordered as requested in partition_slice
};

class qr_partition stub [[writable]] {
    std::optional<partition_key> key; // present when send_partition_key option set in partition_slice
    qr_row static_row;
    utils::chunked_vector<qr_clustered_row> rows; // ordered by key
};

class query_result stub [[writable]] {
    utils::chunked_vector<qr_partition> partitions; // in ring order
};

enum class digest_algorithm : uint8_t {
    none = 0,  // digest not required
    MD5 = 1,
    xxHash = 2,// default algorithm
};

}
