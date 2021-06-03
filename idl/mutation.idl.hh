/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

class counter_id final {
    utils::UUID to_uuid();
};

class counter_shard final {
    counter_id id();
    int64_t value();
    int64_t logical_clock();
};

class counter_cell_full final stub [[writable]] {
    std::vector<counter_shard> shards;
};

class counter_cell_update final stub [[writable]] {
    int64_t delta;
};

class counter_cell stub [[writable]] {
    api::timestamp_type created_at;
    boost::variant<counter_cell_full, counter_cell_update> value;
};

class tombstone [[writable]] {
    api::timestamp_type timestamp;
    gc_clock::time_point deletion_time;
};

class live_cell stub [[writable]] {
    api::timestamp_type created_at;
    bytes value;
};

class expiring_cell stub [[writable]] {
    gc_clock::duration ttl;
    gc_clock::time_point expiry;
    live_cell c;
};

class dead_cell final stub [[writable]] {
    tombstone tomb;
};

class collection_element stub [[writable]] {
    // key's format depends on its CQL type as defined in the schema and is specified in CQL binary protocol.
    bytes key;
    boost::variant<live_cell, expiring_cell, dead_cell> value;
};

class collection_cell stub [[writable]] {
    tombstone tomb;
    std::vector<collection_element> elements; // sorted by key
};

class column stub [[writable]] {
    uint32_t id;
    boost::variant<boost::variant<live_cell, expiring_cell, dead_cell, counter_cell>, collection_cell> c;
};

class row stub [[writable]] {
    std::vector<column> columns; // sorted by id
};

class no_marker final stub [[writable]] {};

class live_marker stub [[writable]] {
    api::timestamp_type created_at;
};

class expiring_marker stub [[writable]] {
    live_marker lm;
    gc_clock::duration ttl;
    gc_clock::time_point expiry;
};

class dead_marker final stub [[writable]] {
    tombstone tomb;
};

class deletable_row stub [[writable]] {
    clustering_key key;
    boost::variant<live_marker, expiring_marker, dead_marker, no_marker> marker;
    tombstone deleted_at;
    row cells;
    tombstone shadowable_deleted_at [[version 1.8]] = deleted_at;
};

enum class bound_kind : uint8_t {
    excl_end,
    incl_start,
    incl_end,
    excl_start,
};

class range_tombstone [[writable]] {
    clustering_key_prefix start;
    tombstone tomb;
    bound_kind start_kind [[version 1.3]] = bound_kind::incl_start;
    clustering_key_prefix end [[version 1.3]] = start;
    bound_kind end_kind [[version 1.3]] = bound_kind::incl_end;
};

class mutation_partition stub [[writable]] {
    tombstone tomb;
    row static_row;
    std::vector<range_tombstone> range_tombstones; // sorted by key
    utils::chunked_vector<deletable_row> rows; // sorted by key

};

class mutation stub [[writable]] {
    utils::UUID table_id;
    utils::UUID schema_version;
    partition_key key;
    mutation_partition partition;
};

class column_mapping_entry {
    bytes name();
    sstring type_name();
};

class column_mapping {
    std::vector<column_mapping_entry> columns();
    uint32_t n_static();
};

class canonical_mutation stub [[writable]] {
    utils::UUID table_id;
    utils::UUID schema_version;
    partition_key key;
    column_mapping mapping;
    mutation_partition partition;
}

class clustering_row stub [[writable]] {
    deletable_row row;
};

class static_row stub [[writable]] {
    row cells;
};

class partition_start stub [[writable]] {
    partition_key key;
    tombstone partition_tombstone;
};

class partition_end {
};

class mutation_fragment stub [[writable]] {
    std::variant<clustering_row, static_row, range_tombstone,
                   partition_start, partition_end> fragment;
};

