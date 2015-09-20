
/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <experimental/optional>
#include "keys.hh"
#include "dht/i_partitioner.hh"
#include "enum_set.hh"
#include "range.hh"

namespace query {

template <typename T>
using range = ::range<T>;

using ring_position = dht::ring_position;
using partition_range = range<ring_position>;
using clustering_range = range<clustering_key_prefix>;

extern const partition_range full_partition_range;

// FIXME: Move this to i_partitioner.hh after query::range<> is moved to utils/range.hh
query::partition_range to_partition_range(query::range<dht::token>);

inline
bool is_wrap_around(const query::partition_range& range, const schema& s) {
    return range.is_wrap_around(dht::ring_position_comparator(s));
}

inline
bool is_single_partition(const query::partition_range& range) {
    return range.is_singular() && range.start()->value().has_key();
}

// Specifies subset of rows, columns and cell attributes to be returned in a query.
// Can be accessed across cores.
class partition_slice {
public:
    enum class option { send_clustering_key, send_partition_key, send_timestamp_and_expiry, reversed, distinct };
    using option_set = enum_set<super_enum<option,
        option::send_clustering_key,
        option::send_partition_key,
        option::send_timestamp_and_expiry,
        option::reversed,
        option::distinct>>;
public:
    std::vector<clustering_range> row_ranges;
    std::vector<column_id> static_columns; // TODO: consider using bitmap
    std::vector<column_id> regular_columns;  // TODO: consider using bitmap
    option_set options;
public:
    partition_slice(std::vector<clustering_range> row_ranges, std::vector<column_id> static_columns,
        std::vector<column_id> regular_columns, option_set options)
        : row_ranges(std::move(row_ranges))
        , static_columns(std::move(static_columns))
        , regular_columns(std::move(regular_columns))
        , options(options)
    { }
    friend std::ostream& operator<<(std::ostream& out, const partition_slice& ps);
};

constexpr auto max_rows = std::numeric_limits<uint32_t>::max();

// Full specification of a query to the database.
// Intended for passing across replicas.
// Can be accessed across cores.
class read_command {
public:
    utils::UUID cf_id;
    partition_slice slice;
    uint32_t row_limit;
    gc_clock::time_point timestamp;
public:
    read_command(const utils::UUID& cf_id, partition_slice slice, uint32_t row_limit = max_rows, gc_clock::time_point now = gc_clock::now())
        : cf_id(cf_id)
        , slice(std::move(slice))
        , row_limit(row_limit)
        , timestamp(now)
    { }

    size_t serialized_size() const;
    void serialize(bytes::iterator& out) const;
    static read_command deserialize(bytes_view& v);

    friend std::ostream& operator<<(std::ostream& out, const read_command& r);
};

}

// Allow using query::range<T> in a hash table. The hash function 31 * left +
// right is the same one used by Cassandra's AbstractBounds.hashCode().
namespace std {
template<typename T>
struct hash<query::range<T>> {
    using argument_type =  query::range<T>;
    using result_type = decltype(std::hash<T>()(std::declval<T>()));
    result_type operator()(argument_type const& s) const {
        auto hash = std::hash<T>();
        auto left = s.start() ? hash(s.start()->value()) : 0;
        auto right = s.end() ? hash(s.end()->value()) : 0;
        return 31 * left + right;
    }
};
}
