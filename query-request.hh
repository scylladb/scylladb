
/*
 * Copyright (C) 2015-present ScyllaDB
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

#pragma once

#include <optional>

#include "keys.hh"
#include "dht/i_partitioner.hh"
#include "enum_set.hh"
#include "range.hh"
#include "tracing/tracing.hh"
#include "utils/small_vector.hh"
#include "query_class_config.hh"

class position_in_partition_view;

namespace query {

using column_id_vector = utils::small_vector<column_id, 8>;

template <typename T>
using range = wrapping_range<T>;

using ring_position = dht::ring_position;
using clustering_range = nonwrapping_range<clustering_key_prefix>;

extern const dht::partition_range full_partition_range;
extern const clustering_range full_clustering_range;

inline
bool is_single_partition(const dht::partition_range& range) {
    return range.is_singular() && range.start()->value().has_key();
}

inline
bool is_single_row(const schema& s, const query::clustering_range& range) {
    return range.is_singular() && range.start()->value().is_full(s);
}

typedef std::vector<clustering_range> clustering_row_ranges;

/// Trim the clustering ranges.
///
/// Equivalent of intersecting each clustering range with [pos, +inf) position
/// in partition range, or (-inf, pos] position in partition range if
/// reversed == true. Ranges that do not intersect are dropped. Ranges that
/// partially overlap are trimmed.
/// Result: each range will overlap fully with [pos, +inf), or (-int, pos] if
/// reversed is true.
void trim_clustering_row_ranges_to(const schema& s, clustering_row_ranges& ranges, position_in_partition_view pos, bool reversed = false);

/// Trim the clustering ranges.
///
/// Equivalent of intersecting each clustering range with (key, +inf) clustering
/// range, or (-inf, key) clustering range if reversed == true. Ranges that do
/// not intersect are dropped. Ranges that partially overlap are trimmed.
/// Result: each range will overlap fully with (key, +inf), or (-int, key) if
/// reversed is true.
void trim_clustering_row_ranges_to(const schema& s, clustering_row_ranges& ranges, const clustering_key& key, bool reversed = false);

class specific_ranges {
public:
    specific_ranges(partition_key pk, clustering_row_ranges ranges)
            : _pk(std::move(pk)), _ranges(std::move(ranges)) {
    }
    specific_ranges(const specific_ranges&) = default;

    void add(const schema& s, partition_key pk, clustering_row_ranges ranges) {
        if (!_pk.equal(s, pk)) {
            throw std::runtime_error("Only single specific range supported currently");
        }
        _pk = std::move(pk);
        _ranges = std::move(ranges);
    }
    bool contains(const schema& s, const partition_key& pk) {
        return _pk.equal(s, pk);
    }
    size_t size() const {
        return 1;
    }
    const clustering_row_ranges* range_for(const schema& s, const partition_key& key) const {
        if (_pk.equal(s, key)) {
            return &_ranges;
        }
        return nullptr;
    }
    const partition_key& pk() const {
        return _pk;
    }
    const clustering_row_ranges& ranges() const {
        return _ranges;
    }
private:
    friend std::ostream& operator<<(std::ostream& out, const specific_ranges& r);

    partition_key _pk;
    clustering_row_ranges _ranges;
};

constexpr auto max_rows = std::numeric_limits<uint64_t>::max();
constexpr auto partition_max_rows = std::numeric_limits<uint64_t>::max();
constexpr auto max_rows_if_set = std::numeric_limits<uint32_t>::max();

// Specifies subset of rows, columns and cell attributes to be returned in a query.
// Can be accessed across cores.
// Schema-dependent.
class partition_slice {
public:
    enum class option {
        send_clustering_key,
        send_partition_key,
        send_timestamp,
        send_expiry,
        reversed,
        distinct,
        collections_as_maps,
        send_ttl,
        allow_short_read,
        with_digest,
        bypass_cache,
        // Normally, we don't return static row if the request has clustering
        // key restrictions and the partition doesn't have any rows matching
        // the restrictions, see #589. This flag overrides this behavior.
        always_return_static_content,
        // Use the new data range scan variant, which builds query::result
        // directly, bypassing the intermediate reconcilable_result format used
        // in pre 4.5 range scans.
        range_scan_data_variant,
    };
    using option_set = enum_set<super_enum<option,
        option::send_clustering_key,
        option::send_partition_key,
        option::send_timestamp,
        option::send_expiry,
        option::reversed,
        option::distinct,
        option::collections_as_maps,
        option::send_ttl,
        option::allow_short_read,
        option::with_digest,
        option::bypass_cache,
        option::always_return_static_content,
        option::range_scan_data_variant>>;
    clustering_row_ranges _row_ranges;
public:
    column_id_vector static_columns; // TODO: consider using bitmap
    column_id_vector regular_columns;  // TODO: consider using bitmap
    option_set options;
private:
    std::unique_ptr<specific_ranges> _specific_ranges;
    cql_serialization_format _cql_format;
    uint32_t _partition_row_limit_low_bits;
    uint32_t _partition_row_limit_high_bits;
public:
    partition_slice(clustering_row_ranges row_ranges, column_id_vector static_columns,
        column_id_vector regular_columns, option_set options,
        std::unique_ptr<specific_ranges> specific_ranges,
        cql_serialization_format,
        uint32_t partition_row_limit_low_bits,
        uint32_t partition_row_limit_high_bits);
    partition_slice(clustering_row_ranges row_ranges, column_id_vector static_columns,
        column_id_vector regular_columns, option_set options,
        std::unique_ptr<specific_ranges> specific_ranges = nullptr,
        cql_serialization_format = cql_serialization_format::internal(),
        uint64_t partition_row_limit = partition_max_rows);
    partition_slice(clustering_row_ranges ranges, const schema& schema, const column_set& mask, option_set options);
    partition_slice(const partition_slice&);
    partition_slice(partition_slice&&);
    ~partition_slice();

    partition_slice& operator=(partition_slice&& other) noexcept;

    const clustering_row_ranges& row_ranges(const schema&, const partition_key&) const;
    void set_range(const schema&, const partition_key&, clustering_row_ranges);
    void clear_range(const schema&, const partition_key&);
    void clear_ranges() {
        _specific_ranges = nullptr;
    }
    // FIXME: possibly make this function return a const ref instead.
    clustering_row_ranges get_all_ranges() const;

    const clustering_row_ranges& default_row_ranges() const {
        return _row_ranges;
    }
    const std::unique_ptr<specific_ranges>& get_specific_ranges() const {
        return _specific_ranges;
    }
    const cql_serialization_format& cql_format() const {
        return _cql_format;
    }
    const uint32_t partition_row_limit_low_bits() const {
        return _partition_row_limit_low_bits;
    }
    const uint32_t partition_row_limit_high_bits() const {
        return _partition_row_limit_high_bits;
    }
    const uint64_t partition_row_limit() const {
        return (static_cast<uint64_t>(_partition_row_limit_high_bits) << 32) | _partition_row_limit_low_bits;
    }
    void set_partition_row_limit(uint64_t limit) {
        _partition_row_limit_low_bits = static_cast<uint64_t>(limit);
        _partition_row_limit_high_bits = static_cast<uint64_t>(limit >> 32);
    }

    friend std::ostream& operator<<(std::ostream& out, const partition_slice& ps);
    friend std::ostream& operator<<(std::ostream& out, const specific_ranges& ps);
};

constexpr auto max_partitions = std::numeric_limits<uint32_t>::max();

// Tagged integers to disambiguate constructor arguments.
enum class row_limit : uint64_t { max = max_rows };
enum class partition_limit : uint32_t { max = max_partitions };

using is_first_page = bool_class<class is_first_page_tag>;

// Full specification of a query to the database.
// Intended for passing across replicas.
// Can be accessed across cores.
class read_command {
public:
    utils::UUID cf_id;
    table_schema_version schema_version; // TODO: This should be enough, drop cf_id
    partition_slice slice;
    uint32_t row_limit_low_bits;
    gc_clock::time_point timestamp;
    std::optional<tracing::trace_info> trace_info;
    uint32_t partition_limit; // The maximum number of live partitions to return.
    // The "query_uuid" field is useful in pages queries: It tells the replica
    // that when it finishes the read request prematurely, i.e., reached the
    // desired number of rows per page, it should not destroy the reader object,
    // rather it should keep it alive - at its current position - and save it
    // under the unique key "query_uuid". Later, when we want to resume
    // the read at exactly the same position (i.e., to request the next page)
    // we can pass this same unique id in that query's "query_uuid" field.
    utils::UUID query_uuid;
    // Signal to the replica that this is the first page of a (maybe) paged
    // read request as far the replica is concerned. Can be used by the replica
    // to avoid doing work normally done on paged requests, e.g. attempting to
    // reused suspended readers.
    query::is_first_page is_first_page;
    // The maximum size of the query result, for all queries.
    // We use the entire value range, so we need an optional for the case when
    // the remote doesn't send it.
    std::optional<query::max_result_size> max_result_size;
    uint32_t row_limit_high_bits;
    api::timestamp_type read_timestamp; // not serialized
public:
    // IDL constructor
    read_command(utils::UUID cf_id,
                 table_schema_version schema_version,
                 partition_slice slice,
                 uint32_t row_limit_low_bits,
                 gc_clock::time_point now,
                 std::optional<tracing::trace_info> ti,
                 uint32_t partition_limit,
                 utils::UUID query_uuid,
                 query::is_first_page is_first_page,
                 std::optional<query::max_result_size> max_result_size,
                 uint32_t row_limit_high_bits)
        : cf_id(std::move(cf_id))
        , schema_version(std::move(schema_version))
        , slice(std::move(slice))
        , row_limit_low_bits(row_limit_low_bits)
        , timestamp(now)
        , trace_info(std::move(ti))
        , partition_limit(partition_limit)
        , query_uuid(query_uuid)
        , is_first_page(is_first_page)
        , max_result_size(max_result_size)
        , row_limit_high_bits(row_limit_high_bits)
        , read_timestamp(api::new_timestamp())
    { }

    read_command(utils::UUID cf_id,
            table_schema_version schema_version,
            partition_slice slice,
            query::max_result_size max_result_size,
            query::row_limit row_limit = query::row_limit::max,
            query::partition_limit partition_limit = query::partition_limit::max,
            gc_clock::time_point now = gc_clock::now(),
            std::optional<tracing::trace_info> ti = std::nullopt,
            utils::UUID query_uuid = utils::UUID(),
            query::is_first_page is_first_page = query::is_first_page::no,
            api::timestamp_type rt = api::new_timestamp())
        : cf_id(std::move(cf_id))
        , schema_version(std::move(schema_version))
        , slice(std::move(slice))
        , row_limit_low_bits(static_cast<uint32_t>(row_limit))
        , timestamp(now)
        , trace_info(std::move(ti))
        , partition_limit(static_cast<uint32_t>(partition_limit))
        , query_uuid(query_uuid)
        , is_first_page(is_first_page)
        , max_result_size(max_result_size)
        , row_limit_high_bits(static_cast<uint32_t>(static_cast<uint64_t>(row_limit) >> 32))
        , read_timestamp(rt)
    { }


    uint64_t get_row_limit() const {
        return (static_cast<uint64_t>(row_limit_high_bits) << 32) | row_limit_low_bits;
    }
    void set_row_limit(uint64_t new_row_limit) {
        row_limit_low_bits = static_cast<uint32_t>(new_row_limit);
        row_limit_high_bits = static_cast<uint32_t>(new_row_limit >> 32);
    }
    friend std::ostream& operator<<(std::ostream& out, const read_command& r);
};

}
