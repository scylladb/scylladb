
/*
 * Copyright 2015 Cloudius Systems
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

typedef std::vector<clustering_range> clustering_row_ranges;

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

// Specifies subset of rows, columns and cell attributes to be returned in a query.
// Can be accessed across cores.
// Schema-dependent.
class partition_slice {
public:
    enum class option { send_clustering_key, send_partition_key, send_timestamp, send_expiry, reversed, distinct };
    using option_set = enum_set<super_enum<option,
        option::send_clustering_key,
        option::send_partition_key,
        option::send_timestamp,
        option::send_expiry,
        option::reversed,
        option::distinct>>;
    clustering_row_ranges _row_ranges;
public:
    std::vector<column_id> static_columns; // TODO: consider using bitmap
    std::vector<column_id> regular_columns;  // TODO: consider using bitmap
    option_set options;
private:
    std::unique_ptr<specific_ranges> _specific_ranges;
public:
    partition_slice(clustering_row_ranges row_ranges, std::vector<column_id> static_columns,
        std::vector<column_id> regular_columns, option_set options,
        std::unique_ptr<specific_ranges> specific_ranges = nullptr);
    partition_slice(const partition_slice&);
    partition_slice(partition_slice&&);
    ~partition_slice();

    const clustering_row_ranges& row_ranges(const schema&, const partition_key&) const;
    void set_range(const schema&, const partition_key&, clustering_row_ranges);
    void clear_range(const schema&, const partition_key&);

    const clustering_row_ranges& default_row_ranges() const {
        return _row_ranges;
    }
    const std::unique_ptr<specific_ranges>& get_specific_ranges() const {
        return _specific_ranges;
    }

    friend std::ostream& operator<<(std::ostream& out, const partition_slice& ps);
    friend std::ostream& operator<<(std::ostream& out, const specific_ranges& ps);
};

constexpr auto max_rows = std::numeric_limits<uint32_t>::max();

// Full specification of a query to the database.
// Intended for passing across replicas.
// Can be accessed across cores.
class read_command {
public:
    utils::UUID cf_id;
    table_schema_version schema_version; // TODO: This should be enough, drop cf_id
    partition_slice slice;
    uint32_t row_limit;
    gc_clock::time_point timestamp;
public:
    read_command(utils::UUID cf_id,
                 table_schema_version schema_version,
                 partition_slice slice,
                 uint32_t row_limit = max_rows,
                 gc_clock::time_point now = gc_clock::now())
        : cf_id(std::move(cf_id))
        , schema_version(std::move(schema_version))
        , slice(std::move(slice))
        , row_limit(row_limit)
        , timestamp(now)
    { }

    friend std::ostream& operator<<(std::ostream& out, const read_command& r);
};

}
