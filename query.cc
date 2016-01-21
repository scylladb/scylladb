/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

#include <limits>
#include "db/serializer.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-set.hh"
#include "to_string.hh"
#include "bytes.hh"
#include "mutation.hh"
#include "mutation_partition_serializer.hh"

namespace query {

const partition_range full_partition_range = partition_range::make_open_ended_both_sides();

std::ostream& operator<<(std::ostream& out, const specific_ranges& s);

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    out << "{"
        << "regular_cols=[" << join(", ", ps.regular_columns) << "]"
        << ", static_cols=[" << join(", ", ps.static_columns) << "]"
        << ", rows=[" << join(", ", ps._row_ranges) << "]"
        ;
    if (ps._specific_ranges) {
        out << ", specific=[" << *ps._specific_ranges << "]";
    }
    return out << ", options=" << sprint("%x", ps.options.mask()) // FIXME: pretty print options
        << "}";
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    return out << "read_command{"
        << "cf_id=" << r.cf_id
        << ", version=" << r.schema_version
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit
        << ", timestamp=" << r.timestamp.time_since_epoch().count() << "}";
}

std::ostream& operator<<(std::ostream& out, const specific_ranges& s) {
    return out << "{" << s._pk << " : " << join(", ", s._ranges) << "}";
}

partition_slice::partition_slice(clustering_row_ranges row_ranges, std::vector<column_id> static_columns,
    std::vector<column_id> regular_columns, option_set options, std::unique_ptr<specific_ranges> specific_ranges)
    : _row_ranges(std::move(row_ranges))
    , static_columns(std::move(static_columns))
    , regular_columns(std::move(regular_columns))
    , options(options)
    , _specific_ranges(std::move(specific_ranges))
{}

partition_slice::partition_slice(partition_slice&&) = default;

// Only needed because selection_statement::execute does copies of its read_command
// in the map-reduce op.
partition_slice::partition_slice(const partition_slice& s)
    : _row_ranges(s._row_ranges)
    , static_columns(s.static_columns)
    , regular_columns(s.regular_columns)
    , options(s.options)
    , _specific_ranges(s._specific_ranges ? std::make_unique<specific_ranges>(*s._specific_ranges) : nullptr)
{}

partition_slice::~partition_slice()
{}

const clustering_row_ranges& partition_slice::row_ranges(const schema& s, const partition_key& k) const {
    auto* r = _specific_ranges ? _specific_ranges->range_for(s, k) : nullptr;
    return r ? *r : _row_ranges;
}

void partition_slice::set_range(const schema& s, const partition_key& k, clustering_row_ranges range) {
    if (!_specific_ranges) {
        _specific_ranges = std::make_unique<specific_ranges>(k, std::move(range));
    } else {
        _specific_ranges->add(s, k, std::move(range));
    }
}

void partition_slice::clear_range(const schema& s, const partition_key& k) {
    if (_specific_ranges && _specific_ranges->contains(s, k)) {
        // just in case someone changes the impl above,
        // we should do actual remove if specific_ranges suddenly
        // becomes an actual map
        assert(_specific_ranges->size() == 1);
        _specific_ranges = nullptr;
    }
}

query::partition_range
to_partition_range(query::range<dht::token> r) {
    using bound_opt = std::experimental::optional<query::partition_range::bound>;
    auto start = r.start()
                 ? bound_opt(dht::ring_position(r.start()->value(),
            r.start()->is_inclusive()
            ? dht::ring_position::token_bound::start
            : dht::ring_position::token_bound::end))
                 : bound_opt();

    auto end = r.end()
               ? bound_opt(dht::ring_position(r.end()->value(),
            r.end()->is_inclusive()
            ? dht::ring_position::token_bound::end
            : dht::ring_position::token_bound::start))
               : bound_opt();

    return { std::move(start), std::move(end) };
}

sstring
result::pretty_print(schema_ptr s, const query::partition_slice& slice) const {
    std::ostringstream out;
    out << "{" << result_set::from_raw_result(s, slice, *this) << "}";
    return out.str();
}

}
