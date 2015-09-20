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

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    return out << "{"
        << "regular_cols=[" << join(", ", ps.regular_columns) << "]"
        << ", static_cols=[" << join(", ", ps.static_columns) << "]"
        << ", rows=[" << join(", ", ps.row_ranges) << "]"
        << ", options=" << sprint("%x", ps.options.mask()) // FIXME: pretty print options
        << "}";
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    return out << "read_command{"
        << "cf_id=" << r.cf_id
        << ", slice=" << r.slice << ""
        << ", limit=" << r.row_limit
        << ", timestamp=" << r.timestamp.time_since_epoch().count() << "}";
}

size_t read_command::serialized_size() const {
    size_t row_range_size = serialize_int32_size;
    for (auto&& i : slice.row_ranges) {
        row_range_size += i.serialized_size();
    }
    return 2 * serialize_int64_size // cf_id
            + serialize_int32_size // row_limit
            + serialize_int32_size // timestamp
            + serialize_int64_size // slice.options
            + (slice.static_columns.size() + 1) * serialize_int32_size
            + (slice.regular_columns.size() + 1) * serialize_int32_size
            + row_range_size;
}

void read_command::serialize(bytes::iterator& out) const {
    serialize_int64(out, cf_id.get_most_significant_bits());
    serialize_int64(out, cf_id.get_least_significant_bits());
    serialize_int32(out, row_limit);
    serialize_int32(out, timestamp.time_since_epoch().count());
    serialize_int64(out, slice.options.mask());
    serialize_int32(out, slice.static_columns.size());
    for(auto i : slice.static_columns) {
        serialize_int32(out, i);
    }
    serialize_int32(out, slice.regular_columns.size());
    for(auto i : slice.regular_columns) {
        serialize_int32(out, i);
    }
    serialize_int32(out, slice.row_ranges.size());
    for (auto&& i : slice.row_ranges) {
        i.serialize(out);
    }
}

read_command read_command::deserialize(bytes_view& v) {
    auto msb = read_simple<int64_t>(v);
    auto lsb = read_simple<int64_t>(v);
    utils::UUID uuid(msb, lsb);
    uint32_t row_limit = read_simple<int32_t>(v);
    auto timestamp = gc_clock::time_point(gc_clock::duration(read_simple<int32_t>(v)));
    partition_slice::option_set options = partition_slice::option_set::from_mask(read_simple<int64_t>(v));

    uint32_t size = read_simple<uint32_t>(v);
    std::vector<column_id> static_columns;
    static_columns.reserve(size);
    while(size--) {
        static_columns.push_back(read_simple<uint32_t>(v));
    };

    size = read_simple<uint32_t>(v);
    std::vector<column_id> regular_columns;
    regular_columns.reserve(size);
    while(size--) {
        regular_columns.push_back(read_simple<uint32_t>(v));
    };

    size = read_simple<uint32_t>(v);
    std::vector<clustering_range> row_ranges;
    row_ranges.reserve(size);
    while(size--) {
        row_ranges.emplace_back(clustering_range::deserialize(v));
    };

    return read_command(std::move(uuid), partition_slice(std::move(row_ranges), std::move(static_columns), std::move(regular_columns), options), row_limit, timestamp);
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
            r.start()->is_inclusive()
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
