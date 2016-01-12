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

std::ostream& operator<<(std::ostream& out, const partition_slice::specific_ranges& s);

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

static size_t ranges_size(const clustering_row_ranges& r) {
    size_t row_range_size = serialize_int32_size;
    for (auto&& i : r) {
        row_range_size += i.serialized_size();
    }
    return row_range_size;
}

static void serialize_ranges(bytes::iterator& out, const clustering_row_ranges& r) {
    serialize_int32(out, r.size());
    for (auto&& i : r) {
        i.serialize(out);
    }
}

static clustering_row_ranges deserialize_ranges(bytes_view& v) {
    auto size = read_simple<uint32_t>(v);
    clustering_row_ranges row_ranges;
    row_ranges.reserve(size);
    while (size--) {
        row_ranges.emplace_back(clustering_range::deserialize(v));
    };
    return row_ranges;
}

class partition_slice::specific_ranges {
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
    size_t serialized_size() const {
        return serialize_int32_size + _pk.representation().size()
                + ranges_size(_ranges);
    }
    void serialize(bytes::iterator& out) const {
        // I so wish this used serializers & data_output...
        const auto& v = _pk.representation();
        serialize_int32(out, v.size());
        out = std::copy(v.begin(), v.end(), out);
        serialize_ranges(out, _ranges);
    }

    static specific_ranges deserialize(bytes_view& v) {
        auto size = read_simple<uint32_t>(v);
        auto pk = partition_key::from_bytes(to_bytes(read_simple_bytes(v, size)));
        auto range = deserialize_ranges(v);

        return specific_ranges(std::move(pk), std::move(range));
    }
private:
    friend std::ostream& operator<<(std::ostream& out, const specific_ranges& r);

    partition_key _pk;
    clustering_row_ranges _ranges;
};

std::ostream& operator<<(std::ostream& out, const partition_slice::specific_ranges& s) {
    return out << "{" << s._pk << " : " << join(", ", s._ranges) << "}";
}

partition_slice::partition_slice(clustering_row_ranges row_ranges, std::vector<column_id> static_columns,
    std::vector<column_id> regular_columns, option_set options)
    : _row_ranges(std::move(row_ranges))
    , static_columns(std::move(static_columns))
    , regular_columns(std::move(regular_columns))
    , options(options)
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

size_t partition_slice::serialized_size() const {
    return ranges_size(_row_ranges)
            + serialize_int64_size // options
            + (static_columns.size() + 1) * serialize_int32_size
            + (regular_columns.size() + 1) * serialize_int32_size
            + (_specific_ranges ? _specific_ranges->serialized_size() : 0)
            ;
}

void partition_slice::serialize(bytes::iterator& out) const {
    serialize_int64(out, options.mask());
    serialize_int32(out, static_columns.size());
    for(auto i : static_columns) {
        serialize_int32(out, i);
    }
    serialize_int32(out, regular_columns.size());
    for(auto i : regular_columns) {
        serialize_int32(out, i);
    }

    serialize_ranges(out, _row_ranges);

    if (_specific_ranges) {
        _specific_ranges->serialize(out);
    }
}
partition_slice partition_slice::deserialize(bytes_view& v) {
    partition_slice::option_set options = partition_slice::option_set::from_mask(read_simple<int64_t>(v));

    auto read_columns = [](bytes_view& v) {
        uint32_t size = read_simple<uint32_t>(v);
        std::vector<column_id> columns;
        columns.reserve(size);
        while (size--) {
            columns.push_back(read_simple<uint32_t>(v));
        };
        return columns;
    };

    auto static_columns = read_columns(v);
    auto regular_columns = read_columns(v);
    auto row_ranges = deserialize_ranges(v);

    partition_slice ps(row_ranges, static_columns, regular_columns, options);

    if (v.size() > 0) {
        ps._specific_ranges = std::make_unique<specific_ranges>(specific_ranges::deserialize(v));
    }
    return ps;
}

size_t read_command::serialized_size() const {
    return 4 * serialize_int64_size // cf_id
            + serialize_int32_size // row_limit
            + serialize_int32_size // timestamp
            + slice.serialized_size()
            ;
}

void read_command::serialize(bytes::iterator& out) const {
    serialize_int64(out, cf_id.get_most_significant_bits());
    serialize_int64(out, cf_id.get_least_significant_bits());
    serialize_int64(out, schema_version.get_most_significant_bits());
    serialize_int64(out, schema_version.get_least_significant_bits());
    serialize_int32(out, row_limit);
    serialize_int32(out, timestamp.time_since_epoch().count());
    slice.serialize(out);
}

static utils::UUID read_uuid(bytes_view& v) {
    auto msb = read_simple<int64_t>(v);
    auto lsb = read_simple<int64_t>(v);
    return { msb, lsb };
}

read_command read_command::deserialize(bytes_view& v) {
    auto cf_id = read_uuid(v);
    auto schema_version = read_uuid(v);
    uint32_t row_limit = read_simple<int32_t>(v);
    auto timestamp = gc_clock::time_point(gc_clock::duration(read_simple<int32_t>(v)));

    partition_slice slice = partition_slice::deserialize(v);
    return read_command(cf_id, schema_version, std::move(slice), row_limit, timestamp);
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

template class db::serializer<query::result>;

using query_result_size_type = uint32_t;

template<>
db::serializer<query::result>::serializer(const query::result& v)
        : _item(v)
        , _size(sizeof(query_result_size_type) + v.buf().size())
{
    static_assert(std::numeric_limits<bytes_ostream::size_type>::max() <=
                  std::numeric_limits<query_result_size_type>::max(), "query_result_size_type too small");
}

template<>
void
db::serializer<query::result>::write(output& out, const query::result& v) {
    const bytes_ostream& buf = v.buf();
    out.write<query_result_size_type>(buf.size());
    for (bytes_view frag : buf.fragments()) {
        out.write(frag.begin(), frag.end());
    }
}

template<>
query::result db::serializer<query::result>::read(input& in) {
    bytes_ostream buf;
    auto size = in.read<query_result_size_type>();
    buf.write(in.read_view(size));
    return query::result(std::move(buf));
}
