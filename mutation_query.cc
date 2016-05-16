/*
 * Copyright (C) 2015 ScyllaDB
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

#include "mutation_query.hh"
#include "gc_clock.hh"
#include "mutation_partition_serializer.hh"
#include "service/priority_manager.hh"
#include "query-result-writer.hh"

reconcilable_result::~reconcilable_result() {}

reconcilable_result::reconcilable_result()
    : _row_count(0)
{ }

reconcilable_result::reconcilable_result(uint32_t row_count, std::vector<partition> p)
    : _row_count(row_count)
    , _partitions(std::move(p))
{ }

const std::vector<partition>& reconcilable_result::partitions() const {
    return _partitions;
}

std::vector<partition>& reconcilable_result::partitions() {
    return _partitions;
}

bool
reconcilable_result::operator==(const reconcilable_result& other) const {
    return boost::equal(_partitions, other._partitions);
}

bool reconcilable_result::operator!=(const reconcilable_result& other) const {
    return !(*this == other);
}

query::result
to_data_query_result(const reconcilable_result& r, schema_ptr s, const query::partition_slice& slice) {
    query::result::builder builder(slice, query::result_request::only_result);
    for (const partition& p : r.partitions()) {
        p.mut().unfreeze(s).query(builder, slice, gc_clock::time_point::min(), query::max_rows);
    }
    return builder.build();
}


querying_reader::querying_reader(schema_ptr s,
        const mutation_source& source,
        const query::partition_range& range,
        const query::partition_slice& slice,
        uint32_t row_limit,
        gc_clock::time_point query_time,
        std::function<void(uint32_t, mutation&&)> consumer)
    : _schema(std::move(s))
    , _range(range)
    , _slice(slice)
    , _requested_limit(row_limit)
    , _query_time(query_time)
    , _limit(row_limit)
    , _source(source)
    , _consumer(std::move(consumer))
{ }

future<> querying_reader::read() {
    _reader = _source(_schema, _range, query::clustering_key_filtering_context::create(_schema, _slice),
            service::get_local_sstable_query_read_priority());
    return consume(*_reader, [this](mutation&& m) {
        // FIXME: Make data sources respect row_ranges so that we don't have to filter them out here.
        auto is_distinct = _slice.options.contains(query::partition_slice::option::distinct);
        auto is_reversed = _slice.options.contains(query::partition_slice::option::reversed);
        auto limit = !is_distinct ? _limit : 1;
        auto rows_left = m.partition().compact_for_query(*m.schema(), _query_time,
                                                         _slice.row_ranges(*m.schema(), m.key()),
                                                         is_reversed, limit);
        _limit -= rows_left;

        if (rows_left || !m.partition().empty()) {
            // NOTE: We must return all columns, regardless of what's in
            // partition_slice, for the results to be reconcilable with tombstones.
            // That's because row's presence depends on existence of any
            // column in a row (See mutation_partition::query). We could
            // optimize this case and only send cell timestamps, without data,
            // for the cells which are not queried for (TODO).
            _consumer(rows_left, std::move(m));
        }

        return _limit ? stop_iteration::no : stop_iteration::yes;
    });
}

class reconcilable_result_builder {
    querying_reader _reader;
    std::vector<partition> _result;
    uint32_t _total = 0;
public:
    reconcilable_result_builder(schema_ptr s,
        const mutation_source& source,
        const query::partition_range& range,
        const query::partition_slice& slice,
        uint32_t row_limit,
        gc_clock::time_point query_time)
            : _reader(std::move(s), source, range, slice, row_limit, query_time, [this] (uint32_t live_rows, mutation&& m) {
                _result.emplace_back(partition{live_rows, freeze(m)});
                _total += live_rows;
            })
    { }

    reconcilable_result_builder(reconcilable_result_builder&&) = delete; // this captured

    future<reconcilable_result> build() {
        return _reader.read().then([this] {
            return make_ready_future<reconcilable_result>(reconcilable_result(_total, std::move(_result)));
        });
    }
};

future<reconcilable_result>
mutation_query(schema_ptr s,
               const mutation_source& source,
               const query::partition_range& range,
               const query::partition_slice& slice,
               uint32_t row_limit,
               gc_clock::time_point query_time)
{
    if (row_limit == 0) {
        return make_ready_future<reconcilable_result>(reconcilable_result());
    }

    auto b_ptr = std::make_unique<reconcilable_result_builder>(std::move(s), source, range, slice, row_limit, query_time);
    auto& b = *b_ptr;
    return b.build().finally([keep = std::move(b_ptr)] {});
}

std::ostream& operator<<(std::ostream& out, const reconcilable_result::printer& pr) {
    out << "{rows=" << pr.self.row_count() << ", [";
    bool first = true;
    for (const partition& p : pr.self.partitions()) {
        if (!first) {
            out << ", ";
        }
        first = false;
        out << "{rows=" << p.row_count() << ", ";
        out << p._m.pretty_printer(pr.schema);
        out << "}";
    }
    out << "]}";
    return out;
}

reconcilable_result::printer reconcilable_result::pretty_printer(schema_ptr s) const {
    return { *this, std::move(s) };
}
