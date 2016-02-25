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

#include "mutation_query.hh"
#include "gc_clock.hh"
#include "db/serializer.hh"
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
    query::result::builder builder(slice);
    for (const partition& p : r.partitions()) {
        auto pb = builder.add_partition(*s, p._m.key(*s));
        p.mut().unfreeze(s).partition().query(pb, *s, gc_clock::time_point::min(), query::max_rows);
    }
    return builder.build();
}

future<reconcilable_result>
mutation_query(schema_ptr s,
    const mutation_source& source,
    const query::partition_range& range,
    const query::partition_slice& slice,
    uint32_t row_limit,
    gc_clock::time_point query_time)
{
    struct query_state {
        const query::partition_range& range;
        const query::partition_slice& slice;
        uint32_t requested_limit;
        gc_clock::time_point query_time;
        uint32_t limit;
        mutation_reader reader;
        std::vector<partition> result;

        query_state(
            const query::partition_range& range,
            const query::partition_slice& slice,
            uint32_t requested_limit,
            gc_clock::time_point query_time
        )
            : range(range)
            , slice(slice)
            , requested_limit(requested_limit)
            , query_time(query_time)
            , limit(requested_limit)
        { }
    };

    if (row_limit == 0) {
        return make_ready_future<reconcilable_result>(reconcilable_result());
    }

    return do_with(query_state(range, slice, row_limit, query_time),
                   [&source, s = std::move(s)] (query_state& state) -> future<reconcilable_result> {
        state.reader = source(std::move(s), state.range, service::get_local_sstable_query_read_priority());
        return consume(state.reader, [&state] (mutation&& m) {
            // FIXME: Make data sources respect row_ranges so that we don't have to filter them out here.
            auto is_distinct = state.slice.options.contains(query::partition_slice::option::distinct);
            auto is_reversed = state.slice.options.contains(query::partition_slice::option::reversed);
            auto limit = !is_distinct ? state.limit : 1;
            auto rows_left = m.partition().compact_for_query(*m.schema(), state.query_time, state.slice.row_ranges(*m.schema(), m.key()),
                is_reversed, limit);
            state.limit -= rows_left;

            if (rows_left || !m.partition().empty()) {
                // NOTE: We must return all columns, regardless of what's in
                // partition_slice, for the results to be reconcilable with tombstones.
                // That's because row's presence depends on existence of any
                // column in a row (See mutation_partition::query). We could
                // optimize this case and only send cell timestamps, without data,
                // for the cells which are not queried for (TODO).
                state.result.emplace_back(partition{rows_left, freeze(m)});
            }

            return state.limit ? stop_iteration::no : stop_iteration::yes;
        }).then([&state] {
            return make_ready_future<reconcilable_result>(
                reconcilable_result(state.requested_limit - state.limit, std::move(state.result)));
        });
    });
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
