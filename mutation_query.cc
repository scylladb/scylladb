/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "mutation_query.hh"
#include "gc_clock.hh"
#include "db/serializer.hh"
#include "mutation_partition_serializer.hh"

template class db::serializer<reconcilable_result>;

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
    auto builder = query::result::builder(slice);
    for (const partition& p : r.partitions()) {
        auto pb = builder.add_partition(p._m.key(*s));
        p.mut().unfreeze(s).partition().query(pb, *s, gc_clock::time_point::min(), query::max_rows);
    }
    return builder.build();
}

static
size_t serialized_size(const reconcilable_result& v) {
    size_t s = 0;
    s += sizeof(uint32_t); /* row_count */
    s += sizeof(uint32_t); /* partition count */
    for (const partition& p : v.partitions()) {
        s += sizeof(uint32_t) /* row_count */;
        s += db::frozen_mutation_serializer(p.mut()).size();
    }
    return s;
}

template<>
db::serializer<reconcilable_result>::serializer(const reconcilable_result& v)
    : _item(v)
    , _size(serialized_size(v))
{ }

template<>
void
db::serializer<reconcilable_result>::write(output& out, const reconcilable_result& v) {
    out.write<uint32_t>(v.row_count());
    out.write<uint32_t>(v.partitions().size());
    for (const partition& p : v.partitions()) {
        out.write<uint32_t>(p.row_count());
        db::frozen_mutation_serializer(p.mut()).write(out);
    }
}

template<>
void db::serializer<reconcilable_result>::read(reconcilable_result& v, input& in) {
    auto row_count = in.read<uint32_t>();
    auto partition_count = in.read<uint32_t>();
    std::vector<partition> partitions;
    partitions.reserve(partition_count);
    while (partition_count--) {
        auto p_row_count = in.read<uint32_t>();
        auto fm = db::frozen_mutation_serializer::read(in);
        partitions.emplace_back(partition(p_row_count, std::move(fm)));
    }
    v = reconcilable_result(row_count, std::move(partitions));
}

future<reconcilable_result>
mutation_query(const mutation_source& source,
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

    return do_with(query_state(range, slice, row_limit, query_time), [&source] (query_state& state) -> future<reconcilable_result> {
        state.reader = source(state.range);
        return consume(state.reader, [&state] (mutation&& m) {
            // FIXME: Make data sources respect row_ranges so that we don't have to filter them out here.
            auto is_distinct = state.slice.options.contains(query::partition_slice::option::distinct);
            auto limit = !is_distinct ? state.limit : 1;
            auto rows_left = m.partition().compact_for_query(*m.schema(), state.query_time, state.slice.row_ranges, limit);
            state.limit -= rows_left;

            // NOTE: We must return all columns, regardless of what's in
            // partition_slice, for the results to be reconcilable with tombstones.
            // That's because row's presence depends on existence of any
            // column in a row (See mutation_partition::query). We could
            // optimize this case and only send cell timestamps, without data,
            // for the cells which are not queried for (TODO).
            state.result.emplace_back(partition{rows_left, freeze(m)});

            return state.limit ? stop_iteration::no : stop_iteration::yes;
        }).then([&state] {
            return make_ready_future<reconcilable_result>(
                reconcilable_result(state.requested_limit - state.limit, std::move(state.result)));
        });
    });
}
