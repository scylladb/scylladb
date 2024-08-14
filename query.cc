/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <limits>
#include <memory>
#include <stdexcept>
#include <fmt/ranges.h>
#include "query-request.hh"
#include "query-result.hh"
#include "query-result-writer.hh"
#include "query-result-set.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include "bytes.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "query-result-reader.hh"
#include "query_result_merger.hh"
#include "partition_slice_builder.hh"
#include "schema/schema_registry.hh"
#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"

namespace query {

static logging::logger qlogger("query");

constexpr size_t result_memory_limiter::minimum_result_size;
constexpr size_t result_memory_limiter::maximum_result_size;
constexpr size_t result_memory_limiter::unlimited_result_size;

thread_local semaphore result_memory_tracker::_dummy { 0 };

const dht::partition_range full_partition_range = dht::partition_range::make_open_ended_both_sides();
const clustering_range full_clustering_range = clustering_range::make_open_ended_both_sides();

std::ostream& operator<<(std::ostream& out, const specific_ranges& s);

std::ostream& operator<<(std::ostream& out, const partition_slice& ps) {
    fmt::print(out,
               "{{regular_cols=[{}], static_cols=[{}], rows=[{}]",
               fmt::join(ps.regular_columns, ", "),
               fmt::join(ps.static_columns, ", "),
               ps._row_ranges);
    if (ps._specific_ranges) {
        fmt::print(out, ", specific=[{}]", *ps._specific_ranges);
    }
    // FIXME: pretty print options
    fmt::print(out, ", options={:x}, , partition_row_limit={}}}",
               ps.options.mask(), ps.partition_row_limit());
    return out;
}

std::ostream& operator<<(std::ostream& out, const read_command& r) {
    fmt::print(out, "read_command{{cf_id={}, version={}, slice={}, limit={}, timestamp={}, partition_limit={}, query_uuid={}, is_first_page={}, read_timestamp={}}}",
               r.cf_id, r.schema_version, r.slice, r.get_row_limit(), r.timestamp.time_since_epoch().count(), r.partition_limit, r.query_uuid, r.is_first_page, r.read_timestamp);
    return out;
}

lw_shared_ptr<query::read_command> reversed(lw_shared_ptr<query::read_command>&& cmd)
{
    auto schema = local_schema_registry().get(cmd->schema_version)->get_reversed();
    cmd->schema_version = schema->version();
    cmd->slice = query::legacy_reverse_slice_to_native_reverse_slice(*schema, cmd->slice);

    return std::move(cmd);
}

std::ostream& operator<<(std::ostream& out, const mapreduce_request::reduction_type& r) {
    out << "reduction_type{";
    switch (r) {
        case mapreduce_request::reduction_type::count:
            out << "count";
            break;
        case mapreduce_request::reduction_type::aggregate:
            out << "aggregate";
            break;
    }
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, const mapreduce_request::aggregation_info& a) {
    fmt::print(out, "aggregation_info{{, name={}, column_names=[{}]}}",
               a.name, fmt::join(a.column_names, ","));;
    return out;
}

std::ostream& operator<<(std::ostream& out, const mapreduce_request& r) {
    auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(r.timeout).time_since_epoch().count();
    fmt::print(out, "mapreduce_request{{reductions=[{}]",
               fmt::join(r.reduction_types, ","));
    if (r.aggregation_infos) {
        fmt::print(out, ", aggregation_infos=[{}]",
                   fmt::join(r.aggregation_infos.value(), ","));
    }
    fmt::print(out, "cmd={}, pr={}, cl={}, timeout(ms)={}}}",
               r.cmd, r.pr, r.cl, ms);
    return out;
}


std::ostream& operator<<(std::ostream& out, const specific_ranges& s) {
    fmt::print(out, "{{{} : {}}}", s._pk, fmt::join(s._ranges, ", "));
    return out;
}

void trim_clustering_row_ranges_to(const schema& s, clustering_row_ranges& ranges, position_in_partition pos) {
    auto cmp = position_in_partition::composite_tri_compare(s);

    auto it = ranges.begin();
    while (it != ranges.end()) {
        auto end_bound = position_in_partition_view::for_range_end(*it);
        if (cmp(end_bound, pos) <= 0) {
            it = ranges.erase(it);
            continue;
        } else if (auto start_bound = position_in_partition_view::for_range_start(*it); cmp(start_bound, pos) <= 0) {
            SCYLLA_ASSERT(cmp(pos, end_bound) < 0);
            *it = clustering_range(clustering_range::bound(pos.key(), pos.get_bound_weight() != bound_weight::after_all_prefixed), it->end());
        }
        ++it;
    }
}

void trim_clustering_row_ranges_to(const schema& s, clustering_row_ranges& ranges, const clustering_key& key) {
    return trim_clustering_row_ranges_to(s, ranges, position_in_partition::after_key(s, key));
}


clustering_range reverse(const clustering_range& range) {
    if (range.is_singular()) {
        return range;
    }
    return clustering_range(range.end(), range.start());
}


static void reverse_clustering_ranges_bounds(clustering_row_ranges& ranges) {
    for (auto& range : ranges) {
        range = reverse(range);
    }
}

partition_slice legacy_reverse_slice_to_native_reverse_slice(const schema& schema, partition_slice slice) {
    return partition_slice_builder(schema, std::move(slice))
        .mutate_ranges([] (clustering_row_ranges& ranges) { reverse_clustering_ranges_bounds(ranges); })
        .mutate_specific_ranges([] (specific_ranges& ranges) { reverse_clustering_ranges_bounds(ranges.ranges()); })
        .build();
}

partition_slice native_reverse_slice_to_legacy_reverse_slice(const schema& schema, partition_slice slice) {
    // They are the same, we give them different names to express intent
    return legacy_reverse_slice_to_native_reverse_slice(schema, std::move(slice));
}

partition_slice reverse_slice(const schema& schema, partition_slice slice) {
    return partition_slice_builder(schema, std::move(slice))
        .mutate_ranges([] (clustering_row_ranges& ranges) {
            std::reverse(ranges.begin(), ranges.end());
            reverse_clustering_ranges_bounds(ranges);
        })
        .mutate_specific_ranges([] (specific_ranges& sranges) {
            auto& ranges = sranges.ranges();
            std::reverse(ranges.begin(), ranges.end());
            reverse_clustering_ranges_bounds(ranges);
        })
        .with_option_toggled<partition_slice::option::reversed>()
        .build();
}

partition_slice::partition_slice(clustering_row_ranges row_ranges,
    query::column_id_vector static_columns,
    query::column_id_vector regular_columns,
    option_set options,
    std::unique_ptr<specific_ranges> specific_ranges,
    cql_serialization_format cql_format,
    uint32_t partition_row_limit_low_bits,
    uint32_t partition_row_limit_high_bits)
    : _row_ranges(std::move(row_ranges))
    , static_columns(std::move(static_columns))
    , regular_columns(std::move(regular_columns))
    , options(options)
    , _specific_ranges(std::move(specific_ranges))
    , _partition_row_limit_low_bits(partition_row_limit_low_bits)
    , _partition_row_limit_high_bits(partition_row_limit_high_bits)
{
    cql_format.ensure_supported();
}

partition_slice::partition_slice(clustering_row_ranges row_ranges,
    query::column_id_vector static_columns,
    query::column_id_vector regular_columns,
    option_set options,
    std::unique_ptr<specific_ranges> specific_ranges,
    uint64_t partition_row_limit)
    : partition_slice(std::move(row_ranges), std::move(static_columns), std::move(regular_columns), options,
            std::move(specific_ranges), cql_serialization_format::latest(), static_cast<uint32_t>(partition_row_limit),
            static_cast<uint32_t>(partition_row_limit >> 32))
{}

partition_slice::partition_slice(clustering_row_ranges ranges, const schema& s, const column_set& columns, option_set options)
    : partition_slice(ranges, query::column_id_vector{}, query::column_id_vector{}, options)
{
    regular_columns.reserve(columns.count());
    for (ordinal_column_id id = columns.find_first(); id != column_set::npos; id = columns.find_next(id)) {
        const column_definition& def = s.column_at(id);
        if (def.is_static()) {
            static_columns.push_back(def.id);
        } else if (def.is_regular()) {
            regular_columns.push_back(def.id);
        } // else clustering or partition key column - skip, these are controlled by options
    }
}

partition_slice::partition_slice(partition_slice&&) = default;

partition_slice& partition_slice::operator=(partition_slice&& other) noexcept = default;

// Only needed because selection_statement::execute does copies of its read_command
// in the map-reduce op.
partition_slice::partition_slice(const partition_slice& s)
    : _row_ranges(s._row_ranges)
    , static_columns(s.static_columns)
    , regular_columns(s.regular_columns)
    , options(s.options)
    , _specific_ranges(s._specific_ranges ? std::make_unique<specific_ranges>(*s._specific_ranges) : nullptr)
    , _partition_row_limit_low_bits(s._partition_row_limit_low_bits)
    , _partition_row_limit_high_bits(s._partition_row_limit_high_bits)
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
        SCYLLA_ASSERT(_specific_ranges->size() == 1);
        _specific_ranges = nullptr;
    }
}

clustering_row_ranges partition_slice::get_all_ranges() const {
    auto all_ranges = default_row_ranges();
    const auto& specific_ranges = get_specific_ranges();
    if (specific_ranges) {
        all_ranges.insert(all_ranges.end(), specific_ranges->ranges().begin(), specific_ranges->ranges().end());
    }
    return all_ranges;
}

sstring
result::pretty_print(schema_ptr s, const query::partition_slice& slice) const {
    std::ostringstream out;
    out << "{ result: " << result_set::from_raw_result(s, slice, *this);
    out << " digest: ";
    if (_digest) {
        out << std::hex << std::setw(2);
        for (auto&& c : _digest->get()) {
            out << unsigned(c) << " ";
        }
    } else {
        out << "{}";
    }
    out << ", short_read=" << is_short_read() << " }";
    return out.str();
}

query::result::printer
result::pretty_printer(schema_ptr s, const query::partition_slice& slice) const {
    return query::result::printer{s, slice, *this};
}

std::ostream& operator<<(std::ostream& os, const query::result::printer& p) {
    os << p.res.pretty_print(p.s, p.slice);
    return os;
}

void result::ensure_counts() {
    if (!_partition_count || !row_count()) {
        uint64_t row_count;
        std::tie(_partition_count, row_count) = result_view::do_with(*this, [] (auto&& view) {
            return view.count_partitions_and_rows();
        });
        set_row_count(row_count);
    }
}

full_position result::get_or_calculate_last_position() const {
    if (_last_position) {
        return *_last_position;
    }
    return result_view::do_with(*this, [] (const result_view& v) {
        return v.calculate_last_position();
    });
}

result::result()
    : result([] {
        bytes_ostream out;
        ser::writer_of_query_result<bytes_ostream>(out).skip_partitions().end_query_result();
        return out;
    }(), short_read::no, 0, 0, {})
{ }

static void write_partial_partition(ser::writer_of_qr_partition<bytes_ostream>&& pw, const ser::qr_partition_view& pv, uint64_t rows_to_include) {
    auto key = pv.key();
    auto static_cells_wr = (key ? std::move(pw).write_key(*key) : std::move(pw).skip_key())
            .start_static_row()
            .start_cells();
    for (auto&& cell : pv.static_row().cells()) {
        static_cells_wr.add(cell);
    }
    auto rows_wr = std::move(static_cells_wr)
            .end_cells()
            .end_static_row()
            .start_rows();
    auto rows = pv.rows();
    // rows.size() can be 0 is there's a single static row
    auto it = rows.begin();
    for (uint64_t i = 0; i < std::min(rows.size(), rows_to_include); ++i) {
        rows_wr.add(*it++);
    }
    std::move(rows_wr).end_rows().end_qr_partition();
}

foreign_ptr<lw_shared_ptr<query::result>> result_merger::get() {
    if (_partial.size() == 1) {
        return std::move(_partial[0]);
    }

    bytes_ostream w;
    auto partitions = ser::writer_of_query_result<bytes_ostream>(w).start_partitions();
    uint64_t row_count = 0;
    short_read is_short_read;
    uint32_t partition_count = 0;

    std::optional<full_position> last_position;

    for (auto&& r : _partial) {
        result_view::do_with(*r, [&] (result_view rv) {
            last_position.reset();
            for (auto&& pv : rv._v.partitions()) {
                auto rows = pv.rows();
                // If rows.empty(), then there's a static row, or there wouldn't be a partition
                const uint64_t rows_in_partition = rows.size() ? : 1;
                const uint64_t rows_to_include = std::min(_max_rows - row_count, rows_in_partition);
                row_count += rows_to_include;
                if (rows_to_include >= rows_in_partition) {
                    partitions.add(pv);
                    if (++partition_count >= _max_partitions) {
                        return;
                    }
                } else if (rows_to_include > 0) {
                    ++partition_count;
                    write_partial_partition(partitions.add(), pv, rows_to_include);
                    return;
                } else {
                    return;
                }
            }
            last_position = r->last_position();
        });
        if (r->is_short_read()) {
            is_short_read = short_read::yes;
            break;
        }
        if (row_count >= _max_rows || partition_count >= _max_partitions) {
            break;
        }
    }

    std::move(partitions).end_partitions().end_query_result();
    return make_foreign(make_lw_shared<query::result>(std::move(w), is_short_read, row_count, partition_count, std::move(last_position)));
}

std::ostream& operator<<(std::ostream& out, const query::mapreduce_result::printer& p) {
    if (p.functions.size() != p.res.query_results.size()) {
        return out << "[malformed mapreduce_result (" << p.res.query_results.size()
            << " results, " << p.functions.size() << " aggregates)]";
    }

    out << "[";
    for (size_t i = 0; i < p.functions.size(); i++) {
        auto& return_type = p.functions[i]->return_type();
        out << return_type->to_string(bytes_view(*p.res.query_results[i]));

        if (i + 1 < p.functions.size()) {
            out << ", ";
        }
    }
    return out << "]";
}

}

std::optional<query::clustering_range> position_range_to_clustering_range(const position_range& r, const schema& s) {
    SCYLLA_ASSERT(r.start().get_type() == partition_region::clustered);
    SCYLLA_ASSERT(r.end().get_type() == partition_region::clustered);

    if (r.start().has_key() && r.end().has_key()
            && clustering_key_prefix::equality(s)(r.start().key(), r.end().key())) {
        SCYLLA_ASSERT(r.start().get_bound_weight() != r.end().get_bound_weight());

        if (r.end().get_bound_weight() == bound_weight::after_all_prefixed
                && r.start().get_bound_weight() != bound_weight::after_all_prefixed) {
            // [before x, after x) and [for x, after x) get converted to [x, x].
            return query::clustering_range::make_singular(r.start().key());
        }

        // [before x, for x) does not contain any keys.
        return std::nullopt;
    }

    // position_range -> clustering_range
    // (recall that position_ranges are always left-closed, right opened):
    // [before x, ...), [for x, ...) -> [x, ...
    // [after x, ...) -> (x, ...
    // [..., before x), [..., for x) -> ..., x)
    // [..., after x) -> ..., x]

    auto to_bound = [&s] (const position_in_partition& p, bool left) -> std::optional<query::clustering_range::bound> {
        if (p.is_before_all_clustered_rows(s)) {
            SCYLLA_ASSERT(left);
            return {};
        }

        if (p.is_after_all_clustered_rows(s)) {
            SCYLLA_ASSERT(!left);
            return {};
        }

        SCYLLA_ASSERT(p.has_key());

        auto bw = p.get_bound_weight();
        bool inclusive = left
            ? bw != bound_weight::after_all_prefixed
            : bw == bound_weight::after_all_prefixed;

        return query::clustering_range::bound{p.key(), inclusive};
    };

    return query::clustering_range{to_bound(r.start(), true), to_bound(r.end(), false)};
}
