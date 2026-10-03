/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/coroutine/maybe_yield.hh>

#include "mutation_query.hh"
#include "schema/schema_registry.hh"
#include "utils/on_internal_error.hh"

#include <boost/range/algorithm/equal.hpp>

reconcilable_result::~reconcilable_result() {}

reconcilable_result::reconcilable_result()
    : _row_count_low_bits(0)
    , _row_count_high_bits(0)
{ }

reconcilable_result::reconcilable_result(uint32_t row_count_low_bits, utils::chunked_vector<partition> p, query::short_read short_read,
                                         uint32_t row_count_high_bits, query::result_memory_tracker memory_tracker)
    : _row_count_low_bits(row_count_low_bits)
    , _short_read(short_read)
    , _memory_tracker(std::move(memory_tracker))
    , _partitions(std::move(p))
    , _row_count_high_bits(row_count_high_bits)
{ }

reconcilable_result::reconcilable_result(uint64_t row_count, utils::chunked_vector<partition> p, query::short_read short_read,
                                         query::result_memory_tracker memory_tracker)
    : reconcilable_result(static_cast<uint32_t>(row_count), std::move(p), short_read, static_cast<uint32_t>(row_count >> 32), std::move(memory_tracker))
{ }

reconcilable_result::reconcilable_result(uint32_t row_count_low_bits, utils::chunked_vector<partition> p, query::short_read short_read,
                                         uint32_t row_count_high_bits, std::optional<full_position> wire_position, std::vector<partition_skip> skips)
    : reconcilable_result(row_count_low_bits, std::move(p), short_read, row_count_high_bits)
{
    _stop = std::move(wire_position);
    _skips = std::move(skips);
}

void reconcilable_result::set_frontier(const schema& s, query::read_frontier frontier, std::span<const full_position> skips) {
    _stop = std::move(frontier.stop);
    _has_frontier = true;
    _skips.clear();
    _skips.reserve(skips.size());
    uint32_t i = 0;
    for (const auto& skip : skips) {
        while (i < _partitions.size() && !_partitions[i].mut().key().equal(s, skip.partition)) {
            ++i;
        }
        if (i == _partitions.size()) {
            utils::on_internal_error(fmt::format("reconcilable_result::set_frontier(): the result has no partition of the skip {}", skip.position));
        }
        _skips.push_back(partition_skip{i, skip.position});
    }
}

const utils::chunked_vector<partition>& reconcilable_result::partitions() const {
    return _partitions;
}

utils::chunked_vector<partition>& reconcilable_result::partitions() {
    return _partitions;
}

bool
reconcilable_result::operator==(const reconcilable_result& other) const {
    return boost::equal(_partitions, other._partitions);
}

void
reconcilable_result::merge_disjoint(schema_ptr schema, const reconcilable_result& other) {
    const auto offset = static_cast<uint32_t>(_partitions.size());
    for (const auto& skip : other._skips) {
        _skips.push_back(partition_skip{offset + skip.partition, skip.position});
    }
    std::copy(other._partitions.begin(), other._partitions.end(), std::back_inserter(_partitions));
    _short_read = _short_read || other._short_read;
    uint64_t row_count = this->row_count() + other.row_count();
    _row_count_low_bits = static_cast<uint32_t>(row_count);
    _row_count_high_bits = static_cast<uint32_t>(row_count >> 32);
    if (_has_frontier && other._has_frontier && !_stop) {
        _stop = other._stop;
    } else {
        _stop.reset();
        _has_frontier = false;
        _skips.clear();
    }
}

auto fmt::formatter<reconcilable_result::printer>::format(
    const reconcilable_result::printer& pr,
    fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out,
                         "{{rows={}, short_read={}, ",
                         pr.self.row_count(),
                         pr.self.is_short_read());
    if (pr.self.frontier()) {
        out = fmt::format_to(out, "frontier={{{}}}, ", query::read_frontier::printer{*pr.schema, *pr.self.frontier()});
    }
    for (const auto& skip : pr.self.skips()) {
        out = fmt::format_to(out, "skip of partition {} at {}, ", skip.partition, skip.position);
    }
    bool first = true;
    for (const partition& p : pr.self.partitions()) {
        if (!first) {
            out = fmt::format_to(out, ", ");
        }
        first = false;
        out = fmt::format_to(out,
                             "{{rows={}, {}}}",
                             p.row_count(),
                             p._m.pretty_printer(pr.schema));
    }
    return fmt::format_to(out, "]}}");
}

reconcilable_result::printer reconcilable_result::pretty_printer(schema_ptr s) const {
    return { *this, std::move(s) };
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> reversed(foreign_ptr<lw_shared_ptr<reconcilable_result>> result)
{
    for (auto& partition : result->partitions())
    {
        auto& m = partition.mut();
        auto schema = local_schema_registry().get(m.schema_version());
        m = frozen_mutation(reverse(m.unfreeze(schema)));
        co_await coroutine::maybe_yield();
    }

    co_return std::move(result);
}
