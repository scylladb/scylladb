/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/coroutine/maybe_yield.hh>

#include "mutation_query.hh"
#include "schema/schema_registry.hh"

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
    std::copy(other._partitions.begin(), other._partitions.end(), std::back_inserter(_partitions));
    _short_read = _short_read || other._short_read;
    uint64_t row_count = this->row_count() + other.row_count();
    _row_count_low_bits = static_cast<uint32_t>(row_count);
    _row_count_high_bits = static_cast<uint32_t>(row_count >> 32);
}

auto fmt::formatter<reconcilable_result::printer>::format(
    const reconcilable_result::printer& pr,
    fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out,
                         "{{rows={}, short_read={}, ",
                         pr.self.row_count(),
                         pr.self.is_short_read());
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
