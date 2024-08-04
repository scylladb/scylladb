/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/heap_algorithm.hpp>

#include "mutation_fragment.hh"
#include "mutation_fragment_v2.hh"
#include "clustering_interval_set.hh"
#include "utils/assert.hh"
#include "utils/hashing.hh"
#include "utils/xx_hasher.hh"

partition_region parse_partition_region(std::string_view s) {
    if (s == "partition_start") {
        return partition_region::partition_start;
    } else if (s == "static_row") {
        return partition_region::static_row;
    } else if (s == "clustered") {
        return partition_region::clustered;
    } else if (s == "partition_end") {
        return partition_region::partition_end;
    } else {
        throw std::runtime_error(fmt::format("Invalid value for partition_region: {}", s));
    }
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, static_row&& r)
    : _kind(kind::static_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_static_row) static_row(std::move(r));
    reset_memory(s);
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, clustering_row&& r)
    : _kind(kind::clustering_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_clustering_row) clustering_row(std::move(r));
    reset_memory(s);
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, range_tombstone&& r)
    : _kind(kind::range_tombstone), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_range_tombstone) range_tombstone(std::move(r));
    reset_memory(s);
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, partition_start&& r)
        : _kind(kind::partition_start), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_start) partition_start(std::move(r));
    reset_memory(s);
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, partition_end&& r)
        : _kind(kind::partition_end), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_end) partition_end(std::move(r));
    reset_memory(s);
}

void mutation_fragment::reset_memory(const schema& s, std::optional<reader_resources> res) {
    try {
        _data->_memory.reset_to(res ? *res : reader_resources::with_memory(calculate_memory_usage(s)));
    } catch (...) {
        destroy_data();
        throw;
    }
}

void mutation_fragment::destroy_data() noexcept
{
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.~clustering_row();
        break;
    case kind::range_tombstone:
        _data->_range_tombstone.~range_tombstone();
        break;
    case kind::partition_start:
        _data->_partition_start.~partition_start();
        break;
    case kind::partition_end:
        _data->_partition_end.~partition_end();
        break;
    }
}

mutation_fragment_v2::mutation_fragment_v2(const schema& s, reader_permit permit, static_row&& r)
    : _kind(kind::static_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_static_row) static_row(std::move(r));
    reset_memory(s);
}

mutation_fragment_v2::mutation_fragment_v2(const schema& s, reader_permit permit, clustering_row&& r)
    : _kind(kind::clustering_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_clustering_row) clustering_row(std::move(r));
    reset_memory(s);
}

mutation_fragment_v2::mutation_fragment_v2(const schema& s, reader_permit permit, range_tombstone_change&& r)
    : _kind(kind::range_tombstone_change), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_range_tombstone_chg) range_tombstone_change(std::move(r));
    reset_memory(s);
}

mutation_fragment_v2::mutation_fragment_v2(const schema& s, reader_permit permit, partition_start&& r)
        : _kind(kind::partition_start), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_start) partition_start(std::move(r));
    reset_memory(s);
}

mutation_fragment_v2::mutation_fragment_v2(const schema& s, reader_permit permit, partition_end&& r)
        : _kind(kind::partition_end), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_end) partition_end(std::move(r));
    reset_memory(s);
}

void mutation_fragment_v2::destroy_data() noexcept
{
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.~clustering_row();
        break;
    case kind::range_tombstone_change:
        _data->_range_tombstone_chg.~range_tombstone_change();
        break;
    case kind::partition_start:
        _data->_partition_start.~partition_start();
        break;
    case kind::partition_end:
        _data->_partition_end.~partition_end();
        break;
    }
}

void mutation_fragment_v2::reset_memory(const schema& s, std::optional<reader_resources> res) {
    try {
        _data->_memory.reset_to(res ? *res : reader_resources::with_memory(calculate_memory_usage(s)));
    } catch (...) {
        destroy_data();
        throw;
    }
}

namespace {

struct get_key_visitor {
    const clustering_key_prefix& operator()(const clustering_row& cr) { return cr.key(); }
    const clustering_key_prefix& operator()(const range_tombstone& rt) { return rt.start; }
    const clustering_key_prefix& operator()(const range_tombstone_change& rt) { return rt.position().key(); }
    template <typename T>
    const clustering_key_prefix& operator()(const T&) { abort(); }
};

}

const clustering_key_prefix& mutation_fragment::key() const
{
    SCYLLA_ASSERT(has_key());
    return visit(get_key_visitor());
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    SCYLLA_ASSERT(mergeable_with(mf));
    switch (_kind) {
    case mutation_fragment::kind::partition_start:
        _data->_partition_start.partition_tombstone().apply(mf._data->_partition_start.partition_tombstone());
        mf._data->_partition_start.~partition_start();
        break;
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        mf._data->_clustering_row.~clustering_row();
        break;
    case mutation_fragment::kind::partition_end:
        // Nothing to do for this guy.
        mf._data->_partition_end.~partition_end();
        break;
    default: abort();
    }
    mf._data.reset();
    reset_memory(s);
}

position_in_partition_view mutation_fragment::position() const
{
    return visit([] (auto& mf) -> position_in_partition_view { return mf.position(); });
}

position_range mutation_fragment::range(const schema& s) const {
    switch (_kind) {
    case kind::static_row:
        return position_range::for_static_row();
    case kind::clustering_row:
        return position_range(position_in_partition(position()), position_in_partition::after_key(s, key()));
    case kind::partition_start:
        return position_range(position_in_partition(position()), position_in_partition::for_static_row());
    case kind::partition_end:
        return position_range(position_in_partition(position()), position_in_partition::after_all_clustered_rows());
    case kind::range_tombstone:
        auto&& rt = as_range_tombstone();
        return position_range(position_in_partition(rt.position()), position_in_partition(rt.end_position()));
    }
    abort();
}

auto fmt::formatter<mutation_fragment::kind>::format(mutation_fragment::kind k, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    switch (k) {
    case mutation_fragment::kind::static_row:
        name = "static row"; break;
    case mutation_fragment::kind::clustering_row:
        name = "clustering row"; break;
    case mutation_fragment::kind::range_tombstone:
        name = "range tombstone"; break;
    case mutation_fragment::kind::partition_start:
        name = "partition start"; break;
    case mutation_fragment::kind::partition_end:
        name = "partition end"; break;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}

auto fmt::formatter<mutation_fragment::printer>::format(const mutation_fragment::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto& mf = p._mutation_fragment;
    auto out = ctx.out();
    out = fmt::format_to(out, "{{mutation_fragment: {} {} ", mf._kind, mf.position());
    out = mf.visit(make_visitor(
        [&] (const clustering_row& cr) { return fmt::format_to(out, "{}", clustering_row::printer(p._schema, cr)); },
        [&] (const static_row& sr) { return fmt::format_to(out, "{}", static_row::printer(p._schema, sr)); },
        [&] (const auto& what) { return fmt::format_to(out, "{}", what); }
    ));
    return fmt::format_to(out, "}}");
}

const clustering_key_prefix& mutation_fragment_v2::key() const
{
    SCYLLA_ASSERT(has_key());
    return visit(get_key_visitor());
}

void mutation_fragment_v2::apply(const schema& s, mutation_fragment_v2&& mf)
{
    SCYLLA_ASSERT(mergeable_with(mf));
    switch (_kind) {
    case mutation_fragment_v2::kind::partition_start:
        _data->_partition_start.partition_tombstone().apply(mf._data->_partition_start.partition_tombstone());
        mf._data->_partition_start.~partition_start();
        break;
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        mf._data->_clustering_row.~clustering_row();
        break;
    case mutation_fragment_v2::kind::partition_end:
        // Nothing to do for this guy.
        mf._data->_partition_end.~partition_end();
        break;
    default: abort();
    }
    mf._data.reset();
    reset_memory(s);
}

position_in_partition_view mutation_fragment_v2::position() const
{
    return visit([] (auto& mf) -> position_in_partition_view { return mf.position(); });
}

std::ostream& operator<<(std::ostream& os, mutation_fragment_v2::kind k)
{
    fmt::print(os, "{}", k);
    return os;
}

auto fmt::formatter<mutation_fragment_v2::printer>::format(const mutation_fragment_v2::printer& p, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto& mf = p._mutation_fragment;
    auto out = ctx.out();
    out = fmt::format_to(out, "{{mutation_fragment: {} {} ", mf._kind, mf.position());
    out = mf.visit(make_visitor(
        [&] (const clustering_row& cr) { return fmt::format_to(out, "{}", clustering_row::printer(p._schema, cr)); },
        [&] (const static_row& sr) { return fmt::format_to(out, "{}", static_row::printer(p._schema, sr)); },
        [&] (const auto& what) { return fmt::format_to(out, "{}", what); }
    ));
    return fmt::format_to(out, "}}");
}

mutation_fragment_opt range_tombstone_stream::do_get_next()
{
    return mutation_fragment(_schema, _permit, _list.pop(_list.begin()));
}

mutation_fragment_opt range_tombstone_stream::get_next(const rows_entry& re)
{
    if (!_list.empty()) {
        return !_cmp(re.position(), _list.begin()->position()) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(const mutation_fragment& mf)
{
    if (!_list.empty()) {
        return !_cmp(mf.position(), _list.begin()->position()) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(position_in_partition_view upper_bound)
{
    if (!_list.empty()) {
        return _cmp(_list.begin()->position(), upper_bound) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next()
{
    if (!_list.empty()) {
        return do_get_next();
    }
    return { };
}

const range_tombstone& range_tombstone_stream::peek_next() const
{
    return _list.begin()->tombstone();
}

void range_tombstone_stream::forward_to(position_in_partition_view pos) {
    _list.erase_where([this, &pos] (const range_tombstone& rt) {
        return !_cmp(pos, rt.end_position());
    });
}

void range_tombstone_stream::reset() {
    _list.clear();
}

bool range_tombstone_stream::empty() const {
    return _list.empty();
}

position_range position_range::from_range(const query::clustering_range& range) {
    auto bv_range = bound_view::from_range(range);
    return {
        position_in_partition(position_in_partition::range_tag_t(), bv_range.first),
        position_in_partition(position_in_partition::range_tag_t(), bv_range.second)
    };
}

position_range::position_range(const query::clustering_range& range)
    : position_range(from_range(range))
{ }

position_range::position_range(query::clustering_range&& range)
    : position_range(range) // FIXME: optimize
{ }

bool mutation_fragment::relevant_for_range(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare cmp(s);
    if (!cmp(position(), pos)) {
        return true;
    }
    return relevant_for_range_assuming_after(s, pos);
}

bool mutation_fragment::relevant_for_range_assuming_after(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare cmp(s);
    // Range tombstones overlapping with the new range are let in
    return is_range_tombstone() && cmp(pos, as_range_tombstone().end_position());
}

bool mutation_fragment_v2::relevant_for_range(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare less(s);
    if (!less(position(), pos)) {
        return true;
    }
    return false;
}

template<typename Hasher>
void appending_hash<mutation_fragment>::operator()(Hasher& h, const mutation_fragment& mf, const schema& s) const {
    auto hash_cell = [&] (const column_definition& col, const atomic_cell_or_collection& cell) {
        feed_hash(h, col.kind);
        feed_hash(h, col.id);
        feed_hash(h, cell, col);
    };

    mf.visit(seastar::make_visitor(
        [&] (const clustering_row& cr) {
            feed_hash(h, cr.key(), s);
            feed_hash(h, cr.tomb());
            feed_hash(h, cr.marker());
            cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
                auto&& col = s.regular_column_at(id);
                hash_cell(col, cell);
            });
        },
        [&] (const static_row& sr) {
            sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
                auto&& col = s.static_column_at(id);
                hash_cell(col, cell);
            });
        },
        [&] (const range_tombstone& rt) {
            feed_hash(h, rt.start, s);
            feed_hash(h, rt.start_kind);
            feed_hash(h, rt.tomb);
            feed_hash(h, rt.end, s);
            feed_hash(h, rt.end_kind);
        },
        [&] (const partition_start& ps) {
            feed_hash(h, ps.key().key(), s);
            if (ps.partition_tombstone()) {
                feed_hash(h, ps.partition_tombstone());
            }
        },
        [&] (const partition_end& pe) {
            throw std::runtime_error("partition_end is not expected");
        }
    ));
}

// Instantiation for repair/row_level.cc
template void appending_hash<mutation_fragment>::operator()<xx_hasher>(xx_hasher& h, const mutation_fragment& cells, const schema& s) const;
