/*
 * Copyright (C) 2016-present ScyllaDB
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

#include <stack>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/util/defer.hh>

#include "mutation.hh"
#include "mutation_fragment.hh"
#include "clustering_interval_set.hh"

std::ostream&
operator<<(std::ostream& os, const clustering_row::printer& p) {
    auto& row = p._clustering_row;
    return os << "{clustering_row: ck " << row._ck << " dr "
              << deletable_row::printer(p._schema, row._row) << "}";
}

std::ostream&
operator<<(std::ostream& os, const static_row::printer& p) {
    return os << "{static_row: "<< row::printer(p._schema, column_kind::static_column, p._static_row._cells) << "}";
}

std::ostream&
operator<<(std::ostream& os, const partition_start& ph) {
    return os << "{partition_start: pk "<< ph._key << " partition_tombstone " << ph._partition_tombstone << "}";
}

std::ostream&
operator<<(std::ostream& os, const partition_end& eop) {
    return os << "{partition_end}";
}

std::ostream& operator<<(std::ostream& out, partition_region r) {
    switch (r) {
        case partition_region::partition_start: out << "partition_start"; break;
        case partition_region::static_row: out << "static_row"; break;
        case partition_region::clustered: out << "clustered"; break;
        case partition_region::partition_end: out << "partition_end"; break;
    }
    return out;
}

std::ostream& operator<<(std::ostream& os, position_in_partition_view::printer p) {
    auto& pos = p._pipv;
    fmt::print(os, "{{position: {},", pos._type);
    if (pos._ck) {
        fmt::print(os, "{}", clustering_key_prefix::with_schema_wrapper(p._schema, *pos._ck));
    } else {
        fmt::print(os, "null");
    }
    fmt::print(os, ", {}}}", int32_t(pos._bound_weight));
    return os;
}

std::ostream& operator<<(std::ostream& out, position_in_partition_view pos) {
    out << "{position: " << pos._type << ",";
    if (pos._ck) {
        out << *pos._ck;
    } else {
        out << "null";
    }
    return out << "," << int32_t(pos._bound_weight) << "}";
}

std::ostream& operator<<(std::ostream& out, const position_in_partition& pos) {
    return out << static_cast<position_in_partition_view>(pos);
}

std::ostream& operator<<(std::ostream& out, const position_range& range) {
    return out << "{" << range.start() << ", " << range.end() << "}";
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, static_row&& r)
    : _kind(kind::static_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_static_row) static_row(std::move(r));
    _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, clustering_row&& r)
    : _kind(kind::clustering_row), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_clustering_row) clustering_row(std::move(r));
    _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, range_tombstone&& r)
    : _kind(kind::range_tombstone), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_range_tombstone) range_tombstone(std::move(r));
    _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, partition_start&& r)
        : _kind(kind::partition_start), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_start) partition_start(std::move(r));
    _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
}

mutation_fragment::mutation_fragment(const schema& s, reader_permit permit, partition_end&& r)
        : _kind(kind::partition_end), _data(std::make_unique<data>(std::move(permit)))
{
    new (&_data->_partition_end) partition_end(std::move(r));
    _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
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

namespace {

struct get_key_visitor {
    const clustering_key_prefix& operator()(const clustering_row& cr) { return cr.key(); }
    const clustering_key_prefix& operator()(const range_tombstone& rt) { return rt.start; }
    template <typename T>
    const clustering_key_prefix& operator()(const T&) { abort(); }
};

}

const clustering_key_prefix& mutation_fragment::key() const
{
    assert(has_key());
    return visit(get_key_visitor());
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    assert(mergeable_with(mf));
    switch (_kind) {
    case mutation_fragment::kind::partition_start:
        _data->_partition_start.partition_tombstone().apply(mf._data->_partition_start.partition_tombstone());
        mf._data->_partition_start.~partition_start();
        break;
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        _data->_memory.reset(reader_resources::with_memory(calculate_memory_usage(s)));
        mf._data->_clustering_row.~clustering_row();
        break;
    case mutation_fragment::kind::partition_end:
        // Nothing to do for this guy.
        mf._data->_partition_end.~partition_end();
        break;
    default: abort();
    }
    mf._data.reset();
}

position_in_partition_view mutation_fragment::position() const
{
    return visit([] (auto& mf) -> position_in_partition_view { return mf.position(); });
}

position_range mutation_fragment::range() const {
    switch (_kind) {
    case kind::static_row:
        return position_range::for_static_row();
    case kind::clustering_row:
        return position_range(position_in_partition(position()), position_in_partition::after_key(key()));
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

std::ostream& operator<<(std::ostream& os, mutation_fragment::kind k)
{
    switch (k) {
    case mutation_fragment::kind::static_row: return os << "static row";
    case mutation_fragment::kind::clustering_row: return os << "clustering row";
    case mutation_fragment::kind::range_tombstone: return os << "range tombstone";
    case mutation_fragment::kind::partition_start: return os << "partition start";
    case mutation_fragment::kind::partition_end: return os << "partition end";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const mutation_fragment::printer& p) {
    auto& mf = p._mutation_fragment;
    os << "{mutation_fragment: " << mf._kind << " " << mf.position() << " ";
    mf.visit(make_visitor(
        [&] (const clustering_row& cr) { os << clustering_row::printer(p._schema, cr); },
        [&] (const static_row& sr) { os << static_row::printer(p._schema, sr); },
        [&] (const auto& what) -> void { os << what; }
    ));
    os << "}";
    return os;
}

mutation_fragment_opt range_tombstone_stream::do_get_next()
{
    return mutation_fragment(_schema, _permit, _list.pop_as<range_tombstone>(_list.begin()));
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

std::ostream& operator<<(std::ostream& out, const range_tombstone_stream& rtl) {
    return out << rtl._list;
}

std::ostream& operator<<(std::ostream& out, const clustering_interval_set& set) {
    return out << "{" << ::join(",\n  ", set) << "}";
}
