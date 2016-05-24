/*
 * Copyright (C) 2016 ScyllaDB
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

#include <boost/range/algorithm/heap_algorithm.hpp>

#include "mutation.hh"
#include "streamed_mutation.hh"

mutation_fragment::mutation_fragment(static_row&& r)
    : _kind(kind::static_row), _data(std::make_unique<data>())
{
    new (&_data->_static_row) static_row(std::move(r));
}

mutation_fragment::mutation_fragment(clustering_row&& r)
    : _kind(kind::clustering_row), _data(std::make_unique<data>())
{
    new (&_data->_clustering_row) clustering_row(std::move(r));
}

mutation_fragment::mutation_fragment(range_tombstone_begin&& r)
    : _kind(kind::range_tombstone_begin), _data(std::make_unique<data>())
{
    new (&_data->_range_tombstone_begin) range_tombstone_begin(std::move(r));
}

mutation_fragment::mutation_fragment(range_tombstone_end&& r)
    : _kind(kind::range_tombstone_end), _data(std::make_unique<data>())
{
    new (&_data->_range_tombstone_end) range_tombstone_end(std::move(r));
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
    case kind::range_tombstone_begin:
        _data->_range_tombstone_begin.~range_tombstone_begin();
        break;
    case kind::range_tombstone_end:
        _data->_range_tombstone_end.~range_tombstone_end();
        break;
    }
}

const clustering_key_prefix& mutation_fragment::key() const
{
    assert(has_key());
    switch (_kind) {
    case kind::clustering_row:
        return as_clustering_row().key();
    case kind::range_tombstone_begin:
        return as_range_tombstone_begin().key();
    case kind::range_tombstone_end:
        return as_range_tombstone_end().key();
    default:
        abort();
    }
}

int mutation_fragment::bound_kind_weight() const {
    assert(has_key());
    switch (_kind) {
    case kind::clustering_row:
        return 0;
    case kind::range_tombstone_begin:
        return weight(as_range_tombstone_begin().bound().kind);
    case kind::range_tombstone_end:
        return weight(as_range_tombstone_end().bound().kind);
    default:
        abort();
    }
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    assert(_kind == mf._kind);
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        mf._data->_clustering_row.~clustering_row();
        break;
    case kind::range_tombstone_begin:
        _data->_range_tombstone_begin.apply(std::move(mf._data->_range_tombstone_begin));
        mf._data->_range_tombstone_begin.~range_tombstone_begin();
        break;
    case kind::range_tombstone_end:
        mf._data->_range_tombstone_end.~range_tombstone_end();
        break;
    }
    mf._data.reset();
}

position_in_partition mutation_fragment::position() const
{
    switch (_kind) {
    case kind::static_row:
        return _data->_static_row.position();
    case kind::clustering_row:
        return _data->_clustering_row.position();
    case kind::range_tombstone_begin:
        return _data->_range_tombstone_begin.position();
    case kind::range_tombstone_end:
        return _data->_range_tombstone_end.position();
    }
    abort();
}
