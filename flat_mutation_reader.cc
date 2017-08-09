/*
 * Copyright (C) 2017 ScyllaDB
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

#include "flat_mutation_reader.hh"

#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>

void flat_mutation_reader::impl::forward_buffer_to(schema_ptr& schema, const position_in_partition& pos) {
    _buffer.erase(std::remove_if(_buffer.begin(), _buffer.end(), [this, &pos, &schema] (mutation_fragment& f) {
        return !f.relevant_for_range_assuming_after(*schema, pos);
    }), _buffer.end());

    _buffer_size = boost::accumulate(_buffer | boost::adaptors::transformed(std::mem_fn(&mutation_fragment::memory_usage)), size_t(0));
}