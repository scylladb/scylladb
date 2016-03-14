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

#pragma once

// Not part of atomic_cell.hh to avoid cyclic dependency between types.hh and atomic_cell.hh

#include "types.hh"
#include "atomic_cell.hh"
#include "hashing.hh"

template<>
struct appending_hash<collection_mutation_view> {
    template<typename Hasher>
    void operator()(Hasher& h, collection_mutation_view cell) const {
        auto m_view = collection_type_impl::deserialize_mutation_form(cell);
        ::feed_hash(h, m_view.tomb);
        for (auto&& key_and_value : m_view.cells) {
            ::feed_hash(h, key_and_value.first);
            ::feed_hash(h, key_and_value.second);
        }
    }
};

template<>
struct appending_hash<atomic_cell_view> {
    template<typename Hasher>
    void operator()(Hasher& h, atomic_cell_view cell) const {
        feed_hash(h, cell.is_live());
        feed_hash(h, cell.timestamp());
        if (cell.is_live()) {
            if (cell.is_live_and_has_ttl()) {
                feed_hash(h, cell.expiry());
                feed_hash(h, cell.ttl());
            }
            feed_hash(h, cell.value());
        } else {
            feed_hash(h, cell.deletion_time());
        }
    }
};
