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

#include "atomic_cell.hh"
#include "schema.hh"
#include "hashing.hh"

// A variant type that can hold either an atomic_cell, or a serialized collection.
// Which type is stored is determined by the schema.
// Has an "empty" state.
// Objects moved-from are left in an empty state.
class atomic_cell_or_collection final {
    managed_bytes _data;
private:
    atomic_cell_or_collection(managed_bytes&& data) : _data(std::move(data)) {}
public:
    atomic_cell_or_collection() = default;
    atomic_cell_or_collection(atomic_cell ac) : _data(std::move(ac._data)) {}
    static atomic_cell_or_collection from_atomic_cell(atomic_cell data) { return { std::move(data._data) }; }
    atomic_cell_view as_atomic_cell() const { return atomic_cell_view::from_bytes(_data); }
    atomic_cell_ref as_atomic_cell_ref() { return { _data }; }
    atomic_cell_or_collection(collection_mutation cm) : _data(std::move(cm.data)) {}
    explicit operator bool() const {
        return !_data.empty();
    }
    static atomic_cell_or_collection from_collection_mutation(collection_mutation data) {
        return std::move(data.data);
    }
    collection_mutation_view as_collection_mutation() const {
        return collection_mutation_view{_data};
    }
    bytes_view serialize() const {
        return _data;
    }
    bool operator==(const atomic_cell_or_collection& other) const {
        return _data == other._data;
    }
    template<typename Hasher>
    void feed_hash(Hasher& h, const column_definition& def) const {
        if (def.is_atomic()) {
            ::feed_hash(h, as_atomic_cell());
        } else {
            ::feed_hash(as_collection_mutation(), h, def.type);
        }
    }
    friend std::ostream& operator<<(std::ostream&, const atomic_cell_or_collection&);
};
