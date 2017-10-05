/*
 * Copyright (C) 2015 ScyllaDB
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
    atomic_cell_or_collection(atomic_cell_or_collection&&) = default;
    atomic_cell_or_collection(const atomic_cell_or_collection&) = delete;
    atomic_cell_or_collection& operator=(atomic_cell_or_collection&&) = default;
    atomic_cell_or_collection& operator=(const atomic_cell_or_collection&) = delete;
    atomic_cell_or_collection(atomic_cell ac) : _data(std::move(ac._data)) {}
    atomic_cell_or_collection(const abstract_type& at, atomic_cell_view acv) : atomic_cell_or_collection(atomic_cell(at, acv)) { }
    static atomic_cell_or_collection from_atomic_cell(atomic_cell data) { return { std::move(data._data) }; }
    atomic_cell_view as_atomic_cell(const column_definition& cdef) const { return atomic_cell_view::from_bytes(cdef.type->imr_state().type_info(), _data); }
    atomic_cell_ref as_atomic_cell_ref(const column_definition&) { return { _data }; }
    atomic_cell_mutable_view as_mutable_atomic_cell(const column_definition& cdef) { return atomic_cell_mutable_view::from_bytes(cdef.type->imr_state().type_info(), _data); }
    atomic_cell_or_collection(collection_mutation cm) : _data(std::move(cm.data)) {}
    atomic_cell_or_collection copy(const abstract_type&) const { return managed_bytes(_data); }
    explicit operator bool() const {
        return !_data.empty();
    }
    bool can_use_mutable_view() const {
        return !_data.is_fragmented();
    }
    static atomic_cell_or_collection from_collection_mutation(collection_mutation data) {
        return std::move(data.data);
    }
    collection_mutation_view as_collection_mutation() const {
        return collection_mutation_view{atomic_cell_value_view(bytes_view(_data))};
    }
    bytes_view serialize() const {
        return _data;
    }
    bool operator==(const atomic_cell_or_collection& other) const {
        return _data == other._data;
    }
    size_t external_memory_usage(const abstract_type&) const {
        return _data.external_memory_usage();
    }
    friend std::ostream& operator<<(std::ostream&, const atomic_cell_or_collection&);
};
