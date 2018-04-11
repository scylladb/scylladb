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

#include "imr/utils.hh"

// A variant type that can hold either an atomic_cell, or a serialized collection.
// Which type is stored is determined by the schema.
class atomic_cell_or_collection final {
    // FIXME: This has made us lose small-buffer optimisation. Unfortunately,
    // due to the changed cell format it would be less effective now, anyway.
    // Measure the actual impact because any attempts to fix this will become
    // irrelevant once rows are converted to the IMR as well, so maybe we can
    // live with this like that.
    using imr_object_type = imr::utils::object<data::cell::structure>;
    imr_object_type _data;
private:
    atomic_cell_or_collection(imr::utils::object<data::cell::structure>&& data) : _data(std::move(data)) {}
public:
    atomic_cell_or_collection() = default;
    atomic_cell_or_collection(atomic_cell_or_collection&&) = default;
    atomic_cell_or_collection(const atomic_cell_or_collection&) = delete;
    atomic_cell_or_collection& operator=(atomic_cell_or_collection&&) = default;
    atomic_cell_or_collection& operator=(const atomic_cell_or_collection&) = delete;
    atomic_cell_or_collection(atomic_cell ac) : _data(std::move(ac._data)) {}
    atomic_cell_or_collection(const abstract_type& at, atomic_cell_view acv);
    static atomic_cell_or_collection from_atomic_cell(atomic_cell data) { return { std::move(data._data) }; }
    atomic_cell_view as_atomic_cell(const column_definition& cdef) const { return atomic_cell_view::from_bytes(cdef.type->imr_state().type_info(), _data); }
    atomic_cell_ref as_atomic_cell_ref(const column_definition& cdef) { return atomic_cell_mutable_view::from_bytes(cdef.type->imr_state().type_info(), _data); }
    atomic_cell_mutable_view as_mutable_atomic_cell(const column_definition& cdef) { return atomic_cell_mutable_view::from_bytes(cdef.type->imr_state().type_info(), _data); }
    atomic_cell_or_collection(collection_mutation cm) : _data(std::move(cm._data)) { }
    atomic_cell_or_collection copy(const abstract_type&) const;
    explicit operator bool() const {
        return bool(_data);
    }
    static constexpr bool can_use_mutable_view() {
        return true;
    }
    void swap(atomic_cell_or_collection& other) noexcept {
        _data.swap(other._data);
    }
    static atomic_cell_or_collection from_collection_mutation(collection_mutation data) { return std::move(data._data); }
    collection_mutation_view as_collection_mutation() const;
    bytes_view serialize() const;
    bool equals(const abstract_type& type, const atomic_cell_or_collection& other) const;
    size_t external_memory_usage(const abstract_type&) const;
    friend std::ostream& operator<<(std::ostream&, const atomic_cell_or_collection&);
};

namespace std {

inline void swap(atomic_cell_or_collection& a, atomic_cell_or_collection& b) noexcept
{
    a.swap(b);
}

}
