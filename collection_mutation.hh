/*
 * Copyright (C) 2019 ScyllaDB
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

#include "utils/chunked_vector.hh"
#include "schema_fwd.hh"
#include "gc_clock.hh"
#include "atomic_cell.hh"
#include <iosfwd>
#include <forward_list>

class abstract_type;
class compaction_garbage_collector;
class row_tombstone;

class collection_mutation;

class cql_serialization_format;

// An auxiliary struct used to (de)construct collection_mutations.
// Unlike collection_mutation which is a serialized blob, this struct allows to inspect logical units of information
// (tombstone and cells) inside the mutation easily.
struct collection_mutation_description {
    tombstone tomb;
    // FIXME: use iterators?
    // we never iterate over `cells` more than once, so there is no need to store them in memory.
    // In some cases instead of constructing the `cells` vector, it would be more efficient to provide
    // a one-time-use forward iterator which returns the cells.
    utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;

    // Expires cells based on query_time. Expires tombstones based on max_purgeable and gc_before.
    // Removes cells covered by tomb or this->tomb.
    bool compact_and_expire(column_id id, row_tombstone tomb, gc_clock::time_point query_time,
        can_gc_fn&, gc_clock::time_point gc_before, compaction_garbage_collector* collector = nullptr);

    // Packs the data to a serialized blob.
    collection_mutation serialize(const abstract_type&) const;
};

// Similar to collection_mutation_description, except that it doesn't store the cells' data, only observes it.
struct collection_mutation_view_description {
    tombstone tomb;
    // FIXME: use iterators? See the fixme in collection_mutation_description; the same considerations apply here.
    utils::chunked_vector<std::pair<bytes_view, atomic_cell_view>> cells;

    // Copies the observed data, storing it in a collection_mutation_description.
    collection_mutation_description materialize(const abstract_type&) const;

    // Packs the data to a serialized blob.
    collection_mutation serialize(const abstract_type&) const;
};

class collection_mutation_input_stream {
    std::forward_list<bytes> _linearized;
    managed_bytes_view _src;
public:
    collection_mutation_input_stream(const managed_bytes_view& src) : _src(src) {}
    template <Trivial T>
    T read_trivial() {
        return ::read_simple<T>(_src);
    }
    bytes_view read_linearized(size_t n);
    managed_bytes_view read_fragmented(size_t n);
    bool empty() const;
};

// Given a collection_mutation_view, returns an auxiliary struct allowing the inspection of each cell.
// The function needs to be given the type of stored data to reconstruct the structural information.
collection_mutation_view_description deserialize_collection_mutation(const abstract_type&, collection_mutation_input_stream&);

class collection_mutation_view {
public:
    managed_bytes_view data;

    // Is this a noop mutation?
    bool is_empty() const;

    // Is any of the stored cells live (not deleted nor expired) at the time point `tp`,
    // given the later of the tombstones `t` and the one stored in the mutation (if any)?
    // Requires a type to reconstruct the structural information.
    bool is_any_live(const abstract_type&, tombstone t = tombstone(), gc_clock::time_point tp = gc_clock::time_point::min()) const;

    // The maximum of timestamps of the mutation's cells and tombstone.
    api::timestamp_type last_update(const abstract_type&) const;

    // Given a function that operates on a collection_mutation_view_description,
    // calls it on the corresponding description of `this`.
    template <typename F>
    inline decltype(auto) with_deserialized(const abstract_type& type, F f) const {
        collection_mutation_input_stream stream(data);
        return f(deserialize_collection_mutation(type, stream));
    }

    class printer {
        const abstract_type& _type;
        const collection_mutation_view& _cmv;
    public:
        printer(const abstract_type& type, const collection_mutation_view& cmv)
                : _type(type), _cmv(cmv) {}
        friend std::ostream& operator<<(std::ostream& os, const printer& cmvp);
    };
};

// A serialized mutation of a collection of cells.
// Used to represent mutations of collections (lists, maps, sets) or non-frozen user defined types.
// It contains a sequence of cells, each representing a mutation of a single entry (element or field) of the collection.
// Each cell has an associated 'key' (or 'path'). The meaning of each (key, cell) pair is:
//  for sets: the key is the serialized set element, the cell contains no data (except liveness information),
//  for maps: the key is the serialized map element's key, the cell contains the serialized map element's value,
//  for lists: the key is a timeuuid identifying the list entry, the cell contains the serialized value,
//  for user types: the key is an index identifying the field, the cell contains the value of the field.
//  The mutation may also contain a collection-wide tombstone.
class collection_mutation {
public:
    managed_bytes _data;

    collection_mutation() {}
    collection_mutation(const abstract_type&, collection_mutation_view);
    collection_mutation(const abstract_type&, managed_bytes);
    operator collection_mutation_view() const;
};

collection_mutation merge(const abstract_type&, collection_mutation_view, collection_mutation_view);

collection_mutation difference(const abstract_type&, collection_mutation_view, collection_mutation_view);

// Serializes the given collection of cells to a sequence of bytes ready to be sent over the CQL protocol.
bytes_ostream serialize_for_cql(const abstract_type&, collection_mutation_view, cql_serialization_format);
