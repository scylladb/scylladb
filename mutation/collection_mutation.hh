/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/chunked_vector.hh"
#include "schema/schema_fwd.hh"
#include "gc_clock.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/compact_and_expire_result.hh"
#include "compaction/compaction_garbage_collector.hh"
#include <iosfwd>
#include <forward_list>
#include <iterator>
#include <optional>

class abstract_type;
class compaction_garbage_collector;
class row_tombstone;

class collection_mutation;

namespace ser {
class collection_cell_view;
}

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
    compact_and_expire_result compact_and_expire(column_id id, row_tombstone tomb, gc_clock::time_point query_time,
        can_gc_fn&, gc_clock::time_point gc_before, compaction_garbage_collector* collector = nullptr);

    // Packs the data to a serialized blob.
    collection_mutation serialize() const;
};

// Similar to collection_mutation_description, except that it doesn't store the cells' data, only observes it.
struct collection_mutation_view_description {
    tombstone tomb;
    // FIXME: use iterators? See the fixme in collection_mutation_description; the same considerations apply here.
    utils::chunked_vector<std::pair<bytes_view, atomic_cell_view>> cells;

    // Copies the observed data, storing it in a collection_mutation_description.
    collection_mutation_description materialize(const abstract_type&) const;

    // Packs the data to a serialized blob.
    collection_mutation serialize() const;
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
    bool empty() const;

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

    // Returns the collection-level tombstone, or an empty tombstone if none is present.
    tombstone tomb() const;

    // Returns the number of cells stored in the mutation.
    uint32_t size() const;

    // Forward iterator that deserializes cells on the fly.
    // Each element is a (key, value) pair where key is a managed_bytes_view of the serialized
    // cell key (path) and value is an atomic_cell_view of the serialized cell value.
    // The iterator does not require type information to advance.
    // The underlying collection_mutation_view must outlive the iterator.
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using iterator_concept = std::forward_iterator_tag;
        using value_type = std::pair<managed_bytes_view, atomic_cell_view>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;
    private:
        managed_bytes_view _remaining;
        uint32_t _remaining_count = 0;
        std::optional<value_type> _current;

        void advance();
        explicit iterator(managed_bytes_view data);
    public:
        // Default-constructs an end iterator.
        iterator() = default;

        reference operator*() const { return *_current; }
        pointer operator->() const { return &*_current; }

        iterator& operator++() {
            if (_remaining_count) {
                advance();
                --_remaining_count;
            } else {
                _current.reset();
            }
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        bool operator==(const iterator& o) const {
            // End iterator has _remaining = 0 and _current = nullopt.
            return _remaining_count == o._remaining_count && bool(_current) == bool(o._current);
        }

        friend class collection_mutation_view;
    };

    iterator begin() const;
    iterator end() const;

    class printer {
        const abstract_type& _type;
        const collection_mutation_view& _cmv;
    public:
        printer(const abstract_type& type, const collection_mutation_view& cmv)
                : _type(type), _cmv(cmv) {}
        friend fmt::formatter<printer>;
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

    collection_mutation();
    collection_mutation(collection_mutation_view);
    collection_mutation(managed_bytes);
    operator collection_mutation_view() const;
};

class collection_mutation_writer {
public:
    using value_type = std::pair<managed_bytes_view, atomic_cell_view>;

private:
    bytes_ostream _out;
    bytes::value_type* _size_buffer;

    tombstone _tomb;
    int32_t _size{0};
public:
    explicit collection_mutation_writer(tombstone tomb);

    bool empty() const {
        return !_tomb && _size == 0;
    }

    tombstone tombstone() const {
        return _tomb;
    }

    void push_back(managed_bytes_view key, atomic_cell_view value);
    void push_back(managed_bytes_view key, atomic_cell value) {
        push_back(std::move(key), atomic_cell_view(value));
    }

    void push_back(value_type kv) {
        push_back(std::move(kv.first), std::move(kv.second));
    }

    collection_mutation finish() &&;
};

collection_mutation merge(const abstract_type&, collection_mutation_view, collection_mutation_view);

collection_mutation difference(const abstract_type&, collection_mutation_view, collection_mutation_view);

// Transcode a collection from the IDL representation directly into the
// collection_mutation serialization format, without using any intermediary representation.
// Only the final collection-mutation blob is allocated, no intermediate allocations needed.
// Safe to use in LSA, it won't produce garbage.
collection_mutation read_from_collection_cell_view(const abstract_type&, const ser::collection_cell_view&);

// Serializes the given collection of cells to a sequence of bytes ready to be sent over the CQL protocol.
bytes_ostream serialize_for_cql(const abstract_type&, collection_mutation_view);

// Like serialize_for_cql, but uses an extended format that embeds per-element
// timestamps and expiries, for use with WRITETIME(col[key]) / TTL(col[key])
// and WRITETIME(col.field) / TTL(col.field) selectors.
// The format is: [cql-bytes-length as uint32][regular CQL bytes][count as int32]
// [per-element: (key-len as int32)(key bytes)(timestamp as int64)(expiry as int64 in gc_clock ticks, -1 if none)]
bytes_ostream serialize_for_cql_with_timestamps(const abstract_type&, collection_mutation_view);

template <>
struct fmt::formatter<collection_mutation_view::printer> : fmt::formatter<string_view> {
    auto format(const collection_mutation_view::printer&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
