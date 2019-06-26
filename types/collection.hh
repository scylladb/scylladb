/*
 * Copyright (C) 2014 ScyllaDB
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

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <vector>

#include "types.hh"
#include "utils/chunked_vector.hh"

class collection_type_impl : public abstract_type {
    static logging::logger _logger;
public:
    static constexpr size_t max_elements = 65535;

    class kind {
        std::function<shared_ptr<cql3::column_specification> (shared_ptr<cql3::column_specification> collection, bool is_key)> _impl;
    public:
        kind(std::function<shared_ptr<cql3::column_specification> (shared_ptr<cql3::column_specification> collection, bool is_key)> impl)
            : _impl(std::move(impl)) {}
        shared_ptr<cql3::column_specification> make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const;
        static const kind map;
        static const kind set;
        static const kind list;
    };

    const kind& _kind;

protected:
    explicit collection_type_impl(sstring name, const kind& k)
            : abstract_type(std::move(name), {}, data::type_info::make_collection()), _kind(k) {}
public:
    // representation of a collection mutation, key/value pairs, value is a mutation itself
    struct mutation {
        tombstone tomb;
        utils::chunked_vector<std::pair<bytes, atomic_cell>> cells;
        // Expires cells based on query_time. Expires tombstones based on max_purgeable and gc_before.
        // Removes cells covered by tomb or this->tomb.
        bool compact_and_expire(row_tombstone tomb, gc_clock::time_point query_time,
            can_gc_fn&, gc_clock::time_point gc_before);
    };
    struct mutation_view {
        tombstone tomb;
        utils::chunked_vector<std::pair<bytes_view, atomic_cell_view>> cells;
        mutation materialize(const collection_type_impl&) const;
    };
    virtual data_type name_comparator() const = 0;
    virtual data_type value_comparator() const = 0;
    shared_ptr<cql3::column_specification> make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const;
    virtual bool is_collection() const override { return true; }
    bool is_map() const { return &_kind == &kind::map; }
    bool is_set() const { return &_kind == &kind::set; }
    bool is_list() const { return &_kind == &kind::list; }
    std::vector<atomic_cell> enforce_limit(std::vector<atomic_cell>, int version) const;
    virtual std::vector<bytes> serialized_values(std::vector<atomic_cell> cells) const = 0;
    bytes serialize_for_native_protocol(std::vector<atomic_cell> cells, int version) const;
    virtual bool is_compatible_with(const abstract_type& previous) const override;
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override;
    virtual bool is_compatible_with_frozen(const collection_type_impl& previous) const = 0;
    virtual bool is_value_compatible_with_frozen(const collection_type_impl& previous) const = 0;
    virtual bool is_native() const override { return false; }
    template <typename BytesViewIterator>
    static bytes pack(BytesViewIterator start, BytesViewIterator finish, int elements, cql_serialization_format sf);
    // requires linearized collection_mutation_view, lifetime
    mutation_view deserialize_mutation_form(bytes_view in) const;
    bool is_empty(collection_mutation_view in) const;
    bool is_any_live(collection_mutation_view in, tombstone tomb = tombstone(), gc_clock::time_point now = gc_clock::time_point::min()) const;
    api::timestamp_type last_update(collection_mutation_view in) const;
    virtual bytes to_value(mutation_view mut, cql_serialization_format sf) const = 0;
    bytes to_value(collection_mutation_view mut, cql_serialization_format sf) const;
    // FIXME: use iterators?
    collection_mutation serialize_mutation_form(const mutation& mut) const;
    collection_mutation serialize_mutation_form(mutation_view mut) const;
    collection_mutation serialize_mutation_form_only_live(mutation_view mut, gc_clock::time_point now) const;
    collection_mutation merge(collection_mutation_view a, collection_mutation_view b) const;
    collection_mutation difference(collection_mutation_view a, collection_mutation_view b) const;
    // Calls Func(atomic_cell_view) for each cell in this collection.
    // noexcept if Func doesn't throw.
    template<typename Func>
    void for_each_cell(collection_mutation_view c, Func&& func) const {
      c.data.with_linearized([&] (bytes_view c_bv) {
        auto m_view = deserialize_mutation_form(c_bv);
        for (auto&& c : m_view.cells) {
            func(std::move(c.second));
        }
      });
    }
    virtual void serialize(const void* value, bytes::iterator& out, cql_serialization_format sf) const = 0;
    virtual data_value deserialize(bytes_view v, cql_serialization_format sf) const = 0;
    data_value deserialize_value(bytes_view v, cql_serialization_format sf) const {
        return deserialize(v, sf);
    }
    bytes_opt reserialize(cql_serialization_format from, cql_serialization_format to, bytes_view_opt v) const;
};

template <typename BytesViewIterator>
bytes
collection_type_impl::pack(BytesViewIterator start, BytesViewIterator finish, int elements, cql_serialization_format sf) {
    size_t len = collection_size_len(sf);
    size_t psz = collection_value_len(sf);
    for (auto j = start; j != finish; j++) {
        len += j->size() + psz;
    }
    bytes out(bytes::initialized_later(), len);
    bytes::iterator i = out.begin();
    write_collection_size(i, elements, sf);
    while (start != finish) {
        write_collection_value(i, sf, *start++);
    }
    return out;
}
