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

#include "bytes.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "gc_clock.hh"
#include "utils/managed_bytes.hh"
#include "net/byteorder.hh"
#include <cstdint>
#include <iosfwd>
#include <seastar/util/gcc6-concepts.hh>
#include "data/cell.hh"
#include "data/schema_info.hh"
#include "imr/utils.hh"
#include "utils/fragmented_temporary_buffer.hh"

#include "serializer.hh"

class abstract_type;
class collection_type_impl;

using atomic_cell_value_view = data::value_view;
using atomic_cell_value_mutable_view = data::value_mutable_view;

/// View of an atomic cell
template<mutable_view is_mutable>
class basic_atomic_cell_view {
protected:
    data::cell::basic_atomic_cell_view<is_mutable> _view;
    friend class atomic_cell;
public:
    using pointer_type = std::conditional_t<is_mutable == mutable_view::no, const uint8_t*, uint8_t*>;
protected:
    explicit basic_atomic_cell_view(data::cell::basic_atomic_cell_view<is_mutable> v)
        : _view(std::move(v)) { }

    basic_atomic_cell_view(const data::type_info& ti, pointer_type ptr)
        : _view(data::cell::make_atomic_cell_view(ti, ptr))
    { }

    friend class atomic_cell_or_collection;
public:
    operator basic_atomic_cell_view<mutable_view::no>() const noexcept {
        return basic_atomic_cell_view<mutable_view::no>(_view);
    }

    void swap(basic_atomic_cell_view& other) noexcept {
        using std::swap;
        swap(_view, other._view);
    }

    bool is_counter_update() const {
        return _view.is_counter_update();
    }
    bool is_live() const {
        return _view.is_live();
    }
    bool is_live(tombstone t, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter);
    }
    bool is_live(tombstone t, gc_clock::time_point now, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter) && !has_expired(now);
    }
    bool is_live_and_has_ttl() const {
        return _view.is_expiring();
    }
    bool is_dead(gc_clock::time_point now) const {
        return !is_live() || has_expired(now);
    }
    bool is_covered_by(tombstone t, bool is_counter) const {
        return timestamp() <= t.timestamp || (is_counter && t.timestamp != api::missing_timestamp);
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return _view.timestamp();
    }
    void set_timestamp(api::timestamp_type ts) {
        _view.set_timestamp(ts);
    }
    // Can be called on live cells only
    data::basic_value_view<is_mutable> value() const {
        return _view.value();
    }
    // Can be called on live cells only
    size_t value_size() const {
        return _view.value_size();
    }
    bool is_value_fragmented() const {
        return _view.is_value_fragmented();
    }
    // Can be called on live counter update cells only
    int64_t counter_update_value() const {
        return _view.counter_update_value();
    }
    // Can be called only when is_dead(gc_clock::time_point)
    gc_clock::time_point deletion_time() const {
        return !is_live() ? _view.deletion_time() : expiry() - ttl();
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::time_point expiry() const {
        return _view.expiry();
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return _view.ttl();
    }
    // Can be called on live and dead cells
    bool has_expired(gc_clock::time_point now) const {
        return is_live_and_has_ttl() && expiry() <= now;
    }

    bytes_view serialize() const {
        return _view.serialize();
    }
};

class atomic_cell_view final : public basic_atomic_cell_view<mutable_view::no> {
    atomic_cell_view(const data::type_info& ti, const uint8_t* data)
        : basic_atomic_cell_view<mutable_view::no>(ti, data) {}

    template<mutable_view is_mutable>
    atomic_cell_view(data::cell::basic_atomic_cell_view<is_mutable> view)
        : basic_atomic_cell_view<mutable_view::no>(view) { }
    friend class atomic_cell;
public:
    static atomic_cell_view from_bytes(const data::type_info& ti, const imr::utils::object<data::cell::structure>& data) {
        return atomic_cell_view(ti, data.get());
    }

    static atomic_cell_view from_bytes(const data::type_info& ti, bytes_view bv) {
        return atomic_cell_view(ti, reinterpret_cast<const uint8_t*>(bv.begin()));
    }

    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);
};

class atomic_cell_mutable_view final : public basic_atomic_cell_view<mutable_view::yes> {
    atomic_cell_mutable_view(const data::type_info& ti, uint8_t* data)
        : basic_atomic_cell_view<mutable_view::yes>(ti, data) {}
public:
    static atomic_cell_mutable_view from_bytes(const data::type_info& ti, imr::utils::object<data::cell::structure>& data) {
        return atomic_cell_mutable_view(ti, data.get());
    }

    friend class atomic_cell;
};

using atomic_cell_ref = atomic_cell_mutable_view;

class atomic_cell final : public basic_atomic_cell_view<mutable_view::yes> {
    using imr_object_type =  imr::utils::object<data::cell::structure>;
    imr_object_type _data;
    atomic_cell(const data::type_info& ti, imr::utils::object<data::cell::structure>&& data)
        : basic_atomic_cell_view<mutable_view::yes>(ti, data.get()), _data(std::move(data)) {}
public:
    class collection_member_tag;
    using collection_member = bool_class<collection_member_tag>;

    atomic_cell(atomic_cell&&) = default;
    atomic_cell& operator=(const atomic_cell&) = delete;
    atomic_cell& operator=(atomic_cell&&) = default;
    void swap(atomic_cell& other) noexcept {
        basic_atomic_cell_view<mutable_view::yes>::swap(other);
        _data.swap(other._data);
    }
    operator atomic_cell_view() const { return atomic_cell_view(_view); }
    atomic_cell(const abstract_type& t, atomic_cell_view other);
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value,
                                 collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value,
                                 collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value,
                                 collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const bytes& value,
                                 collection_member cm = collection_member::no) {
        return make_live(type, timestamp, bytes_view(value), cm);
    }
    static atomic_cell make_live_counter_update(api::timestamp_type timestamp, int64_t value);
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, bytes_view value,
        gc_clock::time_point expiry, gc_clock::duration ttl, collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value,
        gc_clock::time_point expiry, gc_clock::duration ttl, collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value,
        gc_clock::time_point expiry, gc_clock::duration ttl, collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const bytes& value,
                                 gc_clock::time_point expiry, gc_clock::duration ttl, collection_member cm = collection_member::no)
    {
        return make_live(type, timestamp, bytes_view(value), expiry, ttl, cm);
    }
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value, ttl_opt ttl, collection_member cm = collection_member::no) {
        if (!ttl) {
            return make_live(type, timestamp, value, cm);
        } else {
            return make_live(type, timestamp, value, gc_clock::now() + *ttl, *ttl, cm);
        }
    }
    static atomic_cell make_live_uninitialized(const abstract_type& type, api::timestamp_type timestamp, size_t size);
    friend class atomic_cell_or_collection;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell& ac);
};

class collection_mutation_view;

// Represents a mutation of a collection.  Actual format is determined by collection type,
// and is:
//   set:  list of atomic_cell
//   map:  list of pair<atomic_cell, bytes> (for key/value)
//   list: tbd, probably ugly
class collection_mutation {
public:
    using imr_object_type =  imr::utils::object<data::cell::structure>;
    imr_object_type _data;

    collection_mutation() {}
    collection_mutation(const collection_type_impl&, collection_mutation_view v);
    collection_mutation(const collection_type_impl&, bytes_view bv);
    operator collection_mutation_view() const;
};


class collection_mutation_view {
public:
    atomic_cell_value_view data;
};

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const abstract_type& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
