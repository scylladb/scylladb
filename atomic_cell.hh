/*
 * Copyright (C) 2015-present ScyllaDB
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
#include "utils/fragment_range.hh"
#include <seastar/net//byteorder.hh>
#include <seastar/util/bool_class.hh>
#include <cstdint>
#include <iosfwd>
#include <concepts>
#include "utils/fragmented_temporary_buffer.hh"

#include "serializer.hh"

class abstract_type;
class collection_type_impl;
class atomic_cell_or_collection;

using atomic_cell_value = managed_bytes;
template <mutable_view is_mutable>
using atomic_cell_value_basic_view = managed_bytes_basic_view<is_mutable>;
using atomic_cell_value_view = atomic_cell_value_basic_view<mutable_view::no>;
using atomic_cell_value_mutable_view = atomic_cell_value_basic_view<mutable_view::yes>;

template <typename T>
requires std::is_trivial_v<T>
static void set_field(atomic_cell_value_mutable_view& out, unsigned offset, T val) {
    auto out_view = managed_bytes_mutable_view(out);
    out_view.remove_prefix(offset);
    write<T>(out_view, val);
}

template <typename T>
requires std::is_trivial_v<T>
static void set_field(atomic_cell_value& out, unsigned offset, T val) {
    auto out_view = atomic_cell_value_mutable_view(out);
    set_field(out_view, offset, val);
}

template <FragmentRange Buffer>
static void set_value(managed_bytes& b, unsigned value_offset, const Buffer& value) {
    auto v = managed_bytes_mutable_view(b).substr(value_offset, value.size_bytes());
    for (auto frag : value) {
        write_fragmented(v, single_fragmented_view(frag));
    }
}

template <typename T, FragmentedView Input>
requires std::is_trivial_v<T>
static T get_field(Input in, unsigned offset = 0) {
    in.remove_prefix(offset);
    return read_simple<T>(in);
}

/*
 * Represents atomic cell layout. Works on serialized form.
 *
 * Layout:
 *
 *  <live>  := <int8_t:flags><int64_t:timestamp>(<int64_t:expiry><int32_t:ttl>)?<value>
 *  <dead>  := <int8_t:    0><int64_t:timestamp><int64_t:deletion_time>
 */
class atomic_cell_type final {
private:
    static constexpr int8_t LIVE_FLAG = 0x01;
    static constexpr int8_t EXPIRY_FLAG = 0x02; // When present, expiry field is present. Set only for live cells
    static constexpr int8_t COUNTER_UPDATE_FLAG = 0x08; // Cell is a counter update.
    static constexpr unsigned flags_size = 1;
    static constexpr unsigned timestamp_offset = flags_size;
    static constexpr unsigned timestamp_size = 8;
    static constexpr unsigned expiry_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned expiry_size = 8;
    static constexpr unsigned deletion_time_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned deletion_time_size = 8;
    static constexpr unsigned ttl_offset = expiry_offset + expiry_size;
    static constexpr unsigned ttl_size = 4;
    friend class counter_cell_builder;
private:
    static bool is_counter_update(atomic_cell_value_view cell) {
        return cell.front() & COUNTER_UPDATE_FLAG;
    }
    static bool is_live(atomic_cell_value_view cell) {
        return cell.front() & LIVE_FLAG;
    }
    static bool is_live_and_has_ttl(atomic_cell_value_view cell) {
        return cell.front() & EXPIRY_FLAG;
    }
    static bool is_dead(atomic_cell_value_view cell) {
        return !is_live(cell);
    }
    // Can be called on live and dead cells
    static api::timestamp_type timestamp(atomic_cell_value_view cell) {
        return get_field<api::timestamp_type>(cell, timestamp_offset);
    }
    static void set_timestamp(atomic_cell_value_mutable_view& cell, api::timestamp_type ts) {
        set_field(cell, timestamp_offset, ts);
    }
    // Can be called on live cells only
private:
    template <mutable_view is_mutable>
    static managed_bytes_basic_view<is_mutable> do_get_value(managed_bytes_basic_view<is_mutable> cell) {
        auto expiry_field_size = bool(cell.front() & EXPIRY_FLAG) * (expiry_size + ttl_size);
        auto value_offset = flags_size + timestamp_size + expiry_field_size;
        cell.remove_prefix(value_offset);
        return cell;
    }
public:
    static atomic_cell_value_view value(managed_bytes_view cell) {
        return do_get_value(cell);
    }
    static atomic_cell_value_mutable_view value(managed_bytes_mutable_view cell) {
        return do_get_value(cell);
    }
    // Can be called on live counter update cells only
    static int64_t counter_update_value(atomic_cell_value_view cell) {
        return get_field<int64_t>(cell, flags_size + timestamp_size);
    }
    // Can be called only when is_dead() is true.
    static gc_clock::time_point deletion_time(atomic_cell_value_view cell) {
        assert(is_dead(cell));
        return gc_clock::time_point(gc_clock::duration(get_field<int64_t>(cell, deletion_time_offset)));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::time_point expiry(atomic_cell_value_view cell) {
        assert(is_live_and_has_ttl(cell));
        auto expiry = get_field<int64_t>(cell, expiry_offset);
        return gc_clock::time_point(gc_clock::duration(expiry));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::duration ttl(atomic_cell_value_view cell) {
        assert(is_live_and_has_ttl(cell));
        return gc_clock::duration(get_field<int32_t>(cell, ttl_offset));
    }
    static managed_bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        managed_bytes b(managed_bytes::initialized_later(), flags_size + timestamp_size + deletion_time_size);
        b[0] = 0;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, deletion_time_offset, static_cast<int64_t>(deletion_time.time_since_epoch().count()));
        return b;
    }
    template <FragmentRange Buffer>
    static managed_bytes make_live(api::timestamp_type timestamp, const Buffer& value) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size_bytes());
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_value(b, value_offset, value);
        return b;
    }
    static managed_bytes make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + sizeof(value));
        b[0] = LIVE_FLAG | COUNTER_UPDATE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, value_offset, value);
        return b;
    }
    template <FragmentRange Buffer>
    static managed_bytes make_live(api::timestamp_type timestamp, const Buffer& value, gc_clock::time_point expiry, gc_clock::duration ttl) {
        auto value_offset = flags_size + timestamp_size + expiry_size + ttl_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size_bytes());
        b[0] = EXPIRY_FLAG | LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, expiry_offset, static_cast<int64_t>(expiry.time_since_epoch().count()));
        set_field(b, ttl_offset, static_cast<int32_t>(ttl.count()));
        set_value(b, value_offset, value);
        return b;
    }
    static managed_bytes make_live_uninitialized(api::timestamp_type timestamp, size_t size) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + size);
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        return b;
    }
    template <mutable_view is_mutable>
    friend class basic_atomic_cell_view;
    friend class atomic_cell;
};

/// View of an atomic cell
template<mutable_view is_mutable>
class basic_atomic_cell_view {
protected:
    managed_bytes_basic_view<is_mutable> _view;
	friend class atomic_cell;
protected:
    void set_view(managed_bytes_basic_view<is_mutable> v) {
        _view = v;
    }
    basic_atomic_cell_view() = default;
    explicit basic_atomic_cell_view(managed_bytes_basic_view<is_mutable> v) : _view(std::move(v)) { }
    friend class atomic_cell_or_collection;
public:
    operator basic_atomic_cell_view<mutable_view::no>() const noexcept {
        return basic_atomic_cell_view<mutable_view::no>(_view);
    }

    bool is_counter_update() const {
        return atomic_cell_type::is_counter_update(_view);
    }
    bool is_live() const {
        return atomic_cell_type::is_live(_view);
    }
    bool is_live(tombstone t, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter);
    }
    bool is_live(tombstone t, gc_clock::time_point now, bool is_counter) const {
        return is_live() && !is_covered_by(t, is_counter) && !has_expired(now);
    }
    bool is_live_and_has_ttl() const {
        return atomic_cell_type::is_live_and_has_ttl(_view);
    }
    bool is_dead(gc_clock::time_point now) const {
        return atomic_cell_type::is_dead(_view) || has_expired(now);
    }
    bool is_covered_by(tombstone t, bool is_counter) const {
        return timestamp() <= t.timestamp || (is_counter && t.timestamp != api::missing_timestamp);
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return atomic_cell_type::timestamp(_view);
    }
    void set_timestamp(api::timestamp_type ts) {
        atomic_cell_type::set_timestamp(_view, ts);
    }
    // Can be called on live cells only
    atomic_cell_value_basic_view<is_mutable> value() const {
        return atomic_cell_type::value(_view);
    }
    // Can be called on live cells only
    size_t value_size() const {
        return atomic_cell_type::value(_view).size();
    }
    // Can be called on live counter update cells only
    int64_t counter_update_value() const {
        return atomic_cell_type::counter_update_value(_view);
    }
    // Can be called only when is_dead(gc_clock::time_point)
    gc_clock::time_point deletion_time() const {
        return !is_live() ? atomic_cell_type::deletion_time(_view) : expiry() - ttl();
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::time_point expiry() const {
        return atomic_cell_type::expiry(_view);
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return atomic_cell_type::ttl(_view);
    }
    // Can be called on live and dead cells
    bool has_expired(gc_clock::time_point now) const {
        return is_live_and_has_ttl() && expiry() <= now;
    }

    managed_bytes_view serialize() const {
        return _view;
    }
};

class atomic_cell_view final : public basic_atomic_cell_view<mutable_view::no> {
    atomic_cell_view(managed_bytes_view v)
        : basic_atomic_cell_view(v) {}

    template<mutable_view is_mutable>
    atomic_cell_view(basic_atomic_cell_view<is_mutable> view)
        : basic_atomic_cell_view<mutable_view::no>(view) {}
    friend class atomic_cell;
public:
    static atomic_cell_view from_bytes(const abstract_type& t, managed_bytes_view v) {
        return atomic_cell_view(v);
    }
    static atomic_cell_view from_bytes(const abstract_type& t, bytes_view v) {
        return atomic_cell_view(managed_bytes_view(v));
    }

    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);

    class printer {
        const abstract_type& _type;
        const atomic_cell_view& _cell;
    public:
        printer(const abstract_type& type, const atomic_cell_view& cell) : _type(type), _cell(cell) {}
        friend std::ostream& operator<<(std::ostream& os, const printer& acvp);
    };
};

class atomic_cell_mutable_view final : public basic_atomic_cell_view<mutable_view::yes> {
    atomic_cell_mutable_view(managed_bytes_mutable_view data)
        : basic_atomic_cell_view(data) {}
public:
    static atomic_cell_mutable_view from_bytes(const abstract_type& t, managed_bytes_mutable_view v) {
        return atomic_cell_mutable_view(v);
    }

    friend class atomic_cell;
};

using atomic_cell_ref = atomic_cell_mutable_view;

class atomic_cell final : public basic_atomic_cell_view<mutable_view::yes> {
    managed_bytes _data;
    atomic_cell(managed_bytes b) : _data(std::move(b))  {
        set_view(_data);
    }

public:
    class collection_member_tag;
    using collection_member = bool_class<collection_member_tag>;

    atomic_cell(atomic_cell&& o) noexcept : _data(std::move(o._data)) {
        set_view(_data);
    }
    atomic_cell& operator=(const atomic_cell&) = delete;
    atomic_cell& operator=(atomic_cell&& o) {
        _data = std::move(o._data);
        set_view(_data);
        return *this;
    }
    operator atomic_cell_view() const { return atomic_cell_view(managed_bytes_view(_data)); }
    atomic_cell(const abstract_type& t, atomic_cell_view other);
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value,
                                 collection_member = collection_member::no);
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, managed_bytes_view value,
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
    static atomic_cell make_live(const abstract_type&, api::timestamp_type timestamp, managed_bytes_view value,
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
    static atomic_cell make_live(const abstract_type& type, api::timestamp_type timestamp, const managed_bytes_view& value, ttl_opt ttl, collection_member cm = collection_member::no) {
        if (!ttl) {
            return make_live(type, timestamp, value, cm);
        } else {
            return make_live(type, timestamp, value, gc_clock::now() + *ttl, *ttl, cm);
        }
    }
    static atomic_cell make_live_uninitialized(const abstract_type& type, api::timestamp_type timestamp, size_t size);
    friend class atomic_cell_or_collection;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell& ac);

    class printer : atomic_cell_view::printer {
    public:
        printer(const abstract_type& type, const atomic_cell_view& cell) : atomic_cell_view::printer(type, cell) {}
        friend std::ostream& operator<<(std::ostream& os, const printer& acvp);
    };
};

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const abstract_type& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
