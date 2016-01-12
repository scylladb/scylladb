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

#include "bytes.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "gc_clock.hh"
#include "utils/managed_bytes.hh"
#include "net/byteorder.hh"
#include <cstdint>
#include <iostream>

template<typename T>
static inline
void set_field(managed_bytes& v, unsigned offset, T val) {
    reinterpret_cast<net::packed<T>*>(v.begin() + offset)->raw = net::hton(val);
}

template<typename T>
static inline
T get_field(const bytes_view& v, unsigned offset) {
    return net::ntoh(*reinterpret_cast<const net::packed<T>*>(v.begin() + offset));
}

class atomic_cell_or_collection;

/*
 * Represents atomic cell layout. Works on serialized form.
 *
 * Layout:
 *
 *  <live>  := <int8_t:flags><int64_t:timestamp>(<int32_t:expiry><int32_t:ttl>)?<value>
 *  <dead>  := <int8_t:    0><int64_t:timestamp><int32_t:deletion_time>
 */
class atomic_cell_type final {
private:
    static constexpr int8_t DEAD_FLAGS = 0;
    static constexpr int8_t LIVE_FLAG = 0x01;
    static constexpr int8_t EXPIRY_FLAG = 0x02; // When present, expiry field is present. Set only for live cells
    static constexpr unsigned flags_size = 1;
    static constexpr unsigned timestamp_offset = flags_size;
    static constexpr unsigned timestamp_size = 8;
    static constexpr unsigned expiry_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned expiry_size = 4;
    static constexpr unsigned deletion_time_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned deletion_time_size = 4;
    static constexpr unsigned ttl_offset = expiry_offset + expiry_size;
    static constexpr unsigned ttl_size = 4;
private:
    static bool is_live(const bytes_view& cell) {
        return cell[0] != DEAD_FLAGS;
    }
    static bool is_live_and_has_ttl(const bytes_view& cell) {
        return cell[0] & EXPIRY_FLAG;
    }
    static bool is_dead(const bytes_view& cell) {
        return cell[0] == DEAD_FLAGS;
    }
    // Can be called on live and dead cells
    static api::timestamp_type timestamp(const bytes_view& cell) {
        return get_field<api::timestamp_type>(cell, timestamp_offset);
    }
    // Can be called on live cells only
    static bytes_view value(bytes_view cell) {
        auto expiry_field_size = bool(cell[0] & EXPIRY_FLAG) * (expiry_size + ttl_size);
        auto value_offset = flags_size + timestamp_size + expiry_field_size;
        cell.remove_prefix(value_offset);
        return cell;
    }
    // Can be called only when is_dead() is true.
    static gc_clock::time_point deletion_time(const bytes_view& cell) {
        assert(is_dead(cell));
        return gc_clock::time_point(gc_clock::duration(
            get_field<int32_t>(cell, deletion_time_offset)));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::time_point expiry(const bytes_view& cell) {
        assert(is_live_and_has_ttl(cell));
        auto expiry = get_field<int32_t>(cell, expiry_offset);
        return gc_clock::time_point(gc_clock::duration(expiry));
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::duration ttl(const bytes_view& cell) {
        assert(is_live_and_has_ttl(cell));
        return gc_clock::duration(get_field<int32_t>(cell, ttl_offset));
    }
    static managed_bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        managed_bytes b(managed_bytes::initialized_later(), flags_size + timestamp_size + deletion_time_size);
        b[0] = DEAD_FLAGS;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, deletion_time_offset, deletion_time.time_since_epoch().count());
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value) {
        auto value_offset = flags_size + timestamp_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size());
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    static managed_bytes make_live(api::timestamp_type timestamp, bytes_view value, gc_clock::time_point expiry, gc_clock::duration ttl) {
        auto value_offset = flags_size + timestamp_size + expiry_size + ttl_size;
        managed_bytes b(managed_bytes::initialized_later(), value_offset + value.size());
        b[0] = EXPIRY_FLAG | LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, expiry_offset, expiry.time_since_epoch().count());
        set_field(b, ttl_offset, ttl.count());
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    template<typename ByteContainer>
    friend class atomic_cell_base;
    friend class atomic_cell;
};

template<typename ByteContainer>
class atomic_cell_base {
protected:
    ByteContainer _data;
protected:
    atomic_cell_base(ByteContainer&& data) : _data(std::forward<ByteContainer>(data)) { }
    atomic_cell_base(const ByteContainer& data) : _data(data) { }
public:
    bool is_live() const {
        return atomic_cell_type::is_live(_data);
    }
    bool is_live(tombstone t) const {
        return is_live() && !is_covered_by(t);
    }
    bool is_live(tombstone t, gc_clock::time_point now) const {
        return is_live() && !is_covered_by(t) && !has_expired(now);
    }
    bool is_live_and_has_ttl() const {
        return atomic_cell_type::is_live_and_has_ttl(_data);
    }
    bool is_dead(gc_clock::time_point now) const {
        return atomic_cell_type::is_dead(_data) || has_expired(now);
    }
    bool is_covered_by(tombstone t) const {
        return timestamp() <= t.timestamp;
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return atomic_cell_type::timestamp(_data);
    }
    // Can be called on live cells only
    bytes_view value() const {
        return atomic_cell_type::value(_data);
    }
    // Can be called only when is_dead(gc_clock::time_point)
    gc_clock::time_point deletion_time() const {
        return !is_live() ? atomic_cell_type::deletion_time(_data) : expiry() - ttl();
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::time_point expiry() const {
        return atomic_cell_type::expiry(_data);
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return atomic_cell_type::ttl(_data);
    }
    // Can be called on live and dead cells
    bool has_expired(gc_clock::time_point now) const {
        return is_live_and_has_ttl() && expiry() < now;
    }
    bytes_view serialize() const {
        return _data;
    }
};

class atomic_cell_view final : public atomic_cell_base<bytes_view> {
    atomic_cell_view(bytes_view data) : atomic_cell_base(data) {}
public:
    static atomic_cell_view from_bytes(bytes_view data) { return atomic_cell_view(data); }

    friend class atomic_cell;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);
};

class atomic_cell final : public atomic_cell_base<managed_bytes> {
    atomic_cell(managed_bytes b) : atomic_cell_base(std::move(b)) {}
public:
    atomic_cell(const atomic_cell&) = default;
    atomic_cell(atomic_cell&&) = default;
    atomic_cell& operator=(const atomic_cell&) = default;
    atomic_cell& operator=(atomic_cell&&) = default;
    static atomic_cell from_bytes(managed_bytes b) {
        return atomic_cell(std::move(b));
    }
    atomic_cell(atomic_cell_view other) : atomic_cell_base(managed_bytes{other._data}) {}
    operator atomic_cell_view() const {
        return atomic_cell_view(_data);
    }
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
        return atomic_cell_type::make_dead(timestamp, deletion_time);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value) {
        return atomic_cell_type::make_live(timestamp, value);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value,
        gc_clock::time_point expiry, gc_clock::duration ttl)
    {
        return atomic_cell_type::make_live(timestamp, value, expiry, ttl);
    }
    static atomic_cell make_live(api::timestamp_type timestamp, bytes_view value, ttl_opt ttl) {
        if (!ttl) {
            return atomic_cell_type::make_live(timestamp, value);
        } else {
            return atomic_cell_type::make_live(timestamp, value, gc_clock::now() + *ttl, *ttl);
        }
    }
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
    managed_bytes data;
    collection_mutation() {}
    collection_mutation(managed_bytes b) : data(std::move(b)) {}
    collection_mutation(collection_mutation_view v);
    operator collection_mutation_view() const;
};

class collection_mutation_view {
public:
    bytes_view data;
    bytes_view serialize() const { return data; }
    static collection_mutation_view from_bytes(bytes_view v) { return { v }; }
};

inline
collection_mutation::collection_mutation(collection_mutation_view v)
        : data(v.data) {
}

inline
collection_mutation::operator collection_mutation_view() const {
    return { data };
}

namespace db {
template<typename T>
class serializer;
}

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const column_definition& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
