/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "bytes.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "gc_clock.hh"
#include "net/byteorder.hh"
#include <cstdint>
#include <iostream>

template<typename T>
static inline
void set_field(bytes& v, unsigned offset, T val) {
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
 *  <dead>  := <int8_t:    0><int64_t:timestamp><int32_t:expiry>
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
    // Can be called on live and dead cells. For dead cells, the result is never empty.
    static expiry_opt expiry(const bytes_view& cell) {
        auto flags = cell[0];
        if (flags == DEAD_FLAGS || (flags & EXPIRY_FLAG)) {
            auto expiry = get_field<int32_t>(cell, expiry_offset);
            return {gc_clock::time_point(gc_clock::duration(expiry))};
        }
        return {};
    }
    // Can be called only when is_live_and_has_ttl() is true.
    static gc_clock::duration ttl(const bytes_view& cell) {
        assert(is_live_and_has_ttl(cell));
        return gc_clock::duration(get_field<int32_t>(cell, ttl_offset));
    }
    static bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point expiry) {
        bytes b(bytes::initialized_later(), flags_size + timestamp_size + expiry_size);
        b[0] = DEAD_FLAGS;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, expiry_offset, expiry.time_since_epoch().count());
        return b;
    }
    static bytes make_live(api::timestamp_type timestamp, bytes_view value) {
        auto value_offset = flags_size + timestamp_size;
        bytes b(bytes::initialized_later(), value_offset + value.size());
        b[0] = LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    static bytes make_live(api::timestamp_type timestamp, bytes_view value, gc_clock::time_point expiry, gc_clock::duration ttl) {
        auto value_offset = flags_size + timestamp_size + expiry_size + ttl_size;
        bytes b(bytes::initialized_later(), value_offset + value.size());
        b[0] = EXPIRY_FLAG | LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, expiry_offset, expiry.time_since_epoch().count());
        set_field(b, ttl_offset, ttl.count());
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
    friend class atomic_cell_view;
    friend class atomic_cell;
};

class atomic_cell_view final {
    bytes_view _data;
private:
    atomic_cell_view(bytes_view data) : _data(data) {}
public:
    static atomic_cell_view from_bytes(bytes_view data) { return atomic_cell_view(data); }
    bool is_live() const {
        return atomic_cell_type::is_live(_data);
    }
    bool is_live(tombstone t) const {
        return is_live() && timestamp() > t.timestamp;
    }
    bool is_live_and_has_ttl() const {
        return atomic_cell_type::is_live_and_has_ttl(_data);
    }
    bool is_dead() const {
        return atomic_cell_type::is_dead(_data);
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return atomic_cell_type::timestamp(_data);
    }
    // Can be called on live cells only
    bytes_view value() const {
        return atomic_cell_type::value(_data);
    }
    // Can be called on live and dead cells. For dead cells, the result is never empty.
    expiry_opt expiry() const {
        return atomic_cell_type::expiry(_data);
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return atomic_cell_type::ttl(_data);
    }
    bytes_view serialize() const {
        return _data;
    }
    friend class atomic_cell;
    friend std::ostream& operator<<(std::ostream& os, const atomic_cell_view& acv);
};

class atomic_cell final {
    bytes _data;
private:
    atomic_cell(bytes b) : _data(std::move(b)) {}
public:
    atomic_cell(const atomic_cell&) = default;
    atomic_cell(atomic_cell&&) = default;
    atomic_cell& operator=(const atomic_cell&) = default;
    atomic_cell& operator=(atomic_cell&&) = default;
    static atomic_cell from_bytes(bytes b) {
        return atomic_cell(std::move(b));
    }
    atomic_cell(atomic_cell_view other) : _data(other._data.begin(), other._data.end()) {}
    bool is_live() const {
        return atomic_cell_type::is_live(_data);
    }
    bool is_live_and_has_ttl() const {
        return atomic_cell_type::is_live_and_has_ttl(_data);
    }
    bool is_dead() const {
        return atomic_cell_type::is_dead(_data);
    }
    // Can be called on live and dead cells
    api::timestamp_type timestamp() const {
        return atomic_cell_type::timestamp(_data);
    }
    // Can be called on live cells only
    bytes_view value() const {
        return atomic_cell_type::value(_data);
    }
    // Can be called on live and dead cells. For dead cells, the result is never empty.
    expiry_opt expiry() const {
        return atomic_cell_type::expiry(_data);
    }
    // Can be called only when is_live_and_has_ttl()
    gc_clock::duration ttl() const {
        return atomic_cell_type::ttl(_data);
    }
    bytes_view serialize() const {
        return _data;
    }
    operator atomic_cell_view() const {
        return atomic_cell_view(_data);
    }
    static atomic_cell make_dead(api::timestamp_type timestamp, gc_clock::time_point expiry) {
        return atomic_cell_type::make_dead(timestamp, expiry);
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

// Represents a mutation of a collection.  Actual format is determined by collection type,
// and is:
//   set:  list of atomic_cell
//   map:  list of pair<atomic_cell, bytes> (for key/value)
//   list: tbd, probably ugly
class collection_mutation {
public:
    struct view {
        bytes_view data;
        bytes_view serialize() const { return data; }
        static view from_bytes(bytes_view v) { return { v }; }
    };
    struct one {
        bytes data;
        one() {}
        one(bytes b) : data(std::move(b)) {}
        one(view v) : data(v.data.begin(), v.data.end()) {}
        operator view() const { return { data }; }
    };
};

namespace db {
template<typename T>
class serializer;
}

// A variant type that can hold either an atomic_cell, or a serialized collection.
// Which type is stored is determinied by the schema.
class atomic_cell_or_collection final {
    bytes _data;

    template<typename T>
    friend class db::serializer;
private:
    atomic_cell_or_collection() = default;
    atomic_cell_or_collection(bytes&& data) : _data(std::move(data)) {}
public:
    atomic_cell_or_collection(atomic_cell ac) : _data(std::move(ac._data)) {}
    static atomic_cell_or_collection from_atomic_cell(atomic_cell data) { return { std::move(data._data) }; }
    atomic_cell_view as_atomic_cell() const { return atomic_cell_view::from_bytes(_data); }
    atomic_cell_or_collection(collection_mutation::one cm) : _data(std::move(cm.data)) {}
    static atomic_cell_or_collection from_collection_mutation(collection_mutation::one data) {
        return std::move(data.data);
    }
    collection_mutation::view as_collection_mutation() const {
        return collection_mutation::view{_data};
    }
    bytes_view serialize() const {
        return _data;
    }
    friend std::ostream& operator<<(std::ostream&, const atomic_cell_or_collection&);
};

class column_definition;

int compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right);
void merge_column(const column_definition& def,
        atomic_cell_or_collection& old,
        const atomic_cell_or_collection& neww);
