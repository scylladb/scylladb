/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "dht/i_partitioner.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include <functional>
#include <boost/any.hpp>
#include <cstdint>
#include <boost/variant.hpp>
#include <unordered_map>
#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "tuple.hh"
#include "core/future.hh"
#include "cql3/column_specification.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"

using partition_key_type = tuple_type<>;
using clustering_key_type = tuple_type<>;
using clustering_prefix_type = tuple_prefix;
using partition_key = bytes;
using clustering_key = bytes;
using clustering_prefix = clustering_prefix_type::value_type;

namespace api {

using timestamp_type = int64_t;
timestamp_type constexpr missing_timestamp = std::numeric_limits<timestamp_type>::min();
timestamp_type constexpr min_timestamp = std::numeric_limits<timestamp_type>::min() + 1;
timestamp_type constexpr max_timestamp = std::numeric_limits<timestamp_type>::max();

}

/**
 * Represents deletion operation. Can be commuted with other tombstones via apply() method.
 * Can be empty.
 */
struct tombstone final {
    api::timestamp_type timestamp;
    gc_clock::time_point ttl;

    tombstone(api::timestamp_type timestamp, gc_clock::time_point ttl)
        : timestamp(timestamp)
        , ttl(ttl)
    { }

    tombstone()
        : tombstone(api::missing_timestamp, {})
    { }

    int compare(const tombstone& t) const {
        if (timestamp < t.timestamp) {
            return -1;
        } else if (timestamp > t.timestamp) {
            return 1;
        } else if (ttl < t.ttl) {
            return -1;
        } else if (ttl > t.ttl) {
            return 1;
        } else {
            return 0;
        }
    }

    bool operator<(const tombstone& t) const {
        return compare(t) < 0;
    }

    bool operator<=(const tombstone& t) const {
        return compare(t) <= 0;
    }

    bool operator>(const tombstone& t) const {
        return compare(t) > 0;
    }

    bool operator>=(const tombstone& t) const {
        return compare(t) >= 0;
    }

    bool operator==(const tombstone& t) const {
        return compare(t) == 0;
    }

    bool operator!=(const tombstone& t) const {
        return compare(t) != 0;
    }

    explicit operator bool() const {
        return timestamp != api::missing_timestamp;
    }

    void apply(const tombstone& t) {
        if (*this < t) {
            *this = t;
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const tombstone& t) {
        return out << "{timestamp=" << t.timestamp << ", ttl=" << t.ttl.time_since_epoch().count() << "}";
    }
};

using ttl_opt = std::experimental::optional<gc_clock::time_point>;

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

/*
 * Represents atomic cell layout. Works on serialized form.
 *
 * Layout:
 *
 *  <live>  := <int8_t:flags><int64_t:timestamp><int32_t:ttl>?<value>
 *  <dead>  := <int8_t:    0><int64_t:timestamp><int32_t:ttl>
 */
class atomic_cell final {
private:
    static constexpr int8_t DEAD_FLAGS = 0;
    static constexpr int8_t LIVE_FLAG = 0x01;
    static constexpr int8_t TTL_FLAG  = 0x02; // When present, TTL field is present. Set only for live cells
    static constexpr unsigned flags_size = 1;
    static constexpr unsigned timestamp_offset = flags_size;
    static constexpr unsigned timestamp_size = 8;
    static constexpr unsigned ttl_offset = timestamp_offset + timestamp_size;
    static constexpr unsigned ttl_size = 4;
public:
    static bool is_live(const bytes_view& cell) {
        return cell[0] != DEAD_FLAGS;
    }
    static bool is_live_and_has_ttl(const bytes_view& cell) {
        return cell[0] & TTL_FLAG;
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
        auto ttl_field_size = bool(cell[0] & TTL_FLAG) * ttl_size;
        auto value_offset = flags_size + timestamp_size + ttl_field_size;
        cell.remove_prefix(value_offset);
        return cell;
    }
    // Can be called on live and dead cells. For dead cells, the result is never empty.
    static ttl_opt ttl(const bytes_view& cell) {
        auto flags = cell[0];
        if (flags == DEAD_FLAGS || (flags & TTL_FLAG)) {
            auto ttl = get_field<int32_t>(cell, ttl_offset);
            return {gc_clock::time_point(gc_clock::duration(ttl))};
        }
        return {};
    }
    static bytes make_dead(api::timestamp_type timestamp, gc_clock::time_point ttl) {
        bytes b(bytes::initialized_later(), flags_size + timestamp_size + ttl_size);
        b[0] = DEAD_FLAGS;
        set_field(b, timestamp_offset, timestamp);
        set_field(b, ttl_offset, ttl.time_since_epoch().count());
        return b;
    }
    static bytes make_live(api::timestamp_type timestamp, ttl_opt ttl, bytes_view value) {
        auto value_offset = flags_size + timestamp_size + bool(ttl) * ttl_size;
        bytes b(bytes::initialized_later(), value_offset + value.size());
        b[0] = (ttl ? TTL_FLAG : 0) | LIVE_FLAG;
        set_field(b, timestamp_offset, timestamp);
        if (ttl) {
            set_field(b, ttl_offset, ttl->time_since_epoch().count());
        }
        std::copy_n(value.begin(), value.size(), b.begin() + value_offset);
        return b;
    }
};

using row = std::map<column_id, bytes>;

struct deletable_row final {
    tombstone t;
    row cells;
};

using row_tombstone_set = std::map<bytes, tombstone, serialized_compare>;

class mutation_partition final {
private:
    tombstone _tombstone;
    row _static_row;
    std::map<clustering_key, deletable_row, key_compare> _rows;
    row_tombstone_set _row_tombstones;
public:
    mutation_partition(schema_ptr s)
        : _rows(key_compare(s->clustering_key_type))
        , _row_tombstones(serialized_compare(s->clustering_key_prefix_type))
    { }
    void apply(tombstone t) { _tombstone.apply(t); }
    void apply_delete(schema_ptr schema, const clustering_prefix& prefix, tombstone t);
    void apply_row_tombstone(schema_ptr schema, bytes prefix, tombstone t) {
        apply_row_tombstone(schema, {std::move(prefix), std::move(t)});
    }
    void apply_row_tombstone(schema_ptr schema, std::pair<bytes, tombstone> row_tombstone);
    void apply(schema_ptr schema, const mutation_partition& p);
    const row_tombstone_set& row_tombstones() const { return _row_tombstones; }
    row& static_row() { return _static_row; }
    row& clustered_row(const clustering_key& key) { return _rows[key].cells; }
    row& clustered_row(clustering_key&& key) { return _rows[std::move(key)].cells; }
    row* find_row(const clustering_key& key);
    tombstone tombstone_for_row(schema_ptr schema, const clustering_key& key);
    friend std::ostream& operator<<(std::ostream& os, const mutation_partition& mp);
};

class mutation final {
public:
    schema_ptr schema;
    partition_key key;
    mutation_partition p;
public:
    mutation(partition_key key_, schema_ptr schema_)
        : schema(std::move(schema_))
        , key(std::move(key_))
        , p(schema)
    { }

    mutation(mutation&&) = default;
    mutation(const mutation&) = default;

    void set_static_cell(const column_definition& def, bytes value) {
        p.static_row()[def.id] = std::move(value);
    }

    void set_clustered_cell(const clustering_prefix& prefix, const column_definition& def, bytes value) {
        auto& row = p.clustered_row(serialize_value(*schema->clustering_key_type, prefix));
        row[def.id] = std::move(value);
    }

    void set_clustered_cell(const clustering_key& key, const column_definition& def, bytes value) {
        auto& row = p.clustered_row(key);
        row[def.id] = std::move(value);
    }

    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};

struct column_family {
    column_family(schema_ptr schema);
    mutation_partition& find_or_create_partition(const bytes& key);
    row& find_or_create_row(const bytes& partition_key, const bytes& clustering_key);
    mutation_partition* find_partition(const bytes& key);
    row* find_row(const bytes& partition_key, const bytes& clustering_key);
    schema_ptr _schema;
    // partition key -> partition
    std::map<bytes, mutation_partition, key_compare> partitions;
    void apply(const mutation& m);
};

class keyspace {
public:
    std::unordered_map<sstring, column_family> column_families;
    static future<keyspace> populate(sstring datadir);
    schema_ptr find_schema(const sstring& cf_name);
    column_family* find_column_family(const sstring& cf_name);
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
public:
    std::unordered_map<sstring, keyspace> keyspaces;
    future<> init_from_data_directory(sstring datadir);
    static future<database> populate(sstring datadir);
    keyspace* find_keyspace(const sstring& name);
    future<> stop() { return make_ready_future<>(); }
    void assign(database&& db) {
        *this = std::move(db);
    }
    unsigned shard_of(const dht::token& t);
};

#endif /* DATABASE_HH_ */
