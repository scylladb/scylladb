/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <experimental/optional>
#include <limits>
#include <boost/variant/variant.hpp>

#include "db_clock.hh"
#include "gc_clock.hh"
#include "database.hh"
#include "db/consistency_level.hh"

namespace api {

using partition_key_type = tuple_type<>;
using clustering_key_type = tuple_type<>;
using clustering_prefix_type = tuple_prefix;
using partition_key = bytes;
using clustering_key = bytes;
using clustering_prefix = clustering_prefix_type::value_type;

using timestamp_type = int64_t;
timestamp_type constexpr missing_timestamp = std::numeric_limits<timestamp_type>::min();
timestamp_type constexpr min_timestamp = std::numeric_limits<timestamp_type>::min() + 1;
timestamp_type constexpr max_timestamp = std::numeric_limits<timestamp_type>::max();

/**
 * Represents deletion operation. Can be commuted with other tombstones via apply() method.
 * Can be empty.
 *
 */
struct tombstone final {
    timestamp_type timestamp;
    gc_clock::time_point ttl;

    tombstone(timestamp_type timestamp, gc_clock::time_point ttl)
        : timestamp(timestamp)
        , ttl(ttl)
    { }

    tombstone()
        : tombstone(missing_timestamp, {})
    { }

    bool operator<(const tombstone& t) const {
        return timestamp < t.timestamp || ttl < t.ttl;
    }

    bool operator==(const tombstone& t) const {
        return timestamp == t.timestamp && ttl == t.ttl;
    }

    operator bool() const {
        return timestamp != missing_timestamp;
    }

    void apply(const tombstone& t) {
        if (*this < t) {
            *this = t;
        }
    }
};

using ttl_opt = std::experimental::optional<gc_clock::time_point>;

class live_atomic_cell final {
private:
    timestamp_type _timestamp;
    ttl_opt _ttl;
    bytes _value;
public:
    live_atomic_cell(timestamp_type timestamp, ttl_opt ttl, bytes value)
        : _timestamp(timestamp)
        , _ttl(ttl)
        , _value(value) {
    }
};

using atomic_cell = boost::variant<tombstone, live_atomic_cell>;

using row = std::map<column_id, boost::any>;

struct deletable_row final {
    tombstone t;
    row cells;
};

struct row_tombstone final {
    tombstone t;

    /*
     * Prefix can be shorter than the clustering key size, in which case it
     * means that all rows whose keys have that prefix are removed.
     *
     * Empty prefix removes all rows, which is equivalent to removing the whole partition.
     */
    bytes prefix;
};

struct row_tombstone_compare final {
private:
    data_type _type;
public:
    row_tombstone_compare(data_type type) : _type(type) {}

    bool operator()(const row_tombstone& t1, const row_tombstone& t2) {
        return _type->less(t1.prefix, t2.prefix);
    }
};

class partition final {
private:
    schema_ptr _schema;
    tombstone _tombstone;

    row _static_row;
    std::map<clustering_key, deletable_row, key_compare> _rows;
    std::set<row_tombstone, row_tombstone_compare> _row_tombstones;
public:
    partition(schema_ptr s)
        : _schema(std::move(s))
        , _rows(key_compare(_schema->clustering_key_type))
        , _row_tombstones(row_tombstone_compare(_schema->clustering_key_prefix_type))
    { }

    void apply(tombstone t) {
        _tombstone.apply(t);
    }

    void apply_delete(const clustering_prefix& prefix, tombstone t) {
        if (prefix.empty()) {
            apply(t);
        } else if (prefix.size() == _schema->clustering_key.size()) {
            _rows[serialize_value(*_schema->clustering_key_type, prefix)].t.apply(t);
        } else {
            apply(row_tombstone{t, serialize_value(*_schema->clustering_key_prefix_type, prefix)});
        }
    }

    void apply(const row_tombstone& rt) {
        auto i = _row_tombstones.lower_bound(rt);
        if (i == _row_tombstones.end() || !_schema->clustering_key_prefix_type->equal(rt.prefix, i->prefix) || rt.t > i->t) {
            _row_tombstones.insert(i, rt);
        }
    }

    row& static_row() {
        return _static_row;
    }

    row& clustered_row(const clustering_key& key) {
        return _rows[key].cells;
    }

    row& get_row(const clustering_prefix& prefix) {
        if (prefix.empty()) {
            return static_row();
        }
        return clustered_row(serialize_value(*_schema->clustering_key_type, prefix));
    }
};

class mutation final {
public:
    schema_ptr schema;
    partition_key key;
    partition p;
public:
    mutation(partition_key key_, schema_ptr schema_)
        : schema(std::move(schema_))
        , key(std::move(key_))
        , p(schema)
    { }

    mutation(mutation&&) = default;
    mutation(const mutation&) = delete;

    void set_cell(const clustering_prefix& prefix, column_id col, boost::any value) {
        p.get_row(prefix)[col] = value;
    }
};

}
