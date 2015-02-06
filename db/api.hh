/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <boost/variant.hpp>
#include <cstdint>

#include "schema.hh"
#include "db_clock.hh"
#include "gc_clock.hh"

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
 *
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

    bool operator<(const tombstone& t) const {
        return timestamp < t.timestamp || ttl < t.ttl;
    }

    bool operator==(const tombstone& t) const {
        return timestamp == t.timestamp && ttl == t.ttl;
    }

    operator bool() const {
        return timestamp != api::missing_timestamp;
    }

    void apply(const tombstone& t) {
        if (*this < t) {
            *this = t;
        }
    }
};

using ttl_opt = std::experimental::optional<gc_clock::time_point>;

struct atomic_cell final {
    struct dead {
        gc_clock::time_point ttl;
    };
    struct live {
        ttl_opt ttl;
        bytes value;
    };
    api::timestamp_type timestamp;
    boost::variant<dead, live> value;
    bool is_live() const { return value.which() == 1; }
    // Call only when is_live() == true
    const live& as_live() const { return boost::get<live>(value); }
    // Call only when is_live() == false
    const dead& as_dead() const { return boost::get<dead>(value); }
};

using row = std::map<column_id, boost::any>;

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

    void apply(tombstone t) {
        _tombstone.apply(t);
    }

    void apply_delete(schema_ptr schema, const clustering_prefix& prefix, tombstone t) {
        if (prefix.empty()) {
            apply(t);
        } else if (prefix.size() == schema->clustering_key.size()) {
            _rows[serialize_value(*schema->clustering_key_type, prefix)].t.apply(t);
        } else {
            apply_row_tombstone(schema, serialize_value(*schema->clustering_key_prefix_type, prefix), t);
        }
    }

    void apply_row_tombstone(schema_ptr schema, const bytes& prefix, const tombstone& t) {
        auto i = _row_tombstones.lower_bound(prefix);
        if (i == _row_tombstones.end() || !schema->clustering_key_prefix_type->equal(prefix, i->first) || t > i->second) {
            _row_tombstones.insert(i, {prefix, t});
        }
    }

    void apply(schema_ptr schema, const mutation_partition& p);

    row& static_row() {
        return _static_row;
    }

    row& clustered_row(const clustering_key& key) {
        return _rows[key].cells;
    }

    row& clustered_row(clustering_key&& key) {
        return _rows[std::move(key)].cells;
    }

    row* find_row(const clustering_key& key) {
        auto i = _rows.find(key);
        if (i == _rows.end()) {
            return nullptr;
        }
        return &i->second.cells;
    }
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
    mutation(const mutation&) = delete;

    void set_static_cell(const column_definition& def, boost::any value) {
        p.static_row()[def.id] = std::move(value);
    }

    void set_clustered_cell(const clustering_prefix& prefix, const column_definition& def, boost::any value) {
        auto& row = p.clustered_row(serialize_value(*schema->clustering_key_type, prefix));
        row[def.id] = std::move(value);
    }

    void set_clustered_cell(const clustering_key& key, const column_definition& def, boost::any value) {
        auto& row = p.clustered_row(key);
        row[def.id] = std::move(value);
    }
};
