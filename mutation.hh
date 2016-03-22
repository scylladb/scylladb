/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
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

#include <iostream>

#include "mutation_partition.hh"
#include "keys.hh"
#include "schema.hh"
#include "dht/i_partitioner.hh"
#include "hashing.hh"

class mutation final {
private:
    struct data {
        schema_ptr _schema;
        dht::decorated_key _dk;
        mutation_partition _p;

        data(dht::decorated_key&& key, schema_ptr&& schema);
        data(partition_key&& key, schema_ptr&& schema);
        data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp);
        data(schema_ptr&& schema, dht::decorated_key&& key, mutation_partition&& mp);
    };
    std::unique_ptr<data> _ptr;
private:
    mutation() = default;
public:
    mutation(dht::decorated_key key, schema_ptr schema)
        : _ptr(std::make_unique<data>(std::move(key), std::move(schema)))
    { }
    mutation(partition_key key_, schema_ptr schema)
        : _ptr(std::make_unique<data>(std::move(key_), std::move(schema)))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, const mutation_partition& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), mp))
    { }
    mutation(schema_ptr schema, dht::decorated_key key, mutation_partition&& mp)
        : _ptr(std::make_unique<data>(std::move(schema), std::move(key), std::move(mp)))
    { }
    mutation(const mutation& m)
        : _ptr(std::make_unique<data>(schema_ptr(m.schema()), dht::decorated_key(m.decorated_key()), m.partition()))
    { }
    mutation(mutation&&) = default;
    mutation& operator=(mutation&& x) = default;
    mutation& operator=(const mutation& m);

    void set_static_cell(const column_definition& def, atomic_cell_or_collection&& value);
    void set_static_cell(const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value);
    void set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value);
    void set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value);

    // Upgrades this mutation to a newer schema. The new schema must
    // be obtained using only valid schema transformation:
    //  * primary key column count must not change
    //  * column types may only change to those with compatible representations
    //
    // After upgrade, mutation's partition should only be accessed using the new schema. User must
    // ensure proper isolation of accesses.
    //
    // Strong exception guarantees.
    //
    // Note that the conversion may lose information, it's possible that m1 != m2 after:
    //
    //   auto m2 = m1;
    //   m2.upgrade(s2);
    //   m2.upgrade(m1.schema());
    //
    void upgrade(const schema_ptr&);

    std::experimental::optional<atomic_cell_or_collection> get_cell(const clustering_key& rkey, const column_definition& def) const;
    const partition_key& key() const { return _ptr->_dk._key; };
    const dht::decorated_key& decorated_key() const { return _ptr->_dk; };
    dht::ring_position ring_position() const { return { decorated_key() }; }
    const dht::token& token() const { return _ptr->_dk._token; }
    const schema_ptr& schema() const { return _ptr->_schema; }
    const mutation_partition& partition() const { return _ptr->_p; }
    mutation_partition& partition() { return _ptr->_p; }
    const utils::UUID& column_family_id() const { return _ptr->_schema->id(); }
    // Consistent with hash<canonical_mutation>
    bool operator==(const mutation&) const;
    bool operator!=(const mutation&) const;
public:
    // The supplied partition_slice must be governed by this mutation's schema
    query::result query(const query::partition_slice&, query::result_request request = query::result_request::only_result,
        gc_clock::time_point now = gc_clock::now(), uint32_t row_limit = query::max_rows) const;

    // See mutation_partition::live_row_count()
    size_t live_row_count(gc_clock::time_point query_time = gc_clock::time_point::min()) const;

    void apply(mutation&&);
    void apply(const mutation&);
private:
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
    friend class mutation_opt;
};

struct mutation_decorated_key_less_comparator {
    bool operator()(const mutation& m1, const mutation& m2) const;
};

class mutation_opt {
private:
    mutation _mutation;
public:
    mutation_opt() = default;
    mutation_opt(std::experimental::nullopt_t) noexcept { }
    mutation_opt(const mutation& obj) : _mutation(obj) { }
    mutation_opt(mutation&& obj) noexcept : _mutation(std::move(obj)) { }
    mutation_opt(std::experimental::optional<mutation>&& obj) noexcept {
        if (obj) {
            _mutation = std::move(*obj);
        }
    }
    mutation_opt(const mutation_opt&) = default;
    mutation_opt(mutation_opt&&) = default;

    mutation_opt& operator=(std::experimental::nullopt_t) noexcept {
        _mutation = mutation();
        return *this;
    }
    template<typename T>
    std::enable_if_t<std::is_same<std::decay_t<T>, mutation>::value, mutation_opt&>
    operator=(T&& obj) noexcept {
        _mutation = std::forward<T>(obj);
        return *this;
    }
    mutation_opt& operator=(mutation_opt&&) = default;

    explicit operator bool() const noexcept {
        return bool(_mutation._ptr);
    }

    mutation* operator->() noexcept { return &_mutation; }
    const mutation* operator->() const noexcept { return &_mutation; }

    mutation& operator*() noexcept { return _mutation; }
    const mutation& operator*() const noexcept { return _mutation; }

    bool operator==(const mutation_opt& other) const {
        if (!*this && !other) {
            return true;
        }
        if (!*this || !other) {
            return false;
        }
        return _mutation == other._mutation;
    }
    bool operator!=(const mutation_opt& other) const {
        return !(*this == other);
    }
};

// Consistent with operator==()
// Consistent across the cluster, so should not rely on particular
// serialization format, only on actual data stored.
template<>
struct appending_hash<mutation> {
    template<typename Hasher>
    void operator()(Hasher& h, const mutation& m) const {
        const schema& s = *m.schema();
        m.key().feed_hash(h, s);
        m.partition().feed_hash(h, s);
    }
};

inline mutation_opt move_and_disengage(mutation_opt& opt) {
    return std::move(opt);
}

inline
void apply(mutation_opt& dst, mutation&& src) {
    if (!dst) {
        dst = std::move(src);
    } else {
        dst->apply(std::move(src));
    }
}

inline
void apply(mutation_opt& dst, mutation_opt&& src) {
    if (src) {
        apply(dst, std::move(*src));
    }
}

// Returns a range into partitions containing mutations covered by the range.
// partitions must be sorted according to decorated key.
// range must not wrap around.
boost::iterator_range<std::vector<mutation>::const_iterator> slice(
    const std::vector<mutation>& partitions,
    const query::partition_range&);
