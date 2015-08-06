/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <iostream>

#include "mutation_partition.hh"
#include "keys.hh"
#include "schema.hh"
#include "dht/i_partitioner.hh"

class mutation final {
private:
    schema_ptr _schema;
    dht::decorated_key _dk;
    mutation_partition _p;
public:
    mutation(dht::decorated_key key, schema_ptr schema);
    mutation(partition_key key, schema_ptr schema);
    mutation(schema_ptr schema, dht::decorated_key key, mutation_partition mp);
    mutation(mutation&&) = default;
    mutation(const mutation&) = default;
    mutation& operator=(mutation&& x) = default;

    void set_static_cell(const column_definition& def, atomic_cell_or_collection value);
    void set_static_cell(const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const clustering_key& key, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value);
    void set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    std::experimental::optional<atomic_cell_or_collection> get_cell(const clustering_key& rkey, const column_definition& def) const;
    const partition_key& key() const { return _dk._key; };
    const dht::decorated_key& decorated_key() const { return _dk; };
    const dht::token& token() const { return _dk._token; }
    const schema_ptr& schema() const { return _schema; }
    const mutation_partition& partition() const { return _p; }
    mutation_partition& partition() { return _p; }
    const utils::UUID& column_family_id() const { return _schema->id(); }
    bool operator==(const mutation&) const;
    bool operator!=(const mutation&) const;
public:
    query::result query(const query::partition_slice&, gc_clock::time_point now = gc_clock::now(), uint32_t row_limit = query::max_rows) const;

    // See mutation_partition::live_row_count()
    size_t live_row_count(gc_clock::time_point query_time = gc_clock::time_point::min()) const;
private:
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};

struct mutation_decorated_key_less_comparator {
    bool operator()(const mutation& m1, const mutation& m2) const;
};

using mutation_opt = std::experimental::optional<mutation>;

inline
void apply(mutation_opt& dst, mutation&& src) {
    if (!dst) {
        dst = std::move(src);
    } else {
        dst->partition().apply(*src.schema(), src.partition());
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
