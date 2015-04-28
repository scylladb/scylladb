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
    const dht::decorated_key _dk;
    mutation_partition _p;
public:
    mutation(dht::decorated_key key, schema_ptr schema);
    mutation(partition_key key, schema_ptr schema);
    mutation(mutation&&) = default;
    mutation(const mutation&) = default;
    void set_static_cell(const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const clustering_key& key, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value);
    void set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    std::experimental::optional<atomic_cell_or_collection> get_cell(const clustering_key& rkey, const column_definition& def);
    const partition_key& key() const { return _dk._key; };
    const dht::decorated_key& decorated_key() const { return _dk; };
    const dht::token token() const { return _dk._token; }
    const schema_ptr& schema() const { return _schema; }
    const mutation_partition& partition() const { return _p; }
    mutation_partition& partition() { return _p; }
    const utils::UUID& column_family_id() const { return _schema->id(); }
private:
    static void update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value);
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};
