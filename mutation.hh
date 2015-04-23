/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <iostream>

#include "mutation_partition.hh"
#include "keys.hh"
#include "schema.hh"

class mutation final {
private:
    schema_ptr _schema;
    const partition_key _key;
    mutation_partition _p;
public:
    mutation(partition_key key, schema_ptr schema)
        : _schema(std::move(schema))
        , _key(std::move(key))
        , _p(schema)
    { }
    mutation(mutation&&) = default;
    mutation(const mutation&) = default;
    void set_static_cell(const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    void set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value);
    void set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl = {});
    void set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value);
    std::experimental::optional<atomic_cell_or_collection> get_cell(const clustering_key& rkey, const column_definition& def);
    const partition_key& key() const { return _key; };
    const schema_ptr& schema() const { return _schema; }
    const mutation_partition& partition() const { return _p; }
    mutation_partition& partition() { return _p; }
private:
    static void update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value);
    friend std::ostream& operator<<(std::ostream& os, const mutation& m);
};
