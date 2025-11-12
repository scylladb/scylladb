/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>
#include <vector>
#include <seastar/core/sharded.hh>

#include "dht/i_partitioner_fwd.hh"

namespace replica {

// replica/database.hh
class database;
class keyspace;
class table;
using column_family = table;
class memtable_list;
class keyspace_change;
}


// mutation.hh
class mutation;
class mutation_partition;

// schema/schema.hh
class schema;
class column_definition;
class column_mapping;

// schema_mutations.hh
class schema_mutations;

// keys.hh
class exploded_clustering_prefix;
class partition_key;
class partition_key_view;
class clustering_key_prefix;
class clustering_key_prefix_view;
using clustering_key = clustering_key_prefix;
using clustering_key_view = clustering_key_prefix_view;

// memtable.hh
namespace replica {
class memtable;

using owned_ranges_ptr = seastar::lw_shared_ptr<const dht::token_range_vector>;
owned_ranges_ptr make_owned_ranges_ptr(dht::token_range_vector&& ranges);
}
