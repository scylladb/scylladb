/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner_fwd.hh"

using is_shadowable = bool_class<struct is_shadowable_tag>;

// Determines whether tombstone may be GC-ed.
using can_gc_fn = std::function<bool(tombstone, is_shadowable)>;

extern can_gc_fn always_gc;
extern can_gc_fn never_gc;

using max_purgeable_fn = std::function<api::timestamp_type(const dht::decorated_key&, is_shadowable)>;

extern max_purgeable_fn can_always_purge;
extern max_purgeable_fn can_never_purge;

class atomic_cell;
class row_marker;
struct collection_mutation_description;

class compaction_garbage_collector {
public:
    virtual ~compaction_garbage_collector() = default;
    virtual void collect(column_id id, atomic_cell) = 0;
    virtual void collect(column_id id, collection_mutation_description) = 0;
    virtual void collect(row_marker) = 0;
};
