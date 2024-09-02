/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/tombstone.hh"
#include "schema/schema_fwd.hh"

// Determines whether tombstone may be GC-ed.
using can_gc_fn = std::function<bool(tombstone)>;

extern can_gc_fn always_gc;
extern can_gc_fn never_gc;

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
