/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"

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
