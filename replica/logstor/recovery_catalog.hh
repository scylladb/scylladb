/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/sstring.hh>

#include "dht/token.hh"
#include "schema/schema_fwd.hh"

namespace replica::logstor {

class primary_index;
class logstor_group;

// The tables of the shard, as recovery and the adoption of a streamed segment see them: the index
// of each logstor table, and the compaction groups that own its tokens on this shard. The database
// implements it for a node, and a test implements it over indexes and groups of its own. The
// lookups do not yield.
class recovery_catalog {
public:
    virtual ~recovery_catalog() = default;

    // The index of a table and one of its compaction groups, found with one lookup of the table.
    struct target {
        primary_index* index = nullptr;
        logstor_group* group = nullptr;
    };

    // The index of the table, or null when the table does not exist or does not use logstor.
    virtual primary_index* index(table_id) = 0;

    // The index of the table, and the compaction group that owns the token on this shard or null.
    virtual target find(table_id, dht::token) = 0;

    // The index of the table, and the compaction group that owns the whole token range on this
    // shard or null when no single group of this shard owns it.
    virtual target find(table_id, dht::token first, dht::token last) = 0;

    // The name of the table for log messages.
    virtual sstring table_name(table_id) = 0;
};

} // namespace replica::logstor
