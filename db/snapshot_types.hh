/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "dht/token.hh"
#include "locator/host_id.hh"
#include "sstables/types.hh"

namespace db {

using is_downloaded = bool_class<class is_downloaded_tag>;

enum class snapshot_state : uint8_t {
    unknown,
    local,
    being_backed_up,
    remote_and_local,
    remote,
};

enum class snapshot_table_type : uint32_t {
    cql_table,
    cql_view,
    alternator_table,
};

struct snapshot_sstable_entry {
    sstables::sstable_id sstable_id;
    dht::token first_token;
    dht::token last_token;
    sstring toc_name;
    sstring prefix;
    is_downloaded downloaded{is_downloaded::no};
    locator::host_id node;
    size_t tablet_id;
    snapshot_state state;
    int64_t repaired_at;
    int64_t data_size;
    int64_t index_size;
};

struct snapshot_remote_location_entry {
    sstring endpoint;
    sstring bucket;
    sstring prefix;
};

} // namespace db
