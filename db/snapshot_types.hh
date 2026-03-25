/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "dht/token.hh"
#include "sstables/types.hh"

namespace db {

using is_downloaded = bool_class<class is_downloaded_tag>;

struct snapshot_sstable_entry {
    sstables::sstable_id sstable_id;
    dht::token first_token;
    dht::token last_token;
    sstring toc_name;
    sstring prefix;
    is_downloaded downloaded{is_downloaded::no};
};

struct snapshot_remote_location_entry {
    sstring endpoint;
    sstring bucket;
    sstring prefix;
};

} // namespace db
