/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "sstables/sstable_set.hh"
#include "streaming/stream_reason.hh"
#include "service/topology_guard.hh"
#include <optional>

namespace replica {
class database;
}

namespace db {
namespace view {
class view_builder;
}
}

namespace streaming {

mutation_reader_consumer make_streaming_consumer(sstring origin,
    sharded<replica::database>& db,
    db::view::view_builder& vb,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy,
    service::frozen_topology_guard,
    std::optional<int64_t> repaired_at = std::nullopt,
    lw_shared_ptr<sstables::sstable_list> sstable_list_to_mark_as_repaired = {});

}
