/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "sstables/sstable_set.hh"
#include "streaming/stream_reason.hh"
#include "service/topology_guard.hh"

namespace replica {
class database;
}

namespace db {
namespace view {
class view_builder;
}
}

namespace streaming {

std::function<future<>(mutation_reader)> make_streaming_consumer(sstring origin,
    sharded<replica::database>& db,
    sharded<db::view::view_builder>& vb,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy,
    service::frozen_topology_guard);

}
