/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstables/sstable_set.hh"
#include "streaming/stream_reason.hh"
#include "service/topology_guard.hh"
#include "db/view/view_update_checks.hh"
#include <optional>

namespace replica {
class database;
class table;
}

namespace db {
namespace view {
class view_builder;
class view_building_worker;
}
}

namespace streaming {

// Called instead of registering newly written staging sstables with the view
// building machinery (view_building_worker / view_update_generator) right
// away. Used by incremental tablet repair to defer registration until after
// the repair round has finished trying to mark its output sstables as
// repaired, which may replace them via a component rewrite. Never called with
// db::view::sstable_destination_decision::normal_directory.
using view_update_registration_override_fn = std::function<void (
        std::vector<sstables::shared_sstable> ssts,
        db::view::sstable_destination_decision decision,
        lw_shared_ptr<replica::table> table)>;

mutation_reader_consumer make_streaming_consumer(sstring origin,
    sharded<replica::database>& db,
    db::view::view_builder& vb,
    sharded<db::view::view_building_worker>& vbw,
    uint64_t estimated_partitions,
    stream_reason reason,
    sstables::offstrategy offstrategy,
    service::frozen_topology_guard,
    std::function<void (sstables::shared_sstable sst)> on_sstable_written = {},
    view_update_registration_override_fn view_update_registration_override = {});

}
