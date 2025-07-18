/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "compaction/compaction_fwd.hh"
#include "sstables/sstable_set.hh"

namespace compaction {

// Used by manager to set goals and constraints on compaction strategies
class strategy_control {
public:
    virtual ~strategy_control() {}
    virtual bool has_ongoing_compaction(compaction_group_view& table_s) const noexcept = 0;
    virtual future<std::vector<sstables::shared_sstable>> candidates(compaction_group_view&) const = 0;
    virtual future<std::vector<sstables::frozen_sstable_run>> candidates_as_runs(compaction_group_view&) const = 0;
};

}

