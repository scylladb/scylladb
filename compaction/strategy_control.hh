/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "compaction/compaction_fwd.hh"
#include "sstables/sstable_set.hh"

namespace compaction {

// Used by manager to set goals and constraints on compaction strategies
class strategy_control {
public:
    virtual ~strategy_control() {}
    virtual bool has_ongoing_compaction(table_state& table_s) const noexcept = 0;
    virtual std::vector<sstables::shared_sstable> candidates(table_state&) const = 0;
    virtual std::vector<sstables::frozen_sstable_run> candidates_as_runs(table_state&) const = 0;
};

}

