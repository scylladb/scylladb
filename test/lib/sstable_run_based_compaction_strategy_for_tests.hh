/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <vector>
#include <map>
#include "compaction/compaction_strategy_impl.hh"
#include "sstables/sstable_set.hh"
#include "compaction/compaction.hh"
#include "replica/database.hh"

namespace sstables {

// Not suitable for production, its sole purpose is testing.

class sstable_run_based_compaction_strategy_for_tests : public compaction_strategy_impl {
    static constexpr size_t static_fragment_size_for_run = 1024*1024;
public:
    sstable_run_based_compaction_strategy_for_tests();

    virtual compaction_descriptor get_sstables_for_compaction(table_state& table_s, strategy_control& control) override;

    virtual int64_t estimated_pending_compactions(table_state& table_s) const override;

    virtual compaction_strategy_type type() const override;

    virtual std::unique_ptr<compaction_backlog_tracker::impl> make_backlog_tracker() const override;
};

}
