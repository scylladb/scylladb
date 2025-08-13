/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/gate.hh>

class compaction_manager;

namespace compaction {
    class compaction_group_view;
    class compaction_state;
}

class compaction_reenabler {
    compaction_manager& _cm;
    compaction::compaction_group_view* _table;
    compaction::compaction_state& _compaction_state;
    seastar::gate::holder _holder;

public:
    compaction_reenabler(compaction_manager&, compaction::compaction_group_view&);
    compaction_reenabler(compaction_reenabler&&) noexcept;

    ~compaction_reenabler();

    compaction::compaction_group_view* compacting_table() const noexcept {
        return _table;
    }

    const compaction::compaction_state& compaction_state() const noexcept {
        return _compaction_state;
    }
};

