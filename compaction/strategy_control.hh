/*
 * Copyright (C) 2021-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

namespace compaction {

class table_state;

// Used by manager to set goals and constraints on compaction strategies
class strategy_control {
public:
    virtual ~strategy_control() {}
    virtual bool has_ongoing_compaction(table_state& table_s) const noexcept = 0;
};

}

