/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "database_fwd.hh"

#pragma once

namespace compaction {
class table_state;
}

namespace replica {

// Compaction group is a set of SSTables which are eligible to be compacted together.
// By this definition, we can say:
//      - A group contains SSTables that are owned by the same shard.
//      - Also, a group will be owned by a single table. Different tables own different groups.
//      - Each group can be thought of an isolated LSM tree, where Memtable(s) and SSTable(s) are
//          isolated from other groups.
// Usually, a table T in shard S will own a single compaction group. With compaction_group, a
// table T will be able to own as many groups as it wishes.
class compaction_group {
    table& _t;
    class table_state;
    std::unique_ptr<table_state> _table_state;
public:
    compaction_group(table& t);

    // Will stop ongoing compaction on behalf of this group, etc.
    future<> stop() noexcept;

    compaction::table_state& as_table_state() const noexcept;
};

}
