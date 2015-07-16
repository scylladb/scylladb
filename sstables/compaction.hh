/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include "sstables.hh"
#include <functional>

namespace sstables {
    future<> compact_sstables(std::vector<shared_sstable> sstables,
            schema_ptr schema, std::function<shared_sstable()> creator);

    // Return the most interesting bucket applying the size-tiered strategy.
    // NOTE: currently used for purposes of testing. May also be used by leveled compaction strategy.
    std::vector<sstables::shared_sstable>
    size_tiered_most_interesting_bucket(lw_shared_ptr<sstable_list> candidates);
}
