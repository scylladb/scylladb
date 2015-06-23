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
}
