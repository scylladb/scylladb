/*
 * Copyright (C) 2019-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/weak_ptr.hh>

#include "compress.hh"
#include "sstables/types.hh"
#include "utils/i_filter.hh"

namespace sstables {

// Immutable components that can be shared among shards.
struct shareable_components {
    sstables::compression compression;
    utils::filter_ptr filter;
    sstables::summary summary;
    sstables::statistics statistics;
    std::optional<sstables::scylla_metadata> scylla_metadata;
    weak_ptr<sstables::checksum> checksum;
    std::optional<uint32_t> digest;
};

}   // namespace sstables
