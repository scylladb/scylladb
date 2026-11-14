/*
 * Copyright (C) 2019-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/weak_ptr.hh>

#include "compress.hh"
#include "sstables/parquet/file_summary.hh"
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
    // What the reader keeps from the footer of a Parquet Data file
    // (has_parquet_data_file()). It holds no shard-local state, so the
    // shards that load the sstable read the copy of the owner shard.
    std::optional<sstables::parquet::file_summary> parquet_summary;
};

}   // namespace sstables
