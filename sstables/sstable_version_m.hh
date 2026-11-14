/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "sstable_version.hh"

namespace sstables {

class sstable_version_constants_m final : public sstable_version_constants {
public:
    static const sstable_version_constants::component_map_t create_component_map();
    sstable_version_constants_m() = delete;
    static const sstable_version_constants::component_map_t _component_map;
};

class sstable_version_constants_m_bti final : public sstable_version_constants {
public:
    static const sstable_version_constants::component_map_t create_component_map();
    sstable_version_constants_m_bti() = delete;
    static const sstable_version_constants::component_map_t _component_map;
};

// The components of the `pq` format: those of `ms` and `mt`, without
// CompressionInfo.db and CRC.db. The Parquet Data file holds its own
// compression metadata and page checksums.
class sstable_version_constants_pq final : public sstable_version_constants {
public:
    static const sstable_version_constants::component_map_t create_component_map();
    sstable_version_constants_pq() = delete;
    static const sstable_version_constants::component_map_t _component_map;
};

}
