/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "sstable_version.hh"

namespace sstables {

class sstable_version_constants_m final : public sstable_version_constants {
    static const sstable_version_constants::component_map_t create_component_map();
public:
    sstable_version_constants_m() = delete;
    static const sstable_version_constants::component_map_t _component_map;
};

}
