/*
 * Copyright (C) 2018-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "sstables/types.hh"
#include "version.hh"
#include "component_type.hh"

namespace sstables {

class sstable_version_constants {
public:
    using component_map_t = std::unordered_map<component_type, sstring, enum_hash<component_type>>;
    static const sstring TOC_SUFFIX;
    static const sstring TEMPORARY_TOC_SUFFIX;
    static const component_map_t& get_component_map(sstable_version_types);
    sstable_version_constants() = delete;
protected:
    static component_map_t create_component_map();
};

}
