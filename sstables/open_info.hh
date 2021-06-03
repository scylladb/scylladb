/*
 * Copyright (C) 2020-present ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <seastar/core/sharded.hh>
#include <vector>
#include "sstables/version.hh"
#include "sstables/component_type.hh"
#include "sstables/shareable_components.hh"
#include <seastar/core/shared_ptr.hh>

namespace sstables {

struct entry_descriptor {
    sstring sstdir;
    sstring ks;
    sstring cf;
    int64_t generation;
    sstable_version_types version;
    sstable_format_types format;
    component_type component;

    static entry_descriptor make_descriptor(sstring sstdir, sstring fname);

    entry_descriptor(sstring sstdir, sstring ks, sstring cf, int64_t generation,
                     sstable_version_types version, sstable_format_types format,
                     component_type component)
        : sstdir(sstdir), ks(ks), cf(cf), generation(generation), version(version), format(format), component(component) {}
};

// contains data for loading a sstable using components shared by a single shard;
// can be moved across shards
struct foreign_sstable_open_info {
    foreign_ptr<lw_shared_ptr<shareable_components>> components;
    std::vector<shard_id> owners;
    seastar::file_handle data;
    seastar::file_handle index;
    uint64_t generation;
    sstable_version_types version;
    sstable_format_types format;
    uint64_t uncompressed_data_size;
};

}
