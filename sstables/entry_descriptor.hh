/*
 * Copyright (C) 2019 ScyllaDB
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

#include <regex>

#include <seastar/core/sstring.hh>

#include "sstables/component_type.hh"
#include "sstables/version.hh"

using namespace seastar;

namespace sstables {

class sstable;

class entry_descriptor {
public:
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
private:
    friend class sstable;

    static std::regex la_mc_filename_pattern;
    static std::regex ka_filename_pattern;
    static std::regex sst_dir_pattern;
};

}
