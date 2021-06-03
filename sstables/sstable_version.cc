/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "sstable_version.hh"
#include "sstable_version_k_l.hh"
#include "sstable_version_m.hh"

namespace sstables {

const sstring sstable_version_constants::TOC_SUFFIX = "TOC.txt";
const sstring sstable_version_constants::TEMPORARY_TOC_SUFFIX = "TOC.txt.tmp";

sstable_version_constants::component_map_t sstable_version_constants::create_component_map() {
    return {
        { component_type::Index, "Index.db"},
        { component_type::CompressionInfo, "CompressionInfo.db" },
        { component_type::Data, "Data.db" },
        { component_type::TOC, TOC_SUFFIX },
        { component_type::Summary, "Summary.db" },
        { component_type::CRC, "CRC.db" },
        { component_type::Filter, "Filter.db" },
        { component_type::Statistics, "Statistics.db" },
        { component_type::Scylla, "Scylla.db" },
        { component_type::TemporaryTOC, TEMPORARY_TOC_SUFFIX },
        { component_type::TemporaryStatistics, "Statistics.db.tmp" },
    };
}

const sstable_version_constants::component_map_t&
sstable_version_constants::get_component_map(sstable_version_types version) {
    switch (version) {
        case sstable_version_types::ka:
        case sstable_version_types::la:
            return sstable_version_constants_k_l::_component_map;
        case sstable_version_types::mc:
        case sstable_version_types::md:
            return sstable_version_constants_m::_component_map;
    }
    // Should never reach this.
    // Compiler should complain if the switch above does no cover all sstable_version_types values.
    throw std::invalid_argument("Invalid sstable format version");
}

const sstable_version_constants::component_map_t sstable_version_constants_k_l::create_component_map() {
    auto result = sstable_version_constants::create_component_map();
    result.emplace(component_type::Digest, "Digest.sha1");
    return result;
}

const sstable_version_constants::component_map_t sstable_version_constants_k_l::_component_map =
        sstable_version_constants_k_l::create_component_map();

const sstable_version_constants::component_map_t sstable_version_constants_m::create_component_map() {
    auto result = sstable_version_constants::create_component_map();
    result.emplace(component_type::Digest, "Digest.crc32");
    return result;
}

const sstable_version_constants::component_map_t sstable_version_constants_m::_component_map =
        sstable_version_constants_m::create_component_map();

}
