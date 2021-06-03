/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <stdexcept>
#include <type_traits>
#include <seastar/core/sstring.hh>

namespace sstables {

enum class sstable_version_types { ka, la, mc, md };
enum class sstable_format_types { big };

inline std::array<sstable_version_types, 4> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
    sstable_version_types::md,
};

inline auto get_highest_sstable_version() {
    return all_sstable_versions[all_sstable_versions.size() - 1];
}

inline sstable_version_types from_string(const seastar::sstring& format) {
    if (format == "ka") {
        return sstable_version_types::ka;
    }
    if (format == "la") {
        return sstable_version_types::la;
    }
    if (format == "mc") {
        return sstable_version_types::mc;
    }
    if (format == "md") {
        return sstable_version_types::md;
    }
    throw std::invalid_argument("Wrong sstable format name: " + format);
}

inline seastar::sstring to_string(sstable_version_types format) {
    switch (format) {
        case sstable_version_types::ka: return "ka";
        case sstable_version_types::la: return "la";
        case sstable_version_types::mc: return "mc";
        case sstable_version_types::md: return "md";
    }
    throw std::runtime_error("Wrong sstable format");
}

inline int operator<=>(sstable_version_types a, sstable_version_types b) {
    auto to_int = [] (sstable_version_types x) {
        return static_cast<std::underlying_type_t<sstable_version_types>>(x);
    };
    return to_int(a) - to_int(b);
}

}
