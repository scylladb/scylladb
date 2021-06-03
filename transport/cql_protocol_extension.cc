/*
 * Copyright 2020-present ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/core/print.hh>
#include "transport/cql_protocol_extension.hh"
#include "cql3/result_set.hh"

#include <map>

namespace cql_transport {

static const std::map<cql_protocol_extension, seastar::sstring> EXTENSION_NAMES = {
    {cql_protocol_extension::LWT_ADD_METADATA_MARK, "SCYLLA_LWT_ADD_METADATA_MARK"}
};

cql_protocol_extension_enum_set supported_cql_protocol_extensions() {
    return cql_protocol_extension_enum_set::full();
} 

const seastar::sstring& protocol_extension_name(cql_protocol_extension ext) {
    return EXTENSION_NAMES.at(ext);
}

std::vector<seastar::sstring> additional_options_for_proto_ext(cql_protocol_extension ext) {
    switch (ext) {
        case cql_protocol_extension::LWT_ADD_METADATA_MARK:
            return {format("LWT_OPTIMIZATION_META_BIT_MASK={:d}", cql3::prepared_metadata::LWT_FLAG_MASK)};
        default:
            return {};
    }
}

} // namespace cql_transport
