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
#pragma once

#include <seastar/core/sstring.hh>
#include "enum_set.hh"

#include <map>

namespace cql_transport {

/**
 * CQL Protocol extensions. They can be viewed as an opportunity to provide
 * some vendor-specific extensions to the CQL protocol without changing
 * the version of the protocol itself (i.e. when the changes introduced by
 * extensions are binary-compatible with the current version of the protocol).
 * 
 * Extensions are meant to be passed between client and server in terms of
 * SUPPORTED/STARTUP messages in order to negotiate compatible set of features
 * to be used in a connection.
 * 
 * The negotiation procedure and extensions themselves are documented in the
 * `docs/protocol-extensions.md`. 
 */
enum class cql_protocol_extension {
    LWT_ADD_METADATA_MARK
};

using cql_protocol_extension_enum = super_enum<cql_protocol_extension,
    cql_protocol_extension::LWT_ADD_METADATA_MARK>;

using cql_protocol_extension_enum_set = enum_set<cql_protocol_extension_enum>;

cql_protocol_extension_enum_set supported_cql_protocol_extensions();

/**
 * Returns the name of extension to be used in SUPPORTED/STARTUP feature negotiation.
 */
const seastar::sstring& protocol_extension_name(cql_protocol_extension ext);

/**
 * Returns a list of additional key-value pairs (in the form of "ARG=VALUE" string)
 * that belong to a particular extension and provide some additional capabilities
 * to be used by the client driver in order to support this extension.
 */
std::vector<seastar::sstring> additional_options_for_proto_ext(cql_protocol_extension ext);

} // namespace cql_transport
