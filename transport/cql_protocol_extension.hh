/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/sstring.hh>
#include "enum_set.hh"

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
 * `docs/dev/protocol-extensions.md`. 
 */
enum class cql_protocol_extension {
    LWT_ADD_METADATA_MARK,
    RATE_LIMIT_ERROR,
    TABLETS_ROUTING_V1
};

using cql_protocol_extension_enum = super_enum<cql_protocol_extension,
    cql_protocol_extension::LWT_ADD_METADATA_MARK,
    cql_protocol_extension::RATE_LIMIT_ERROR,
    cql_protocol_extension::TABLETS_ROUTING_V1>;

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
