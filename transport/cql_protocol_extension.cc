/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/format.hh>
#include "transport/cql_protocol_extension.hh"
#include "cql3/result_set.hh"
#include "exceptions/exceptions.hh"

#include <map>

namespace cql_transport {

static const std::map<cql_protocol_extension, seastar::sstring> EXTENSION_NAMES = {
    {cql_protocol_extension::LWT_ADD_METADATA_MARK, "SCYLLA_LWT_ADD_METADATA_MARK"},
    {cql_protocol_extension::RATE_LIMIT_ERROR, "SCYLLA_RATE_LIMIT_ERROR"},
    {cql_protocol_extension::TABLETS_ROUTING_V1, "TABLETS_ROUTING_V1"},
    {cql_protocol_extension::USE_METADATA_ID, "SCYLLA_USE_METADATA_ID"},
    {cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL, "TABLETS_ROUTING_V2_EXPERIMENTAL"},
    {cql_protocol_extension::FAILURE_REASON_MAP, "SCYLLA_FAILURE_REASON_MAP"}
};

const seastar::sstring& protocol_extension_name(cql_protocol_extension ext) {
    return EXTENSION_NAMES.at(ext);
}

std::vector<seastar::sstring> additional_options_for_proto_ext(cql_protocol_extension ext) {
    switch (ext) {
        case cql_protocol_extension::LWT_ADD_METADATA_MARK:
            return {format("LWT_OPTIMIZATION_META_BIT_MASK={:d}", cql3::prepared_metadata::LWT_FLAG_MASK)};
        case cql_protocol_extension::RATE_LIMIT_ERROR:
            return {format("ERROR_CODE={}", exceptions::exception_code::RATE_LIMIT_ERROR)};
        case cql_protocol_extension::FAILURE_REASON_MAP: {
            using reason = exceptions::request_failure_reason;
            return {
                format("REASON_RATE_LIMITED={}", std::to_underlying(reason::SCYLLA_RATE_LIMITED)),
                format("REASON_LARGE_DATA_REJECTED={}", std::to_underlying(reason::SCYLLA_LARGE_DATA_REJECTED)),
                format("REASON_CRITICAL_DISK_UTILIZATION={}", std::to_underlying(reason::SCYLLA_CRITICAL_DISK_UTILIZATION)),
                format("REASON_ABORTED={}", std::to_underlying(reason::SCYLLA_ABORTED)),
                format("REASON_DISCONNECTED={}", std::to_underlying(reason::SCYLLA_DISCONNECTED)),
            };
        }
        default:
            return {};
    }
}

} // namespace cql_transport
