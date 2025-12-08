/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "gms/feature_service.hh"
#include "mutation/mutation.hh"
#include <seastar/core/sharded.hh>

namespace service {

class client_routes_service : public seastar::peering_sharded_service<client_routes_service> {
public:
    client_routes_service(
        gms::feature_service& feature_service,
        cql3::query_processor& qp
    )
    : _feature_service(feature_service)
    , _qp(qp) { }

    struct client_route_key {
        sstring connection_id;
        utils::UUID host_id;

        bool operator<(const client_route_key& other) const {
            if (connection_id != other.connection_id) {
                return connection_id < other.connection_id;
            }
            return host_id < other.host_id;
        }
    };
    using client_route_keys = std::set<client_route_key>;

    struct client_route_entry {
        sstring connection_id;
        utils::UUID host_id;
        sstring address;
        // At least one of the ports should be specified
        std::optional<int32_t> port;
        std::optional<int32_t> tls_port;
        std::optional<int32_t> alternator_port;
        std::optional<int32_t> alternator_https_port;
    };

    gms::feature_service& get_feature_service() noexcept {
        return _feature_service;
    }

    // mutations
    future<mutation> make_remove_client_route_mutation(api::timestamp_type ts, const service::client_routes_service::client_route_key& key);
    future<mutation> make_update_client_route_mutation(api::timestamp_type ts, const client_route_entry& entry);
    future<std::vector<client_route_entry>> get_client_routes() const;

private:
    gms::feature_service& _feature_service;
    cql3::query_processor& _qp;
};

}
