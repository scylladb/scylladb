/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include "gms/feature_service.hh"
#include "mutation/mutation.hh"
#include "service/raft/raft_group0_client.hh"

namespace service {

class endpoint_lifecycle_notifier;

class client_routes_service : public seastar::peering_sharded_service<client_routes_service> {
public:
    client_routes_service(
        abort_source& abort_source,
        gms::feature_service& feature_service,
        service::raft_group0_client& group0_client,
        cql3::query_processor& qp,
        endpoint_lifecycle_notifier& elc_notif
    )
    : _abort_source(abort_source)
    , _feature_service(feature_service)
    , _group0_client(group0_client)
    , _qp(qp)
    , _lifecycle_notifier(elc_notif) { }

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
    seastar::future<> set_client_routes(const std::vector<service::client_routes_service::client_route_entry>& route_entries);
    seastar::future<> delete_client_routes(const std::vector<service::client_routes_service::client_route_key>& route_keys);


    // notifications
    seastar::future<> notify_client_routes_change(const client_route_keys& client_route_keys);
private:
    seastar::future<> set_client_routes_inner(const std::vector<service::client_routes_service::client_route_entry>& route_entries);
    seastar::future<> delete_client_routes_inner(const std::vector<service::client_routes_service::client_route_key>& route_keys);
    template <typename Func>
    seastar::future<> with_retry(Func&& func) const;

    abort_source& _abort_source;
    gms::feature_service& _feature_service;
    service::raft_group0_client& _group0_client;
    cql3::query_processor& _qp;
    endpoint_lifecycle_notifier& _lifecycle_notifier;
};

}
