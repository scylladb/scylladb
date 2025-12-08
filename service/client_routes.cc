/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/client_routes.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "mutation/mutation.hh"
#include "db/system_keyspace.hh"

static logging::logger crlogger("client_routes");

service::query_state& client_routes_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
};

future<mutation> service::client_routes_service::make_remove_client_route_mutation(api::timestamp_type ts, const service::client_routes_service::client_route_key& key) {
    static const sstring stmt = format("DELETE FROM {}.{} WHERE connection_id = ? and host_id = ?", db::system_keyspace::NAME, db::system_keyspace::CLIENT_ROUTES);

    auto muts = co_await _qp.get_mutations_internal(stmt, client_routes_query_state(), ts, {key.connection_id, key.host_id});
    if (muts.size() != 1) {
        on_internal_error(crlogger, fmt::format("expected 1 mutation got {}", muts.size()));
    }
    co_return std::move(muts[0]);
}

future<mutation> service::client_routes_service::make_update_client_route_mutation(api::timestamp_type ts, const service::client_routes_service::client_route_entry& route) {
    static const sstring stmt = format("INSERT INTO {}.{} (connection_id, host_id, address, port, tls_port, alternator_port, alternator_https_port) VALUES (?, ?, ?, ?, ?, ?, ?)", db::system_keyspace::NAME, db::system_keyspace::CLIENT_ROUTES);

    auto muts = co_await _qp.get_mutations_internal(stmt, client_routes_query_state(), ts, {
        route.connection_id,
        route.host_id,
        route.address,
        route.port,
        route.tls_port,
        route.alternator_port,
        route.alternator_https_port
    });
    if (muts.size() != 1) {
        on_internal_error(crlogger, fmt::format("expected 1 mutation got {}", muts.size()));
    }
    co_return std::move(muts[0]);
}

future<std::vector<service::client_routes_service::client_route_entry>> service::client_routes_service::get_client_routes() const {
    std::vector<service::client_routes_service::client_route_entry> result;
    static const sstring query = format("SELECT * from {}.{}", db::system_keyspace::NAME, db::system_keyspace::CLIENT_ROUTES);
    auto rs = co_await _qp.execute_internal(query, cql3::query_processor::cache_internal::yes);
    result.reserve(rs->size());
    for (const auto& row : *rs) {
        result.emplace_back(
            row.get_as<sstring>("connection_id"),
            row.get_as<utils::UUID>("host_id"),
            row.get_as<sstring>("address"),
            row.get_opt<int32_t>("port"),
            row.get_opt<int32_t>("tls_port"),
            row.get_opt<int32_t>("alternator_port"),
            row.get_opt<int32_t>("alternator_https_port")
        );
    }
    co_return result;
}
