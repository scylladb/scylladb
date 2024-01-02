/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "alternator/error.hh"
#include "log.hh"
#include <string>
#include <string_view>
#include "bytes.hh"
#include "alternator/auth.hh"
#include <fmt/format.h>
#include "auth/password_authenticator.hh"
#include "service/storage_proxy.hh"
#include "alternator/executor.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include <seastar/core/coroutine.hh>

namespace alternator {

static logging::logger alogger("alternator-auth");

future<std::string> get_key_from_roles(service::storage_proxy& proxy, std::string username) {
    schema_ptr schema = proxy.data_dictionary().find_schema("system_auth", "roles");
    partition_key pk = partition_key::from_single_value(*schema, utf8_type->decompose(username));
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};
    std::vector<query::clustering_range> bounds{query::clustering_range::make_open_ended_both_sides()};
    const column_definition* salted_hash_col = schema->get_column_definition(bytes("salted_hash"));
    if (!salted_hash_col) {
        co_await coroutine::return_exception(api_error::unrecognized_client(format("Credentials cannot be fetched for: {}", username)));
    }
    auto selection = cql3::selection::selection::for_columns(schema, {salted_hash_col});
    auto partition_slice = query::partition_slice(std::move(bounds), {}, query::column_id_vector{salted_hash_col->id}, selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice,
            proxy.get_max_result_size(partition_slice), query::tombstone_limit(proxy.get_tombstone_limit()));
    auto cl = auth::password_authenticator::consistency_for_user(username);

    service::client_state client_state{service::client_state::internal_tag()};
    service::storage_proxy::coordinator_query_result qr = co_await proxy.query(schema, std::move(command), std::move(partition_ranges), cl,
            service::storage_proxy::coordinator_query_options(executor::default_timeout(), empty_service_permit(), client_state));

    cql3::selection::result_set_builder builder(*selection, gc_clock::now());
    query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *schema, *selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        co_await coroutine::return_exception(api_error::unrecognized_client(format("User not found: {}", username)));
    }
    const managed_bytes_opt& salted_hash = result_set->rows().front().front(); // We only asked for 1 row and 1 column
    if (!salted_hash) {
        co_await coroutine::return_exception(api_error::unrecognized_client(format("No password found for user: {}", username)));
    }
    co_return value_cast<sstring>(utf8_type->deserialize(*salted_hash));
}

}
