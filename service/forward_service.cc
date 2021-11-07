/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/forward_service.hh"

#include <boost/range/algorithm/remove_if.hpp>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include "cql_serialization_format.hh"
#include "db/consistency_level.hh"
#include "gms/gossiper.hh"
#include "idl/forward_request.dist.hh"
#include "locator/abstract_replication_strategy.hh"
#include "log.hh"
#include "message/messaging_service.hh"
#include "query_ranges_to_vnodes.hh"
#include "replica/database.hh"
#include "schema_registry.hh"
#include "service/pager/query_pagers.hh"
#include "utils/fb_utilities.hh"

#include "cql3/column_identifier.hh"
#include "cql3/cql_config.hh"
#include "cql3/query_options.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selectable-expr.hh"
#include "cql3/selection/selectable.hh"
#include "cql3/selection/selection.hh"

namespace service {

static constexpr int DEFAULT_INTERNAL_PAGING_SIZE = 10000;
static logging::logger flogger("forward_service");

static const dht::token& end_token(const dht::partition_range& r) {
    static const dht::token max_token = dht::maximum_token();
    return r.end() ? r.end()->value().token() : max_token;
}

static inet_address_vector_replica_set get_live_endpoints(replica::keyspace& ks, const dht::token& token) {
    auto erm = ks.get_effective_replication_map();
    auto eps = erm->get_natural_endpoints_without_node_being_replaced(token);
    auto itend = boost::range::remove_if(
        eps,
        std::not1(std::bind1st(std::mem_fn(&gms::gossiper::is_alive), &gms::get_local_gossiper()))
    );
    eps.erase(itend, eps.end());
    return eps;
}

static void retain_local_endpoints(inet_address_vector_replica_set& eps) {
    auto itend = boost::range::remove_if(
        eps,
        [](gms::inet_address ep) {
            return !db::is_local(ep);
        }
    );
    eps.erase(itend, eps.end());
}

locator::token_metadata_ptr forward_service::get_token_metadata_ptr() const noexcept {
    return _shared_token_metadata.get();
}

future<> forward_service::stop() {
    return uninit_messaging_service();
}

// Due to `cql3::selection::selection` not being serializable, it cannot be
// stored in `forward_request`. It has to mocked on the receiving node,
// based on requested reduction types.
static shared_ptr<cql3::selection::selection> mock_selection(
    const std::vector<query::forward_request::reduction_type>& reduction_types,
    schema_ptr schema,
    replica::database& db
) {
    std::vector<shared_ptr<cql3::selection::raw_selector>> raw_selectors;

    auto mock_singular_selection = [&] (const query::forward_request::reduction_type& type) {
        switch (type) {
            case query::forward_request::reduction_type::count: {
                auto selectable = cql3::selection::make_count_rows_function_expression();
                auto column_identifier = make_shared<cql3::column_identifier>("count", false);
                return make_shared<cql3::selection::raw_selector>(selectable, column_identifier);
            }
        }
    };

    for (auto const& type : reduction_types) {
        raw_selectors.emplace_back(mock_singular_selection(type));
    }

    return cql3::selection::selection::from_selectors(db.as_data_dictionary(), schema, std::move(raw_selectors));
}

future<query::forward_result> forward_service::execute(query::forward_request req) {
    schema_ptr schema = local_schema_registry().get(req.cmd.schema_version);

    auto reduction_types = std::move(req.reduction_types);
    auto timeout = req.timeout;
    auto now = gc_clock::now();

    auto selection = mock_selection(reduction_types, schema, _db.local());
    auto query_state = make_lw_shared<service::query_state>(
        client_state::for_internal_calls(),
        nullptr,
        empty_service_permit() // FIXME: it probably shouldn't be empty.
    );
    auto query_options = make_lw_shared<cql3::query_options>(
        cql3::default_cql_config,
        req.cl,
        std::optional<std::vector<sstring_view>>(), // Represents empty names.
        std::vector<cql3::raw_value>(), // Represents empty values.
        true, // Skip metadata.
        cql3::query_options::specific_options::DEFAULT,
        cql_serialization_format::latest()
    );
    auto pager = service::pager::query_pagers::pager(
        _proxy,
        schema,
        selection,
        *query_state,
        *query_options,
        make_lw_shared<query::read_command>(std::move(req.cmd)),
        std::move(req.pr),
        nullptr // No filtering restrictions
    );
    auto rs_builder = cql3::selection::result_set_builder(
        *selection,
        now,
        cql_serialization_format::latest(),
        std::vector<size_t>() // Represents empty GROUP BY indices.
    );

    // Execute query.
    while (!pager->is_exhausted()) {
        co_await pager->fetch_page(rs_builder, DEFAULT_INTERNAL_PAGING_SIZE, now, timeout);
    }
    co_return co_await rs_builder.with_thread_if_needed([&] {
        auto rs = rs_builder.build();
        auto& rows = rs->rows();
        if (rows.size() != 1) {
            flogger.error("aggregation result row count != 1");
            throw std::runtime_error("aggregation result row count != 1");
        }
        if (rows[0].size() != reduction_types.size()) {
            flogger.error("aggregation result column count does not match requested column count");
            throw std::runtime_error("aggregation result column count does not match requested column count");
        }
        query::forward_result res = { .query_results = rows[0] };
        return res;
    });
}

void forward_service::init_messaging_service() {
    ser::forward_request_rpc_verbs::register_forward_request(
        &_messaging,
        [this](query::forward_request req) -> future<query::forward_result> {
            return execute(req);
        }
    );
}

future<> forward_service::uninit_messaging_service() {
    return ser::forward_request_rpc_verbs::unregister(&_messaging);
}

future<query::forward_result> forward_service::dispatch(query::forward_request req) {
    schema_ptr schema = local_schema_registry().get(req.cmd.schema_version);
    replica::keyspace& ks = _db.local().find_keyspace(schema->ks_name());

    // next_vnode is used to iterate through all vnodes produced by
    // query_ranges_to_vnodes_generator.
    auto next_vnode = [
        generator = query_ranges_to_vnodes_generator(get_token_metadata_ptr(), schema, req.pr)
    ] () mutable -> std::optional<dht::partition_range> {
        if (auto vnode = generator(1); !vnode.empty()) {
            return vnode[0];
        }
        return {};
    };

    // Group vnodes by assigned endpoint.
    std::map<netw::messaging_service::msg_addr, dht::partition_range_vector> vnodes_per_addr;
    while (std::optional<dht::partition_range> vnode = next_vnode()) {
        inet_address_vector_replica_set live_endpoints = get_live_endpoints(ks, end_token(*vnode));
        // Do not choose an endpoint outside the current datacenter if a request has a local consistency
        if (db::is_datacenter_local(req.cl)) {
            retain_local_endpoints(live_endpoints);
        }

        if (live_endpoints.empty()) {
            throw std::runtime_error("No live endpoint available");
        }

        auto endpoint_addr = netw::messaging_service::msg_addr{*live_endpoints.begin(), 0};
        vnodes_per_addr[endpoint_addr].push_back(*vnode);
    }

    std::optional<query::forward_result> result;
    // Forward request to each endpoint and merge results.
    co_await parallel_for_each(vnodes_per_addr.begin(), vnodes_per_addr.end(),
        [this, &req, &result] (auto vnodes_with_addr) -> future<> {
            auto& addr = vnodes_with_addr.first;
            auto& partition_range = vnodes_with_addr.second;
            auto req_with_modified_pr = req;
            req_with_modified_pr.pr = partition_range;

            flogger.debug("dispatching forward_request={} to address={}", req_with_modified_pr, addr);

            query::forward_result partial_result = co_await [&] {
                if (utils::fb_utilities::is_me(addr.addr)) {
                    return execute(req_with_modified_pr);
                } else {
                    return ser::forward_request_rpc_verbs::send_forward_request(
                        &_messaging, addr, req_with_modified_pr
                    );
                }
            }();

            if (result) {
                result->merge(partial_result, req.reduction_types);
            } else {
                result = partial_result;
            }
        }
    );

    co_return *result;
}

} // namespace service
