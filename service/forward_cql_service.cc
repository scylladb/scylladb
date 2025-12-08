/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>

#include "cql3/dialect.hh"
#include "cql3/query_options.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/strong_consistency/modification_statement.hh"
#include "cql3/statements/strong_consistency/select_statement.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "db/timeout_clock.hh"
#include "locator/abstract_replication_strategy.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "message/messaging_service.hh"
#include "query/query-request.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "seastar/core/shared_ptr.hh"
#include "service/client_state.hh"
#include "service/forward_cql_service.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"
#include "tracing/trace_state.hh"
#include "transport/messages/result_message.hh"
#include "transport/response.hh"
#include "idl/forward_cql.dist.hh"

namespace cql_transport {

// Forward declaration of make_result from transport layer (defined in transport/server.cc)
std::unique_ptr<response>
make_result(int16_t stream, messages::result_message& msg, const tracing::trace_state_ptr& tr_state,
        cql_protocol_version_type version, cql_metadata_id_wrapper&& metadata_id, bool skip_metadata);

} // namespace cql_transport

namespace service {

static logging::logger flog("forward_cql_service");

forward_cql_service::forward_cql_service(
    netw::messaging_service& ms,
    cql3::query_processor& qp,
    storage_proxy& proxy,
    seastar::sharded<strong_consistency::groups_manager>& groups_manager)
    : _ms(ms)
    , _qp(qp)
    , _proxy(proxy)
    , _groups_manager(groups_manager)
{
    register_handlers();
}

forward_cql_service::~forward_cql_service() {
}

future<> forward_cql_service::stop() {
    co_await ser::forward_cql_rpc_verbs::unregister(&_ms);
}

void forward_cql_service::register_handlers() {
    ser::forward_cql_rpc_verbs::register_forward_cql_execute(&_ms,
        [this] (const rpc::client_info& info, rpc::opt_time_point timeout, forward_cql_execute_request req, topology::version_t topology_version) {
            return handle_forward_execute(info, timeout, req, topology_version);
        });
    ser::forward_cql_rpc_verbs::register_query_tablet_leader(&_ms,
        [this] (const rpc::client_info& info, rpc::opt_time_point timeout, query_tablet_leader_request req) {
            return handle_query_tablet_leader(info, timeout, req);
        });
}

static service::client_state make_client_state(const std::optional<sstring>& keyspace, rpc::opt_time_point timeout) {
    auto now = db::timeout_clock::now();
    auto d = db::timeout_clock::duration::max();
    if (timeout) {
        d = now < *timeout
            ? *timeout - now
            : db::timeout_clock::duration::zero();
    }
    service::client_state client_state(service::client_state::internal_tag{},
        timeout_config{d, d, d, d, d, d, d});
    if (keyspace) {
        client_state.set_raw_keyspace(*keyspace);
    }
    return client_state;
}

service::query_state forward_cql_service::make_query_state(service::client_state& cs, const std::optional<tracing::trace_info>& trace_info, locator::host_id src_host_id) const {
    tracing::trace_state_ptr trace_state_ptr;
    if (trace_info) {
        trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
        tracing::begin(trace_state_ptr);
        tracing::trace(trace_state_ptr, "Forward CQL request received from /{}", src_host_id);
    }
    return service::query_state(cs, trace_state_ptr, empty_service_permit());
}

cql3::query_options forward_cql_service::make_query_options(const forward_cql_execute_request& req) const {
    std::optional<std::vector<std::string_view>> names;
    if (req.names) {
        names.emplace();
        names->reserve(req.names->size());
        for (const auto& name : *req.names) {
            names->emplace_back(name);
        }
    }
    std::vector<cql3::raw_value> values;
    for (size_t i = 0; i < req.values.size(); ++i) {
        values.push_back(cql3::raw_value::make_value(req.values[i]));
    }
    cql3::raw_value_vector_with_unset values_with_unset(std::move(values), req.unset);

    cql3::query_options::specific_options specific_opts{
        .page_size = req.page_size.value_or(0),
        .state = req.paging_state ? make_lw_shared<service::pager::paging_state>(std::move(*req.paging_state)) : nullptr,
        .serial_consistency = req.serial_consistency,
        .timestamp = req.ts,
        .node_local_only = service::node_local_only::no
    };

    return cql3::query_options(
        _qp.get_cql_config(),
        req.consistency,
        std::move(names),
        std::move(values_with_unset),
        req.skip_metadata,
        std::move(specific_opts)
    );
}

bool forward_cql_service::is_stale_topology(const replica::column_family& tab, topology::version_t req_topology_version) const {
    auto erm = tab.get_effective_replication_map();
    return erm->get_token_metadata_ptr()->get_version() != req_topology_version;
}

const replica::column_family& forward_cql_service::get_column_family(const cql3::cql_statement& stmt) const {
    auto& db = _proxy.get_db().local();
    if (auto sc_mod_stmt = dynamic_cast<const cql3::statements::strong_consistency::modification_statement*>(&stmt)) {
        return db.find_column_family(sc_mod_stmt->keyspace(), sc_mod_stmt->column_family());
    } else if (auto sel_stmt = dynamic_cast<const cql3::statements::strong_consistency::select_statement*>(&stmt)) {
        return db.find_column_family(sel_stmt->keyspace(), sel_stmt->column_family());
    }
    on_internal_error(flog, "Strongly consistent statement must be either modification or select statement");
}

// Execute a statement on a specific shard
// This is used for shard bouncing when a statement needs to execute on a specific shard
future<std::unique_ptr<cql_transport::response>>
forward_cql_service::execute_on_shard(
    unsigned target_shard,
    service::client_state& cs,
    const tracing::trace_state_ptr& trace_state,
    const forward_cql_execute_request& req,
    cql3::computed_function_values cached_fn_calls)
{
    auto gcs = cs.move_to_other_shard();
    auto gt = tracing::global_trace_state_ptr(trace_state);

    co_return co_await container().invoke_on(target_shard, [gcs = std::move(gcs), gt = std::move(gt),
                                                 cached_fn_calls = std::move(cached_fn_calls),
                                                 req = req] (forward_cql_service& svc) mutable -> future<std::unique_ptr<cql_transport::response>> {
        auto local_cs = gcs.get();
        auto local_trace_state = gt.get();

        service::query_state qs(local_cs, local_trace_state, empty_service_permit());

        // Look up the prepared statement on this shard
        auto prepared = svc._qp.get_prepared(cql3::prepared_cache_key_type(req.prepared_id, cql3::internal_dialect()));
        if (!prepared) {
            // Not found, re-prepare from query string
            if (!req.query_string) {
                throw std::runtime_error("Prepared statement not found on target shard after bounce and no query string available");
            }
            auto prepared_message = co_await svc._qp.prepare(req.query_string.value(), qs, cql3::internal_dialect());
            prepared = prepared_message->get_prepared();
            tracing::trace(local_trace_state, "Re-prepared statement on shard {} after bounce", this_shard_id());
        }
        cql3::query_options opts = svc.make_query_options(req);

        // Set the cached function calls to prevent infinite bouncing
        if (!cached_fn_calls.empty()) {
            opts.set_cached_pk_function_calls(std::move(cached_fn_calls));
        }

        opts.prepare(prepared->bound_names);

        auto stmt = prepared->statement;
        tracing::trace(local_trace_state, "Executing statement on shard {} after bounce", this_shard_id());
        auto result = co_await stmt->execute(svc._qp, qs, opts, std::nullopt);
        if (result->is_exception()) {
            result->throw_if_exception();
        }
        auto skip_metadata = opts.skip_metadata();
        co_return cql_transport::make_result(0, *result, local_trace_state,
                    req.cql_version, cql_transport::cql_metadata_id_wrapper{}, skip_metadata);
    });
}

future<forward_cql_execute_response> forward_cql_service::handle_forward_execute(
    const rpc::client_info& cinfo, rpc::opt_time_point timeout, const forward_cql_execute_request& req, topology::version_t topology_version)
{
    auto src_addr = cinfo.retrieve_auxiliary<locator::host_id>("host_id");

    auto cs = make_client_state(req.keyspace, timeout);
    service::query_state qs = make_query_state(cs, req.trace_info, src_addr);
    cql3::query_options opts = make_query_options(req);
    tracing::trace(qs.get_trace_state(), "Handling forwarded CQL execute request from /{}", src_addr);

    // Try to find the prepared statement
    auto prepared = _qp.get_prepared(cql3::prepared_cache_key_type(req.prepared_id, cql3::internal_dialect()));
    if (!prepared) {
        if (!req.query_string) {
            co_return forward_cql_execute_response{
                .status = forward_cql_status::prepared_not_found,
                .response_body = {},
                .response_flags = 0,
            };
        }
        // If not found, prepare the statement from query_string
        auto prepared_message = co_await _qp.prepare(req.query_string.value(), qs, cql3::internal_dialect());
        prepared = prepared_message->get_prepared();
        tracing::trace(qs.get_trace_state(), "Prepared statement not found in cache, prepared from query string");
    } else {
        tracing::trace(qs.get_trace_state(), "Prepared statement found in cache");
    }

    opts.prepare(prepared->bound_names);

    auto stmt = prepared->statement;
    auto& tab = get_column_family(*stmt);

    if (is_stale_topology(tab, topology_version)) {
        co_return forward_cql_execute_response{
            .status = forward_cql_status::stale_topology,
            .response_body = {},
            .response_flags = 0,
        };
    }

    // Try to execute the statement - if we're not the leader, statement execution will
    // throw not_a_leader_exception which we catch and return as status
    tracing::trace(qs.get_trace_state(), "Executing statement");

    ::shared_ptr<cql_transport::messages::result_message> result;
    std::unique_ptr<cql_transport::response> response;
    try {
        result = co_await stmt->execute(_qp, qs, opts, std::nullopt);
        // Handle shard bouncing - the statement may need to execute on a different shard
        if (result->move_to_shard()) {
            auto target_shard = result->move_to_shard().value();
            auto cached_fn_calls = dynamic_cast<cql_transport::messages::result_message::bounce_to_shard&>(*result).take_cached_pk_function_calls();

            tracing::trace(qs.get_trace_state(), "Bouncing to shard {}", target_shard);
            response = co_await execute_on_shard(target_shard, cs, qs.get_trace_state(), req, std::move(cached_fn_calls));
        } else if (result->is_exception()) {
            result->throw_if_exception();
        } else {
            auto skip_metadata = opts.skip_metadata();
            response = cql_transport::make_result(0, *result, qs.get_trace_state(), req.cql_version, cql_transport::cql_metadata_id_wrapper{}, skip_metadata);
        }
    } catch (const cql3::statements::strong_consistency::not_a_leader_exception& e) {
        tracing::trace(qs.get_trace_state(), "Can't handle the forwarded request: not the leader");
        co_return forward_cql_execute_response{
            .status = forward_cql_status::not_a_leader,
            .response_body = {},
            .response_flags = 0,
        };
    }

    co_return forward_cql_execute_response{
        .status = forward_cql_status::ok,
        .response_body = response->extract_body(),
        .response_flags = response->flags(),
    };
}

future<query_tablet_leader_response> forward_cql_service::handle_query_tablet_leader(
    const rpc::client_info& cinfo, rpc::opt_time_point timeout, const query_tablet_leader_request& req)
{
    // Get tablet info on current shard first
    auto& db = _proxy.get_db().local();
    auto tid = table_id(req.table_id);
    auto& tab = db.find_column_family(tid);
    auto erm = tab.get_effective_replication_map();

    const auto token = dht::token::from_int64(req.token);
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(tid);
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    const auto my_host_id = erm->get_token_metadata().get_my_id();
    const auto group_id = raft_info.group_id;

    co_return co_await _groups_manager.invoke_on(0, [group_id, my_host_id] (strong_consistency::groups_manager& gm) -> future<query_tablet_leader_response> {
        if (!gm.has_raft_group(group_id)) {
            // This node doesn't have this raft group
            flog.debug("Raft group {} does not exist, cannot determine leader", group_id);
            co_return query_tablet_leader_response{
                .leader_host_id = std::nullopt
            };
        }
        auto raft_server = co_await gm.acquire_server(group_id);

        // Use begin_mutate to get leader info
        co_return std::visit(overloaded_functor {
            [&] (const strong_consistency::raft_server::timestamp_with_term& twt) -> query_tablet_leader_response {
                return query_tablet_leader_response{
                    .leader_host_id = my_host_id.uuid()
                };
            },
            [&] (const raft::not_a_leader& nal) -> query_tablet_leader_response {
                return query_tablet_leader_response{
                    .leader_host_id = nal.leader.uuid()
                };
            },
            [] (const strong_consistency::raft_server::need_wait_for_leader&) -> query_tablet_leader_response {
                // No leader known yet, retries will be attempted by the RPC sender
                return query_tablet_leader_response{
                    .leader_host_id = utils::UUID{}
                };
            }
        }, raft_server.begin_mutate());
    });
}

dht::token forward_cql_service::get_token(const cql3::cql_statement& stmt, const cql3::query_options& options) const {
    if (auto sc_mod_stmt = dynamic_cast<const cql3::statements::strong_consistency::modification_statement*>(&stmt)) {
        auto keys = sc_mod_stmt->build_partition_keys(options, std::nullopt);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(flog, "Strongly consistent modification statement must target exactly one partition");
        }
        return keys[0].start()->value().token();
    } else if (auto sc_sel_stmt = dynamic_cast<const cql3::statements::strong_consistency::select_statement*>(&stmt)) {
        auto keys = sc_sel_stmt->get_restrictions()->get_partition_key_ranges(options);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(flog, "Strongly consistent select statement must target exactly one partition");
        }
        return keys[0].start()->value().token();
    } else {
        on_internal_error(flog, "Strongly consistent statement must be either modification or select statement");
    }
}

future<std::optional<locator::host_id>> forward_cql_service::select_replica(table_id id, dht::token token, locator::effective_replication_map_ptr erm) const {
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(id);
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    const auto this_host = erm->get_token_metadata().get_my_id();

    // Check if we're a replica of this tablet
    if (locator::contains(tablet_info.replicas, this_host)) {
        // We are a replica, use local Raft server to find leader
        // groups_manager is pinned to shard 0, so we need to invoke there
        flog.trace("Node is a replica for tablet {}, checking local Raft server", tablet_id);
        const auto group_id = raft_info.group_id;

        co_return co_await _groups_manager.invoke_on(0, [group_id, this_host] (strong_consistency::groups_manager& gm) -> future<std::optional<locator::host_id>> {
            auto raft_server = co_await gm.acquire_server(group_id);

            // Use begin_mutate to get leader info
            std::optional<locator::host_id> leader = std::visit(overloaded_functor {
                [&] (const strong_consistency::raft_server::timestamp_with_term& twt) -> locator::host_id {
                    return this_host;
                },
                [&] (const raft::not_a_leader& nal) -> locator::host_id {
                    return locator::host_id(nal.leader.uuid());
                },
                [] (const strong_consistency::raft_server::need_wait_for_leader&) -> locator::host_id {
                    // No leader known yet, retries will be attempted by the RPC sender
                    return locator::host_id{};
                }
            }, raft_server.begin_mutate());

            if (!leader) {
                // No leader known yet, wait and retry
                co_await raft_server.wait_for_leader();
                co_return std::nullopt;
            } else {
                flog.trace("Found leader {} via local Raft server for group {}", leader, group_id);
                co_return leader;
            }
        });
    }

    // We're not a replica, query one of the replicas for the leader
    flog.trace("Not a replica for tablet {}, querying a replica for leader", tablet_id);

    query_tablet_leader_request leader_req{
        .table_id = id.uuid(),
        .token = token.raw()
    };
    if (tablet_info.replicas.empty()) {
        flog.trace("No replicas found for tablet {}, cannot determine leader", tablet_id);
        co_return std::nullopt;
    }
    auto response = co_await ser::forward_cql_rpc_verbs::send_query_tablet_leader(
        &_ms, tablet_info.replicas.front().host, db::timeout_clock::now() + std::chrono::seconds(5), leader_req);
    if (response.leader_host_id) {
        flog.trace("Found leader {} via replica {}", locator::host_id{*response.leader_host_id}, tablet_info.replicas.front().host);
        co_return locator::host_id{*response.leader_host_id};
    }
    // Replica doesn't know the leader - return nullopt to trigger retry
    flog.trace("Replica {} of the tablet {} doesn't know the leader, will retry", tablet_info.replicas.front().host, tablet_id);
    co_return std::nullopt;
}

forward_cql_execute_request forward_cql_service::make_forward_cql_request(
    ::shared_ptr<cql3::cql_statement> stmt,
    const cql3::cql_prepared_id_type& prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint8_t cql_version) const
{
    // Get query options fields
    std::vector<bytes_opt> values;
    cql3::unset_bind_variable_vector unset;
    std::optional<std::vector<sstring>> names;

    auto& raw_vals = options.get_values();
    for (size_t i = 0; i < raw_vals.size(); ++i) {
        values.push_back(cql3::raw_value::make_value(raw_vals[i]).to_bytes_opt());
        if (options.is_unset(i)) {
            unset.push_back(true);
        } else {
            unset.push_back(false);
        }
    }
    if (auto names_views = options.get_names()) {
        names.emplace();
        names->reserve(names_views->size());
        for (const auto& name : *names_views) {
            names->emplace_back(name);
        }
    }

    forward_cql_execute_request req{
        .prepared_id = prepared_id,
        .query_string = std::nullopt,
        .consistency = options.get_consistency(),
        .values = std::move(values),
        .unset = std::move(unset),
        .names = std::move(names),
        .skip_metadata = options.skip_metadata(),
        .paging_state = options.get_paging_state() ? std::make_optional(*options.get_paging_state()) : std::nullopt,
        .page_size = options.get_page_size(),
        .serial_consistency = options.get_serial_consistency(),
        .ts = options.get_timestamp(qs),
        .keyspace = qs.get_client_state().get_raw_keyspace(),
        .trace_info = tracing::make_trace_info(qs.get_trace_state()),
        .cql_version = cql_version,
    };

    return req;
}

future<std::optional<std::unique_ptr<cql_transport::response>>>
forward_cql_service::try_execute_as_leader(
    ::shared_ptr<cql3::cql_statement> stmt,
    const cql3::cql_prepared_id_type& prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    cql_protocol_version_type version)
{
    // Try to execute the statement - it will throw not_a_leader_exception if we're not the leader
    ::shared_ptr<cql_transport::messages::result_message> result;
    try {
        result = co_await stmt->execute(_qp, qs, options, std::nullopt);
    } catch (const cql3::statements::strong_consistency::not_a_leader_exception&) {
        // Not the leader, need to forward
        tracing::trace(qs.get_trace_state(), "Local statement execution failed: not the leader");
        co_return std::nullopt;
    }
    std::optional<std::unique_ptr<cql_transport::response>> response;
    if (result->move_to_shard()) {
        auto target_shard = result->move_to_shard().value();
        auto cached_fn_calls = dynamic_cast<cql_transport::messages::result_message::bounce_to_shard&>(*result).take_cached_pk_function_calls();
        auto req = make_forward_cql_request(stmt, prepared_id, qs, options, version);
        req.query_string = stmt->raw_cql_statement;
        tracing::trace(qs.get_trace_state(), "Bouncing to shard {}", target_shard);
        response = co_await execute_on_shard(target_shard, qs.get_client_state(), qs.get_trace_state(), req, std::move(cached_fn_calls));
    } else {
        auto skip_metadata = options.skip_metadata();
        response = cql_transport::make_result(stream, *result, qs.get_trace_state(), version, cql_transport::cql_metadata_id_wrapper{}, skip_metadata);
    }
    tracing::trace(qs.get_trace_state(), "Local statement execution succeeded as leader");

    co_return std::move(response);
}


static db::timeout_clock::time_point get_timeout(::shared_ptr<cql3::cql_statement> stmt, service::query_state& qs, const cql3::query_options& options) {
    if (auto sc_mod_stmt = dynamic_pointer_cast<cql3::statements::strong_consistency::modification_statement>(stmt)) {
        return db::timeout_clock::now() + sc_mod_stmt->get_timeout(qs.get_client_state(), options);
    } else if (auto sc_sel_stmt = dynamic_pointer_cast<cql3::statements::strong_consistency::select_statement>(stmt)) {
        return db::timeout_clock::now() + sc_sel_stmt->get_timeout(qs.get_client_state(), options);
    } else {
        on_internal_error(flog, "Strongly consistent statement must be either modification or select statement");
    }
}

future<std::unique_ptr<cql_transport::response>>
forward_cql_service::forward_cql(
    ::shared_ptr<cql3::cql_statement> stmt,
    const cql3::cql_prepared_id_type& prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    cql_protocol_version_type version)
{
    // First, try to execute locally if we're the leader
    auto local_result = co_await try_execute_as_leader(stmt, prepared_id, qs, options, stream, version);
    if (local_result) {
        co_return std::move(*local_result);
    }

    // Need to forward to the leader
    auto req = make_forward_cql_request(stmt, prepared_id, qs, options, version);
    auto timeout = get_timeout(stmt, qs, options);

    forward_cql_execute_response response;
    const auto& tab = get_column_family(*stmt);

    while (db::timeout_clock::now() < timeout) {
        // Keep the effective replication map for the duration of the RPC
        locator::effective_replication_map_ptr erm = tab.get_effective_replication_map();
        auto topology_version = erm->get_token_metadata_ptr()->get_version();

        auto token = get_token(*stmt, options);
        auto target_opt = co_await select_replica(tab.schema()->id(), token, erm);
        if (!target_opt) {
            // No leader found, retry with fresh topology
            tracing::trace(qs.get_trace_state(), "No leader found, retrying");
            continue;
        }
        auto target = *target_opt;
        tracing::trace(qs.get_trace_state(), "Forwarding CQL statement to replica: /{}", target);

        response = co_await ser::forward_cql_rpc_verbs::send_forward_cql_execute(&_ms, target, timeout, req, topology_version);

        switch (response.status) {
        case forward_cql_status::ok:
            // Success, return the response
            co_return cql_transport::response::make_from_body(stream, cql_transport::cql_binary_opcode::RESULT, 
                                                              response.response_flags, std::move(response.response_body));
        case forward_cql_status::prepared_not_found:
            // Retry with query string
            req.query_string = stmt->raw_cql_statement;
            tracing::trace(qs.get_trace_state(), "Prepared statement not found, retrying with query string");
            continue;
        case forward_cql_status::stale_topology:
            // Topology changed, retry with fresh ERM
            tracing::trace(qs.get_trace_state(), "Stale topology, retrying");
            continue;
        case forward_cql_status::not_a_leader:
            // Target was not the leader, retry and find the current leader
            tracing::trace(qs.get_trace_state(), "Target is not the leader, retrying");
            continue;
        }
    }

    throw std::runtime_error("Forward CQL request timed out");
}

} // namespace service
