/*
 * Copyright (C) 2026-present ScyllaDB
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
#include "query/query-request.hh"
#include "replica/database.hh"
#include "seastar/core/shared_ptr.hh"
#include "service/client_state.hh"
#include "service/forward_cql_service.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"
#include "tracing/trace_state.hh"
#include "transport/messages/result_message.hh"
#include "transport/messages/result_message_base.hh"
#include "transport/response.hh"
#include "idl/forward_cql.dist.hh"
#include "transport/server.hh"

namespace cql_transport {

// Forward declarations of make_result and make_error_result from transport layer (defined in transport/server.cc)
std::unique_ptr<response>
make_result(int16_t stream, messages::result_message& msg, const tracing::trace_state_ptr& tr_state,
        cql_protocol_version_type version, cql_metadata_id_wrapper&& metadata_id, bool skip_metadata);

std::unique_ptr<cql_server::response>
make_error_result(int16_t stream, std::exception_ptr eptr, const tracing::trace_state_ptr& trace_state,
        cql_protocol_version_type version, bool has_rate_limit_extension);

exceptions::exception_code get_error_code(std::exception_ptr eptr);

sstring make_log_message(int16_t stream, std::exception_ptr eptr);

std::optional<seastar::lowres_clock::time_point> wait_until_timeout(std::exception_ptr eptr);

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
        std::bind_front(&forward_cql_service::handle_forward_execute, this));
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

static cql_transport::cql_metadata_id_wrapper make_metadata_id(forward_cql_execute_request& req) {
    cql_transport::cql_metadata_id_wrapper metadata_id{};
    if (req.request_metadata_id && req.response_metadata_id) {
        metadata_id = cql_transport::cql_metadata_id_wrapper(
            cql3::cql_metadata_id_type(std::move(*req.request_metadata_id)),
            cql3::cql_metadata_id_type(std::move(*req.response_metadata_id)));
    } else if (req.response_metadata_id) {
        metadata_id = cql_transport::cql_metadata_id_wrapper(
            cql3::cql_metadata_id_type(std::move(*req.response_metadata_id)));
    }
    return metadata_id;
}

// Execute a statement on a specific shard
// This is used for shard bouncing when a statement needs to execute on a specific shard
future<cql_transport::cql_server::process_fn_return_type>
forward_cql_service::execute_on_shard(
    unsigned target_shard,
    service::client_state& cs,
    const tracing::trace_state_ptr& trace_state,
    forward_cql_execute_request req,
    cql3::computed_function_values cached_fn_calls,
    const sstring& query_string)
{
    flog.trace("Executing prepared statement {} on shard {}", req.prepared_id, target_shard);
    tracing::trace(trace_state, "Executing prepared statement {} on shard {}", req.prepared_id, target_shard);
    auto gcs = cs.move_to_other_shard();
    auto gt = tracing::global_trace_state_ptr(trace_state);
    if (req.query_string.empty()) {
        req.query_string = query_string;
    }
    co_return co_await container().invoke_on(target_shard, [gcs = std::move(gcs), gt = std::move(gt),
                                                 cached_fn_calls = std::move(cached_fn_calls), req = std::move(req)] (forward_cql_service& svc) mutable -> future<cql_transport::cql_server::process_fn_return_type> {
        auto local_cs = gcs.get();
        auto local_trace_state = gt.get();

        service::query_state qs(local_cs, local_trace_state, empty_service_permit());

        // Look up the prepared statement on this shard
        auto prepared = svc._qp.get_prepared(cql3::prepared_cache_key_type(req.prepared_id, cql3::internal_dialect()));
        if (!prepared) {
            // Not found, re-prepare from query string
            auto prepared_message = co_await svc._qp.prepare(req.query_string, qs, cql3::internal_dialect());
            prepared = prepared_message->get_prepared();
            flog.trace("Prepared statement not found in cache on shard {}, re-prepared from query string for statement {}", this_shard_id(), req.prepared_id);
            tracing::trace(local_trace_state, "Re-prepared statement on shard {} after bounce", this_shard_id());
        }
        cql3::query_options opts = svc.make_query_options(req);

        // Set the cached function calls to prevent infinite bouncing
        if (!cached_fn_calls.empty()) {
            opts.set_cached_pk_function_calls(std::move(cached_fn_calls));
        }

        opts.prepare(prepared->bound_names);

        auto stmt = prepared->statement;
        flog.info("Got prepared statement {} on shard {}, executing", req.prepared_id, this_shard_id());
        auto msg = co_await stmt->execute(svc._qp, qs, opts, std::nullopt);
        flog.info("Prepared statement {} executed successfully on shard {}", req.prepared_id, this_shard_id());

        if (msg->move_to_shard()) {
            co_return cql_transport::cql_server::process_fn_return_type(make_foreign(dynamic_pointer_cast<cql_transport::messages::result_message::bounce_to_shard>(msg)));
        } else {
            tracing::trace(local_trace_state, "Done processing - preparing a result");
            auto skip_metadata = opts.skip_metadata();
            co_return cql_transport::cql_server::process_fn_return_type(make_foreign(make_result(req.stream, *msg, local_trace_state, req.cql_version, make_metadata_id(req), skip_metadata)));
        }
    });
}

future<forward_cql_execute_response> forward_cql_service::handle_forward_execute(
    const rpc::client_info& cinfo, rpc::opt_time_point timeout, forward_cql_execute_request req, topology::version_t topology_version)
{
    auto src_host = cinfo.retrieve_auxiliary<locator::host_id>("host_id");

    auto cs = make_client_state(req.keyspace, timeout);
    service::query_state qs = make_query_state(cs, req.trace_info, src_host);
    cql3::query_options opts = make_query_options(req);
    flog.trace("Handling forwarded CQL execute request from {} for statement {}", src_host, req.prepared_id);
    tracing::trace(qs.get_trace_state(), "Handling forwarded CQL execute request from {}", src_host);

    // Try to find the prepared statement
    auto prepared = _qp.get_prepared(cql3::prepared_cache_key_type(req.prepared_id, cql3::internal_dialect()));
    if (!prepared) {
        if (req.query_string.empty()) {
            co_return forward_cql_execute_response{
                .status = forward_cql_status::prepared_not_found,
                .response_body = {},
                .response_flags = 0,
                .error_info = {},
            };
        }
        // If not found, prepare the statement from query_string
        flog.trace("Prepared statement {} not found in cache, preparing from query string: {}", req.prepared_id, req.query_string);
        auto prepared_message = co_await _qp.prepare(req.query_string, qs, cql3::internal_dialect());
        prepared = prepared_message->get_prepared();

        if (auto id = cql_transport::messages::result_message::prepared::cql::get_id(prepared_message); id != req.prepared_id) {
            on_internal_error(flog, format("Prepared statement ID mismatch: expected {}, got {} after preparing from query string", req.prepared_id, id));
        }
        flog.trace("Prepared statement not found in cache, prepared from query string for statement {}", req.prepared_id);
        tracing::trace(qs.get_trace_state(), "Prepared statement not found in cache, prepared from query string");
    } else {
        flog.trace("Prepared statement found in cache for statement {}", req.prepared_id);
        tracing::trace(qs.get_trace_state(), "Prepared statement found in cache");
    }

    opts.prepare(prepared->bound_names);

    auto stmt = prepared->statement;

    // Try to execute the statement - if we're not the leader, statement execution will
    // throw not_a_leader_exception and the coordinator will retry
    tracing::trace(qs.get_trace_state(), "Executing statement");

    try {
        auto msg = co_await stmt->execute(_qp, qs, opts, std::nullopt);
        std::optional<cql_transport::cql_server::process_fn_return_type> result;
        if (msg->move_to_shard()) {
            result = cql_transport::cql_server::process_fn_return_type(make_foreign(dynamic_pointer_cast<cql_transport::messages::result_message::bounce_to_shard>(msg)));
        } else {
            // We don't need to bounce, prepare the final result
            auto skip_metadata = opts.skip_metadata();
            result = cql_transport::cql_server::process_fn_return_type(make_foreign(cql_transport::make_result(req.stream, *msg, qs.get_trace_state(), req.cql_version, make_metadata_id(req), skip_metadata)));
        }

        // Handle shard bouncing - the statement may need to execute on a different shard
        while (auto* bounce_msg = std::get_if<cql_transport::cql_server::result_with_bounce_to_shard>(&result.value())) {
            auto target_shard = (*bounce_msg)->move_to_shard().value();
            auto&& cached_fn_calls = (*bounce_msg)->take_cached_pk_function_calls();
            tracing::trace(qs.get_trace_state(), "Bouncing {} to shard {}", req.prepared_id, target_shard);
            result = co_await execute_on_shard(target_shard, qs.get_client_state(), qs.get_trace_state(), req, std::move(cached_fn_calls), stmt->raw_cql_statement);
        }
        auto* final_result = std::get_if<cql_transport::cql_server::result_with_foreign_response_ptr>(&result.value());
        auto response = std::move(*final_result).assume_value();
        flog.trace("Remote statement execution of {} succeeded", req.prepared_id);
        tracing::trace(qs.get_trace_state(), "Remote statement execution succeeded");

        co_return forward_cql_execute_response{
            .status = forward_cql_status::finished,
            .response_body = response->extract_body(),
            .response_flags = response->flags(),
            .error_info = {},
        };
    } catch (const cql3::statements::strong_consistency::not_a_leader_exception&) {
        flog.debug("Can't handle the forwarded request: not the leader");
        tracing::trace(qs.get_trace_state(), "Can't handle the forwarded request: not the leader");
        co_return forward_cql_execute_response{
            .status = forward_cql_status::not_a_leader,
            .response_body = {},
            .response_flags = 0,
            .error_info = {},
        };
    } catch (...) {
        flog.trace("Remote statement execution of {} failed with an error", req.prepared_id);
        tracing::trace(qs.get_trace_state(), "Remote statement execution failed with an error");
        auto eptr = std::current_exception();
        auto response = cql_transport::make_error_result(req.stream, eptr, qs.get_trace_state(), req.cql_version, req.has_rate_limit_extension);

        co_return forward_cql_execute_response{
            .status = forward_cql_status::finished,
            .response_body = response->extract_body(),
            .response_flags = response->flags(),
            .error_info = forwarded_error_info{
                .exception_code = static_cast<int32_t>(cql_transport::get_error_code(eptr)),
                .log_message = cql_transport::make_log_message(req.stream, eptr),
                .timeout = cql_transport::wait_until_timeout(eptr),
            },
        };
    }

}

future<locator::host_id> forward_cql_service::get_leader_if_known(
    table_id id, dht::token token, locator::effective_replication_map_ptr erm, bool should_wait_for_leader) const
{
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(id);
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    const auto group_id = raft_info.group_id;

    // Check if we have this raft group locally
    auto my_host_id = erm->get_token_metadata().get_my_id();
    auto shard = -1;
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    for (const auto& replica : tablet_info.replicas) {
        if (replica.host == my_host_id) {
            shard = replica.shard;
            break;
        }
    }
    if (shard == -1) {
        // This node doesn't have this raft group
        co_return locator::host_id{};
    }

    co_return co_await _groups_manager.invoke_on(shard, [group_id, my_host_id, should_wait_for_leader] (strong_consistency::groups_manager& gm) -> future<locator::host_id> {
        if (!gm.has_raft_group(group_id)) {
            // This node doesn't have this raft group, the tablet replica set must be stale
            co_return locator::host_id{};
        }
        auto raft_server = co_await gm.acquire_server(group_id);

        // Use begin_mutate to get leader info
        auto disposition = raft_server.begin_mutate();
        if (auto nal = std::get_if<raft::not_a_leader>(&disposition)) {
            co_return locator::host_id{nal->leader.uuid()};
        } else if (std::get_if<strong_consistency::raft_server::timestamp_with_term>(&disposition)) {
            co_return my_host_id;
        } else if (auto wait_for_leader = std::get_if<strong_consistency::raft_server::need_wait_for_leader>(&disposition)) {
            if (should_wait_for_leader) {
                co_await std::move(wait_for_leader->future);
            }
            co_return locator::host_id{};
        }
        co_return locator::host_id{};
    });
}

// Select closest replica from a tablet replica set, preferring replicas in same rack
static locator::host_id select_closest_replica(const locator::tablet_replica_set& replicas, const locator::topology& topo) {
    // Convert tablet_replica_set to host_id_vector_replica_set for sort_by_proximity
    host_id_vector_replica_set hosts;
    hosts.reserve(replicas.size());
    for (const auto& replica : replicas) {
        hosts.push_back(replica.host);
    }
    topo.sort_by_proximity(topo.my_host_id(), hosts);
    return hosts.front();
}

locator::host_id forward_cql_service::select_replica_for_read(table_id id, dht::token token, locator::effective_replication_map_ptr erm) const {
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(id);
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    const auto& topo = erm->get_token_metadata().get_topology();
    return select_closest_replica(tablet_info.replicas, topo);
}

future<locator::host_id> forward_cql_service::select_replica_for_write(table_id id, dht::token token, locator::effective_replication_map_ptr erm, db::timeout_clock::time_point timeout) const {
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(id);
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);
    const auto this_host = erm->get_token_metadata().get_my_id();


    // Check if we're a replica of this tablet and get our shard if so
    for (const auto& replica : tablet_info.replicas) {
        if (replica.host == this_host) {
            flog.trace("Node is a replica for tablet {}, checking local Raft server", tablet_id);
            co_return co_await get_leader_if_known(id, token, erm, true);
        }
    }

    flog.error("Not a replica for tablet {}, we can't find the leader", tablet_id);
    throw std::runtime_error(format("Node is not a replica for tablet {}, cannot determine leader", tablet_id));
}

future<locator::host_id> forward_cql_service::select_replica(const cql3::cql_statement& stmt, table_id id, dht::token token, locator::effective_replication_map_ptr erm, db::timeout_clock::time_point timeout) const {
    // For selects, we can forward to any replica (prefer closest).
    // For modifications, we need to forward to the leader.
    if (dynamic_cast<const cql3::statements::strong_consistency::modification_statement*>(&stmt)) {
        co_return co_await select_replica_for_write(id, token, erm, timeout);
    } else if (dynamic_cast<const cql3::statements::strong_consistency::select_statement*>(&stmt)) {
        co_return select_replica_for_read(id, token, erm);
    } else {
        on_internal_error(flog, "Strongly consistent statement must be either modification or select statement");
    }
}

forward_cql_execute_request forward_cql_service::make_forward_cql_request(
    ::shared_ptr<cql3::cql_statement> stmt,
    const cql3::cql_prepared_id_type& prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    uint8_t cql_version,
    cql_transport::cql_metadata_id_wrapper metadata_id) const
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
        .query_string = sstring(),
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
        .stream = stream,
        .cql_version = cql_version,
        .request_metadata_id = metadata_id.has_request_metadata_id() ? std::make_optional(metadata_id.get_request_metadata_id()._metadata_id) : std::nullopt,
        .response_metadata_id = metadata_id.has_response_metadata_id() ? std::make_optional(metadata_id.get_response_metadata_id()._metadata_id) : std::nullopt,
        .has_rate_limit_extension = qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::RATE_LIMIT_ERROR),
    };

    return req;
}

future<std::optional<forward_cql_result>>
forward_cql_service::try_execute(
    ::shared_ptr<cql3::cql_statement> stmt,
    const cql3::cql_prepared_id_type& prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    cql_protocol_version_type version,
    cql_transport::cql_metadata_id_wrapper& metadata_id)
{
    flog.trace("Trying to execute statement {} locally. Query string: {}", prepared_id, stmt->raw_cql_statement);
    tracing::trace(qs.get_trace_state(), "Trying to execute statement locally");
    // Try to execute the statement - it will throw not_a_leader_exception if we need to forward the statement to another node
    try {
        auto msg = co_await stmt->execute(_qp, qs, options, std::nullopt);
        std::optional<cql_transport::cql_server::process_fn_return_type> result;
        if (msg->move_to_shard()) {
            result = cql_transport::cql_server::process_fn_return_type(make_foreign(dynamic_pointer_cast<cql_transport::messages::result_message::bounce_to_shard>(msg)));
        } else {
            auto skip_metadata = options.skip_metadata();
            result = cql_transport::cql_server::process_fn_return_type(make_foreign(cql_transport::make_result(stream, *msg, qs.get_trace_state(), version, std::move(metadata_id), skip_metadata)));
        }

        while (auto* bounce_msg = std::get_if<cql_transport::cql_server::result_with_bounce_to_shard>(&result.value())) {
            auto target_shard = (*bounce_msg)->move_to_shard().value();
            auto&& cached_fn_calls = (*bounce_msg)->take_cached_pk_function_calls();
            auto req = make_forward_cql_request(stmt, prepared_id, qs, options, stream, version, metadata_id);
            tracing::trace(qs.get_trace_state(), "Bouncing {} to shard {}", prepared_id, target_shard);
            result = co_await execute_on_shard(target_shard, qs.get_client_state(), qs.get_trace_state(), std::move(req), std::move(cached_fn_calls), stmt->raw_cql_statement);
        }
        auto* final_result = std::get_if<cql_transport::cql_server::result_with_foreign_response_ptr>(&result.value());
        auto response = std::move(*final_result).assume_value();
        flog.trace("Local statement execution of {} succeeded as leader", prepared_id);
        tracing::trace(qs.get_trace_state(), "Local statement execution succeeded as leader");

        co_return forward_cql_result{
            .response = std::move(response),
            .error_info = {},
        };
    } catch (const cql3::statements::strong_consistency::not_a_leader_exception&) {
        flog.trace("Local statement execution of {} failed: not the leader", prepared_id);
        tracing::trace(qs.get_trace_state(), "Local statement execution failed: not the leader");
        co_return std::nullopt;
    } catch (...) {
        auto eptr = std::current_exception();
        auto response = cql_transport::make_error_result(stream, eptr, qs.get_trace_state(), version, qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::RATE_LIMIT_ERROR));
        flog.trace("Local statement execution of {} failed with an error", prepared_id);
        tracing::trace(qs.get_trace_state(), "Local statement execution failed with an error");

        co_return forward_cql_result{
            .response = std::move(response),
            .error_info = forwarded_error_info{
                .exception_code = static_cast<int32_t>(cql_transport::get_error_code(eptr)),
                .log_message = cql_transport::make_log_message(stream, eptr),
                .timeout = cql_transport::wait_until_timeout(eptr),
            },
        };
    }
}

future<forward_cql_result>
forward_cql_service::forward_cql(
    ::shared_ptr<cql3::cql_statement> stmt,
    cql3::cql_prepared_id_type prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    cql_protocol_version_type version,
    cql_transport::cql_metadata_id_wrapper metadata_id)
{
    // First, try to execute locally if we're the leader (for modifications) or a replica (for selects)
    auto local_result = co_await try_execute(stmt, prepared_id, qs, options, stream, version, metadata_id);
    if (local_result) {
        co_return std::move(*local_result);
    }

    // Need to forward to a replica
    auto req = make_forward_cql_request(stmt, prepared_id, qs, options, stream, version, std::move(metadata_id));

    // Get schema and token according to statement type
    db::timeout_clock::time_point timeout;
    dht::token token;
    schema_ptr schema;
    if (auto sc_mod_stmt = dynamic_pointer_cast<cql3::statements::strong_consistency::modification_statement>(stmt)) {
        timeout = db::timeout_clock::now() + sc_mod_stmt->get_timeout(qs.get_client_state(), options);
        schema = _proxy.get_db().local().find_column_family(sc_mod_stmt->keyspace(), sc_mod_stmt->column_family()).schema();
        auto keys = sc_mod_stmt->build_partition_keys(options, std::nullopt);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(flog, "Strongly consistent modification statement must target exactly one partition");
        }
        token = keys[0].start()->value().token();
    } else if (auto sc_sel_stmt = dynamic_pointer_cast<cql3::statements::strong_consistency::select_statement>(stmt)) {
        timeout = db::timeout_clock::now() + sc_sel_stmt->get_timeout(qs.get_client_state(), options);
        schema = _proxy.get_db().local().find_column_family(sc_sel_stmt->keyspace(), sc_sel_stmt->column_family()).schema();
        auto keys = sc_sel_stmt->get_restrictions()->get_partition_key_ranges(options);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(flog, "Strongly consistent select statement must target exactly one partition");
        }
        token =  keys[0].start()->value().token();
    } else {
        on_internal_error(flog, "Strongly consistent statement must be either modification or select statement");
    }

    const auto& tab = schema->table();

    while (db::timeout_clock::now() < timeout) {
        // Keep the effective replication map for the duration of the RPC
        locator::effective_replication_map_ptr erm = tab.get_effective_replication_map();
        auto topology_version = erm->get_token_metadata_ptr()->get_version();

        locator::host_id target = co_await select_replica(*stmt, schema->id(), token, erm, timeout);

        if (!target) {
            // No replica found, retry with fresh topology
            flog.trace("No replica found for token {} of statement {}, retrying", token, prepared_id);
            tracing::trace(qs.get_trace_state(), "No replica found, retrying");
            // An election is ongoing or a node queried for the leader found out that it's no longer owning the tablet before we did
            // In both cases we might keep reentering this case for a while, so add a small sleep to avoid busy looping
            co_await seastar::sleep(std::chrono::milliseconds(5));
            continue;
        }
        flog.trace("Forwarding statement {} to replica {}", prepared_id, target);
        tracing::trace(qs.get_trace_state(), "Forwarding CQL statement to replica: {}", target);

        auto response = co_await ser::forward_cql_rpc_verbs::send_forward_cql_execute(&_ms, target, timeout, req, topology_version);

        switch (response.status) {
        case forward_cql_status::finished:
            // Success, return the response with error info for logging/stats
            tracing::trace(qs.get_trace_state(), "Forwarded CQL statement executed successfully on replica: {}", target);
            co_return forward_cql_result{
                // Don't pass trace_state here - the response_body already contains tracing info if it was traced on the remote node.
                // Passing trace_state would cause the response constructor to prepend another tracing UUID, corrupting the response.
                .response = cql_transport::response::make_from_body(stream, !response.error_info ? cql_transport::cql_binary_opcode::RESULT : cql_transport::cql_binary_opcode::ERROR,
                                                              response.response_flags, std::move(response.response_body)),
                .error_info = std::move(response.error_info),
            };
        case forward_cql_status::prepared_not_found:
            // Retry with query string
            req.query_string = stmt->raw_cql_statement;
            tracing::trace(qs.get_trace_state(), "Prepared statement not found, retrying with query string");
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
