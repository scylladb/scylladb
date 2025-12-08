/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service/forward_cql_service.hh"

#include <random>

#include <seastar/core/coroutine.hh>

#include "cql3/dialect.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/select_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/strongly_consistent_statement.hh"
#include "message/messaging_service.hh"
#include "idl/forward_cql.dist.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"

namespace service {

static logging::logger fwd_cql_log("forward_cql_service");

forward_cql_service::forward_cql_service(
    netw::messaging_service& ms,
    cql3::query_processor& qp,
    storage_proxy& proxy)
    : _ms(ms)
    , _qp(qp)
    , _proxy(proxy)
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

    fwd_cql_log.info("forward_cql_service RPC handlers registered");
}

service::query_state forward_cql_service::make_query_state(const std::optional<sstring>& keyspace, const std::optional<tracing::trace_info>& trace_info, rpc::opt_time_point timeout, locator::host_id src_host_id) {
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
    tracing::trace_state_ptr trace_state_ptr;
    if (trace_info) {
        trace_state_ptr = tracing::tracing::get_local_tracing_instance().create_session(*trace_info);
        tracing::begin(trace_state_ptr);
        tracing::trace(trace_state_ptr, "Forward CQL request received from /{}", src_host_id);
    }
    return service::query_state(client_state, trace_state_ptr, empty_service_permit());
}

cql3::query_options forward_cql_service::make_query_options(const forward_cql_execute_request& req) {
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

bool forward_cql_service::is_stale_topology(const cql3::statements::strongly_consistent_statement& stmt, topology::version_t req_topology_version) {
    auto& db = _proxy.get_db().local();
    if (auto mod_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(stmt.statement)) {
        auto& tab = db.find_column_family(mod_stmt->keyspace(), mod_stmt->column_family());
        auto erm = tab.get_effective_replication_map();
        if (erm->get_token_metadata_ptr()->get_version() != req_topology_version) {
            return true;
        }
    } else if (auto sel_stmt = dynamic_pointer_cast<cql3::statements::select_statement>(stmt.statement)) {
        auto& tab = db.find_column_family(sel_stmt->keyspace(), sel_stmt->column_family());
        auto erm = tab.get_effective_replication_map();
        if (erm->get_token_metadata_ptr()->get_version() != req_topology_version) {
            return true;
        }
    } else {
        on_internal_error(fwd_cql_log, "Tried to forward a strongly-consistent non-modification/select statement");
    }
    return false;
}

forward_cql_execute_response forward_cql_service::make_forward_cql_response(
    const ::shared_ptr<cql_transport::messages::result_message>& result_msg)
{
    forward_cql_execute_response response{
        .prepared_not_found = false,
        .stale_topology = false
    };
    if (auto rows_msg = dynamic_pointer_cast<const cql_transport::messages::result_message::rows>(result_msg)) {
        auto& cql_result = rows_msg->rs();
        response.result = cql3::serialize_result_set(cql_result.result_set());
    } else if (auto ex_msg = dynamic_pointer_cast<const cql_transport::messages::result_message::exception>(result_msg)) {
        response.ex = exceptions::serialize_coordinator_exception(ex_msg->get_exception());
    } else if (dynamic_pointer_cast<const cql_transport::messages::result_message::void_message>(result_msg)) {
        // void message, nothing to do
    } else {
        on_internal_error(fwd_cql_log, "Unsupported result message type received in forward CQL response");
    }
    response.warnings = result_msg->warnings();
    if (auto custom_payload = result_msg->custom_payload()) {
        response.custom_payload = *custom_payload;
    }
    return response;
}

future<forward_cql_execute_response> forward_cql_service::handle_forward_execute(
    const rpc::client_info& cinfo, rpc::opt_time_point timeout, const forward_cql_execute_request& req, topology::version_t topology_version)
{

    auto src_addr = cinfo.retrieve_auxiliary<locator::host_id>("host_id");

    service::query_state qs = make_query_state(req.keyspace, req.trace_info, timeout, src_addr);

    cql3::query_options opts = make_query_options(req);


    // Try to find the prepared statement
    auto prepared = _qp.get_prepared(cql3::prepared_cache_key_type(req.prepared_id, cql3::internal_dialect()));
    if (!prepared) {
        if (!req.query_string) {
            co_return forward_cql_execute_response{
                .prepared_not_found = true
            };
        }
        // If not found, prepare the statement from query_string
        auto prepared_message = co_await _qp.prepare(req.query_string.value(), qs, cql3::internal_dialect());
        prepared = prepared_message->get_prepared();
    }

    opts.prepare(prepared->bound_names);

    // Execute the statement
    auto strongly_consistent_stmt = dynamic_pointer_cast<cql3::statements::strongly_consistent_statement>(prepared->statement);
    if (!strongly_consistent_stmt) {
        on_internal_error(fwd_cql_log, "Tried to forward a non-strongly-consistent statement");
    }

    if (is_stale_topology(*strongly_consistent_stmt, topology_version)) {
        co_return forward_cql_execute_response{
            .prepared_not_found = false,
            .stale_topology = true
        };
    }

    auto result = co_await strongly_consistent_stmt->execute_direct(_qp, qs, opts);

    co_return make_forward_cql_response(result);
}

dht::token forward_cql_service::get_token(const cql3::statements::strongly_consistent_statement& stmt, const cql3::query_options& options) {
    if (auto mod_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(stmt.statement)) {
        auto keys = mod_stmt->build_partition_keys(options, std::nullopt);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(fwd_cql_log, "Strongly consistent modification statement must target exactly one partition");
        }
        return keys[0].start()->value().token();
    } else if (auto sel_stmt = dynamic_pointer_cast<cql3::statements::select_statement>(stmt.statement)) {
        auto keys = sel_stmt->get_restrictions()->get_partition_key_ranges(options);
        if (keys.size() != 1 || !query::is_single_partition(keys.front())) {
            on_internal_error(fwd_cql_log, "Strongly consistent select statement must target exactly one partition");
        }
        return keys[0].start()->value().as_decorated_key().token();
    } else {
        on_internal_error(fwd_cql_log, "Strongly consistent statement must be either modification or select statement");
    }
}

locator::host_id forward_cql_service::select_replica(const cql3::statements::strongly_consistent_statement& stmt, const cql3::query_options& options, const locator::effective_replication_map& erm) {
    auto token = get_token(stmt, options);
    auto replicas = erm.get_natural_replicas(token);
    if (replicas.empty()) {
        throw std::runtime_error("No replicas found for token");
    }

    // Randomly select one of the replicas
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, replicas.size() - 1);

    return replicas[dis(gen)];
}


forward_cql_execute_request forward_cql_service::make_forward_cql_request(
    const cql3::statements::strongly_consistent_statement& stmt,
    service::query_state& qs,
    const cql3::query_options& options,
    const cql3::query_processor& qp)
{
   // Get the prepared statement ID
    auto cache_key = _qp.compute_id(stmt.raw_cql_statement, qs.get_client_state().get_raw_keyspace(), cql3::internal_dialect());
    bytes prepared_id = cql3::prepared_cache_key_type::cql_id(cache_key);

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
        .paging_state = *options.get_paging_state(),
        .page_size = options.get_page_size(),
        .serial_consistency = options.get_serial_consistency(),
        .ts = options.get_timestamp(qs),
        .keyspace = qs.get_client_state().get_raw_keyspace(),
        .trace_info = tracing::make_trace_info(qs.get_trace_state())
    };

    return req;
}

::shared_ptr<cql_transport::messages::result_message>
forward_cql_service::make_result_message_from_response(
    forward_cql_execute_response& response)
{
    shared_ptr<cql_transport::messages::result_message> result_msg;
    if (response.result) {
        auto rs = cql3::deserialize_result_set(std::move(*response.result));
        auto cql_result = cql3::result(std::make_unique<cql3::result_set>(std::move(rs)));
        result_msg = ::make_shared<cql_transport::messages::result_message::rows>(std::move(cql_result));
    } else if (response.ex) {
        auto ex = exceptions::deserialize_coordinator_exception(*response.ex);
        result_msg = ::make_shared<cql_transport::messages::result_message::exception>(std::move(ex));
    } else {
        result_msg = ::make_shared<cql_transport::messages::result_message::void_message>();
    }
    for (auto& w : response.warnings) {
        result_msg->add_warning(std::move(w));
    }
    if (auto& custom_payload = response.custom_payload) {
        for (const auto& [key, value] : *custom_payload) {
            result_msg->add_custom_payload(key, value);
        }
    }
    return result_msg;
}

future<::shared_ptr<cql_transport::messages::result_message>>
forward_cql_service::forward_cql(const cql3::statements::strongly_consistent_statement& stmt, service::query_state& qs, const cql3::query_options& options) {
    auto req = make_forward_cql_request(stmt, qs, options, _qp);
    auto timeout = db::timeout_clock::now();// + stmt.statement->get_timeout(qs.get_client_state(), options);

    forward_cql_execute_response response;
    do {
        // Get fencing token from the effective replication map
        auto& db = _proxy.get_db().local();
        auto& tab = db.find_column_family("a", "b");
        auto erm = tab.get_effective_replication_map();
        auto topology_version = erm->get_token_metadata_ptr()->get_version();

        auto target = select_replica(stmt, options, *erm);
        fwd_cql_log.debug("Forwarding CQL statement to replica: {}", target);
        response = co_await ser::forward_cql_rpc_verbs::send_forward_cql_execute(&_ms, target, timeout, req, topology_version);
        if (response.prepared_not_found) {
            // If prepared statement not found, retry with query string. Shouldn't enter here more than once.
            req.query_string = stmt.raw_cql_statement;
            fwd_cql_log.debug("Prepared statement not found, retrying with query string: {}", req.query_string);
            response = co_await ser::forward_cql_rpc_verbs::send_forward_cql_execute(&_ms, target, timeout, req, topology_version);
        }
    } while (response.stale_topology && db::timeout_clock::now() < timeout);

    if (db::timeout_clock::now() >= timeout) {
        throw std::runtime_error("Forward CQL request timed out");
    }

    co_return make_result_message_from_response(response);
}

} // namespace service
