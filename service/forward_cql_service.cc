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

std::optional<seastar::lowres_clock::time_point> timeout_for_sleep(std::exception_ptr eptr);

} // namespace cql_transport

namespace service {

static logging::logger flog("forward_cql_service");

forward_cql_service::forward_cql_service(
    cql3::query_processor& qp)
    : _qp(qp)
{
    register_handlers();
}

forward_cql_service::~forward_cql_service() {
}

future<> forward_cql_service::stop() {
    co_return;
}

void forward_cql_service::register_handlers() {
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
        auto msg = co_await stmt->execute(svc._qp, qs, opts, std::nullopt);

        if (auto bounce_msg = dynamic_pointer_cast<cql_transport::messages::result_message::bounce>(msg)) {
            tracing::trace(local_trace_state, "Statement needs to bounce again on shard {}", this_shard_id());
            co_return cql_transport::cql_server::process_fn_return_type(make_foreign(std::move(bounce_msg)));
        } else {
            tracing::trace(local_trace_state, "Statement executed successfully on shard {}", this_shard_id());
            auto skip_metadata = opts.skip_metadata();
            co_return cql_transport::cql_server::process_fn_return_type(make_foreign(make_result(req.stream, *msg, local_trace_state, req.cql_version, make_metadata_id(req), skip_metadata)));
        }
    });
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
    // Query options fields need to be prepared for transport, the remaining fields can be passed more directly
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
    return forward_cql_without_checking_exceptions(std::move(stmt), std::move(prepared_id), qs, options, stream, version, std::move(metadata_id))
        .then_wrapped([&qs, stream, version] (future<forward_cql_result> f) {
            if (f.failed()) {
                auto eptr = f.get_exception();
                auto response = cql_transport::make_error_result(stream, eptr, qs.get_trace_state(), version,
                    qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::RATE_LIMIT_ERROR));
                flog.trace("Local statement execution failed with an error");
                tracing::trace(qs.get_trace_state(), "Local statement execution failed with an error");

                return forward_cql_result{
                    .response = std::move(response),
                    .error_info = forwarded_error_info{
                        .exception_code = static_cast<int32_t>(cql_transport::get_error_code(eptr)),
                        .log_message = cql_transport::make_log_message(stream, eptr),
                        .timeout = cql_transport::timeout_for_sleep(eptr),
                    },
                };
            }
            return f.get();
        });
}

future<forward_cql_result>
forward_cql_service::forward_cql_without_checking_exceptions(
    ::shared_ptr<cql3::cql_statement> stmt,
    cql3::cql_prepared_id_type prepared_id,
    service::query_state& qs,
    const cql3::query_options& options,
    uint16_t stream,
    cql_protocol_version_type version,
    cql_transport::cql_metadata_id_wrapper metadata_id)
{
    flog.trace("Trying to execute statement {} locally. Query string: {}", prepared_id, stmt->raw_cql_statement);
    tracing::trace(qs.get_trace_state(), "Trying to execute statement locally");
    auto msg = co_await stmt->execute(_qp, qs, options, std::nullopt);
    auto bounce_result_message = dynamic_pointer_cast<cql_transport::messages::result_message::bounce>(msg);
    if (!bounce_result_message) {
        // No need to bounce, return early on success
        flog.trace("Local statement execution of {} succeeded", prepared_id);
        tracing::trace(qs.get_trace_state(), "Local statement execution succeeded");

        co_return forward_cql_result{
            .response = make_foreign(cql_transport::make_result(stream, *msg, qs.get_trace_state(), version, std::move(metadata_id), options.skip_metadata())),
            .error_info = {},
        };
    }
    auto result = cql_transport::cql_server::process_fn_return_type(make_foreign(bounce_result_message));

    std::optional<forward_cql_execute_request> req;
    auto* bounce_msg = std::get_if<cql_transport::cql_server::result_with_bounce>(&result);
    while (bounce_msg) {
        if (auto target_shard = (*bounce_msg)->move_to_shard()) {
            auto&& cached_fn_calls = (*bounce_msg)->take_cached_pk_function_calls();
            auto req = make_forward_cql_request(stmt, prepared_id, qs, options, stream, version, metadata_id);
            tracing::trace(qs.get_trace_state(), "Bouncing {} to shard {}", prepared_id, *target_shard);
            result = co_await execute_on_shard(*target_shard, qs.get_client_state(), qs.get_trace_state(), std::move(req), std::move(cached_fn_calls), stmt->raw_cql_statement);
            bounce_msg = std::get_if<cql_transport::cql_server::result_with_bounce>(&result);
        } else {
            auto target = (*bounce_msg)->move_to_node();
            const auto my_host_id = _qp.db().real_database().get_token_metadata().get_topology().my_host_id();
            throw exceptions::invalid_request_exception(format(
                "Strongly consistent writes can be executed only on the leader node, "
                "leader id {}, current host id {}",
                target.host, my_host_id));
        }
    }
    auto* final_result = std::get_if<cql_transport::cql_server::result_with_foreign_response_ptr>(&result);
    auto response = std::move(*final_result).assume_value();
    flog.trace("Local statement execution of {} succeeded after bouncing", prepared_id);
    tracing::trace(qs.get_trace_state(), "Local statement execution succeeded after bouncing");

    co_return forward_cql_result{
        .response = std::move(response),
        .error_info = {},
    };
}

} // namespace service
