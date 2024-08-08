/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "server.hh"

#include <boost/bimap/unordered_set_of.hpp>
#include <boost/range/irange.hpp>
#include <boost/bimap.hpp>
#include <boost/assign.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "types/collection.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/map.hh"
#include "dht/token-sharding.hh"
#include "service/migration_manager.hh"
#include "service/memory_limiter.hh"
#include "service/storage_proxy.hh"
#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/lazy.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/core/execution_stage.hh>
#include "utils/assert.hh"
#include "utils/result_try.hh"
#include "utils/result_combinators.hh"
#include "db/operation_type.hh"

#include "enum_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "exceptions/exceptions.hh"
#include "client_data.hh"
#include "cql3/query_processor.hh"
#include "auth/authenticator.hh"

#include <cassert>
#include <string>

#include <snappy-c.h>
#include <lz4.h>

#include "response.hh"
#include "request.hh"

#include "types/user.hh"

#include "transport/cql_protocol_extension.hh"
#include "utils/bit_cast.hh"
#include "db/config.hh"
#include "utils/reusable_buffer.hh"

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

namespace cql_transport {

static logging::logger clogger("cql_server");

/**
 * Skip registering CQL metrics for these SGs - these are internal scheduling groups that are not supposed to handle CQL
 * requests.
 */
static const std::vector<sstring> non_cql_scheduling_classes_names = {
        "atexit",
        "background_reclaim",
        "compaction",
        "gossip",
        "main",
        "mem_compaction",
        "memtable",
        "memtable_to_cache",
        "streaming"
};

struct cql_frame_error : std::exception {
    const char* what() const throw () override {
        return "bad cql binary frame";
    }
};

inline int16_t consistency_to_wire(db::consistency_level c)
{
    switch (c) {
    case db::consistency_level::ANY:          return 0x0000;
    case db::consistency_level::ONE:          return 0x0001;
    case db::consistency_level::TWO:          return 0x0002;
    case db::consistency_level::THREE:        return 0x0003;
    case db::consistency_level::QUORUM:       return 0x0004;
    case db::consistency_level::ALL:          return 0x0005;
    case db::consistency_level::LOCAL_QUORUM: return 0x0006;
    case db::consistency_level::EACH_QUORUM:  return 0x0007;
    case db::consistency_level::SERIAL:       return 0x0008;
    case db::consistency_level::LOCAL_SERIAL: return 0x0009;
    case db::consistency_level::LOCAL_ONE:    return 0x000A;
    default:                                  throw std::runtime_error("Invalid consistency level");
    }
}

sstring to_string(const event::topology_change::change_type t) {
    using type = event::topology_change::change_type;
    switch (t) {
    case type::NEW_NODE:     return "NEW_NODE";
    case type::REMOVED_NODE: return "REMOVED_NODE";
    case type::MOVED_NODE:   return "MOVED_NODE";
    }
    throw std::invalid_argument("unknown change type");
}

sstring to_string(cql_binary_opcode op) {
    switch(op) {
    case cql_binary_opcode::ERROR:          return "ERROR";
    case cql_binary_opcode::STARTUP:        return "STARTUP";
    case cql_binary_opcode::READY:          return "READY";
    case cql_binary_opcode::AUTHENTICATE:   return "AUTHENTICATE";
    case cql_binary_opcode::CREDENTIALS:    return "CREDENTIALS";
    case cql_binary_opcode::OPTIONS:        return "OPTIONS";
    case cql_binary_opcode::SUPPORTED:      return "SUPPORTED";
    case cql_binary_opcode::QUERY:          return "QUERY";
    case cql_binary_opcode::RESULT:         return "RESULT";
    case cql_binary_opcode::PREPARE:        return "PREPARE";
    case cql_binary_opcode::EXECUTE:        return "EXECUTE";
    case cql_binary_opcode::REGISTER:       return "REGISTER";
    case cql_binary_opcode::EVENT:          return "EVENT";
    case cql_binary_opcode::BATCH:          return "BATCH";
    case cql_binary_opcode::AUTH_CHALLENGE: return "AUTH_CHALLENGE";
    case cql_binary_opcode::AUTH_RESPONSE:  return "AUTH_RESPONSE";
    case cql_binary_opcode::AUTH_SUCCESS:   return "AUTH_SUCCESS";
    case cql_binary_opcode::OPCODES_COUNT:  return "OPCODES_COUNT";
    }
    return format("Unknown CQL binary opcode {}", static_cast<unsigned>(op));
}

sstring to_string(const event::status_change::status_type t) {
    using type = event::status_change::status_type;
    switch (t) {
    case type::UP:   return "UP";
    case type::DOWN: return "DOWN";
    }
    throw std::invalid_argument("unknown change type");
}

sstring to_string(const event::schema_change::change_type t) {
    switch (t) {
    case event::schema_change::change_type::CREATED: return "CREATED";
    case event::schema_change::change_type::UPDATED: return "UPDATED";
    case event::schema_change::change_type::DROPPED: return "DROPPED";
    }
    SCYLLA_ASSERT(false && "unreachable");
}

sstring to_string(const event::schema_change::target_type t) {
    switch (t) {
    case event::schema_change::target_type::KEYSPACE: return "KEYSPACE";
    case event::schema_change::target_type::TABLE:    return "TABLE";
    case event::schema_change::target_type::TYPE:     return "TYPE";
    case event::schema_change::target_type::FUNCTION: return "FUNCTION";
    case event::schema_change::target_type::AGGREGATE:return "AGGREGATE";
    }
    SCYLLA_ASSERT(false && "unreachable");
}

event::event_type parse_event_type(const sstring& value)
{
    if (value == "TOPOLOGY_CHANGE") {
        return event::event_type::TOPOLOGY_CHANGE;
    } else if (value == "STATUS_CHANGE") {
        return event::event_type::STATUS_CHANGE;
    } else if (value == "SCHEMA_CHANGE") {
        return event::event_type::SCHEMA_CHANGE;
    } else {
        throw exceptions::protocol_exception(format("Invalid value '{}' for Event.Type", value));
    }
}

cql_sg_stats::cql_sg_stats(maintenance_socket_enabled used_by_maintenance_socket)
    : _cql_requests_stats(static_cast<uint8_t>(cql_binary_opcode::OPCODES_COUNT))
{
    if (used_by_maintenance_socket) {
        return;
    }

    auto& vector_ref = non_cql_scheduling_classes_names;
    if (std::find(vector_ref.begin(), vector_ref.end(), current_scheduling_group().name()) != vector_ref.end()) {
        return;
    }
    register_metrics();
}

void cql_sg_stats::register_metrics()
{
    namespace sm = seastar::metrics;
    std::vector<sm::metric_definition> transport_metrics;
    auto cur_sg_name = current_scheduling_group().name();

    for (uint8_t i = 0; i < static_cast<uint8_t>(cql_binary_opcode::OPCODES_COUNT); ++i) {
        cql_binary_opcode opcode = cql_binary_opcode{i};

        transport_metrics.emplace_back(
                sm::make_counter("cql_requests_count", [this, opcode] { return get_cql_opcode_stats(opcode).count; },
                                 sm::description("Counts the total number of CQL messages of a specific kind."),
                                 {{"kind", to_string(opcode)}, {"scheduling_group_name", cur_sg_name}}).set_skip_when_empty()
        );

        transport_metrics.emplace_back(
                sm::make_counter("cql_request_bytes", [this, opcode] { return get_cql_opcode_stats(opcode).request_size; },
                                 sm::description("Counts the total number of received bytes in CQL messages of a specific kind."),
                                 {{"kind", to_string(opcode)}, {"scheduling_group_name", cur_sg_name}}).set_skip_when_empty()
        );

        transport_metrics.emplace_back(
                sm::make_counter("cql_response_bytes", [this, opcode] { return get_cql_opcode_stats(opcode).response_size; },
                                 sm::description("Counts the total number of sent response bytes for CQL requests of a specific kind."),
                                 {{"kind", to_string(opcode)}, {"scheduling_group_name", cur_sg_name}}).set_skip_when_empty()
        );
    }

    _metrics.add_group("transport", std::move(transport_metrics));
}

cql_server::cql_server(distributed<cql3::query_processor>& qp, auth::service& auth_service,
        service::memory_limiter& ml, cql_server_config config, const db::config& db_cfg,
        qos::service_level_controller& sl_controller, gms::gossiper& g, scheduling_group_key stats_key,
        maintenance_socket_enabled used_by_maintenance_socket)
    : server("CQLServer", clogger)
    , _query_processor(qp)
    , _config(std::move(config))
    , _max_request_size(_config.max_request_size)
    , _max_concurrent_requests(db_cfg.max_concurrent_requests_per_shard)
    , _memory_available(ml.get_semaphore())
    , _notifier(std::make_unique<event_notifier>(*this))
    , _auth_service(auth_service)
    , _sl_controller(sl_controller)
    , _gossiper(g)
    , _stats_key(stats_key)
{
    namespace sm = seastar::metrics;

    if (used_by_maintenance_socket) {
        return;
    }

    auto ls = {
        sm::make_counter("cql-connections", _stats.connects,
                        sm::description("Counts a number of client connections.")),

        sm::make_gauge("current_connections", _stats.connections,
                        sm::description("Holds a current number of client connections.")),

        sm::make_counter("requests_served", _stats.requests_served,
                        sm::description("Counts a number of served requests.")),

        sm::make_gauge("requests_serving", _stats.requests_serving,
                        sm::description("Holds a number of requests that are being processed right now.")),

        sm::make_gauge("requests_blocked_memory_current", [this] { return _memory_available.waiters(); },
                        sm::description(
                            seastar::format("Holds the number of requests that are currently blocked due to reaching the memory quota limit ({}B). "
                                            "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"CQL transport\" component.", _max_request_size))),
        sm::make_counter("requests_blocked_memory", _stats.requests_blocked_memory,
                        sm::description(
                            seastar::format("Holds an incrementing counter with the requests that ever blocked due to reaching the memory quota limit ({}B). "
                                            "The first derivative of this value shows how often we block due to memory exhaustion in the \"CQL transport\" component.", _max_request_size))),
        sm::make_counter("requests_shed", _stats.requests_shed,
                        sm::description("Holds an incrementing counter with the requests that were shed due to overload (threshold configured via max_concurrent_requests_per_shard). "
                                            "The first derivative of this value shows how often we shed requests due to overload in the \"CQL transport\" component.")),
        sm::make_gauge("requests_memory_available", [this] { return _memory_available.current(); },
                        sm::description(
                            seastar::format("Holds the amount of available memory for admitting new requests (max is {}B)."
                                            "Zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"CQL transport\" component.", _max_request_size)))
    };

    std::vector<sm::metric_definition> transport_metrics;
    for (auto& m : ls) {
        transport_metrics.emplace_back(std::move(m));
    }

    sm::label cql_error_label("type");
    for (const auto& e : exceptions::exception_map()) {
        _stats.errors.insert({e.first, 0});
        auto label_instance = cql_error_label(e.second);

        transport_metrics.emplace_back(
            sm::make_counter("cql_errors_total", sm::description("Counts the total number of returned CQL errors."),
                        {label_instance},
                        [this, code = e.first] { auto it = _stats.errors.find(code); return it != _stats.errors.end() ? it->second : 0; }).set_skip_when_empty()
        );
    }

    _metrics.add_group("transport", std::move(transport_metrics));
}

cql_server::~cql_server() = default;

shared_ptr<generic_server::connection>
cql_server::make_connection(socket_address server_addr, connected_socket&& fd, socket_address addr) {
    auto conn = make_shared<connection>(*this, server_addr, std::move(fd), std::move(addr));
    ++_stats.connects;
    ++_stats.connections;
    return conn;
}

future<>
cql_server::advertise_new_connection(shared_ptr<generic_server::connection> raw_conn) {
    if (auto conn = dynamic_pointer_cast<connection>(raw_conn)) {
        const auto ip = conn->get_client_state().get_client_address().addr();
        const auto port = conn->get_client_state().get_client_port();
        clogger.trace("Advertising new connection from CQL client {}:{}", ip, port);
    }
    return make_ready_future<>();
}

future<>
cql_server::unadvertise_connection(shared_ptr<generic_server::connection> raw_conn) {
    --_stats.connections;
    if (auto conn = dynamic_pointer_cast<connection>(raw_conn)) {
        const auto ip = conn->get_client_state().get_client_address().addr();
        const auto port = conn->get_client_state().get_client_port();
        clogger.trace("Advertising disconnection of CQL client {}:{}", ip, port);
    }
    return make_ready_future<>();
}

unsigned
cql_server::connection::frame_size() const {
    return 9;
}

cql_binary_frame_v3
cql_server::connection::parse_frame(temporary_buffer<char> buf) const {
    if (buf.size() != frame_size()) {
        throw cql_frame_error();
    }
    cql_binary_frame_v3 v3;
    switch (_version) {
    case 3:
    case 4: {
        cql_binary_frame_v3 raw = read_unaligned<cql_binary_frame_v3>(buf.get());
        v3 = net::ntoh(raw);
        break;
    }
    default:
        throw exceptions::protocol_exception(format("Invalid or unsupported protocol version: {:d}", _version));
    }
    if (v3.version != _version) {
        throw exceptions::protocol_exception(format("Invalid message version. Got {:d} but previous messages on this connection had version {:d}", v3.version, _version));

    }
    return v3;
}

future<std::optional<cql_binary_frame_v3>>
cql_server::connection::read_frame() {
    using ret_type = std::optional<cql_binary_frame_v3>;
    if (!_version) {
        // We don't know the frame size before reading the first frame,
        // so read just one byte, and then read the rest of the frame.
        return _read_buf.read_exactly(1).then([this] (temporary_buffer<char> buf) {
            if (buf.empty()) {
                return make_ready_future<ret_type>();
            }
            _version = buf[0];
            if (_version < 3 || _version > current_version) {
                auto client_version = _version;
                _version = current_version;
                throw exceptions::protocol_exception(format("Invalid or unsupported protocol version: {:d}", client_version));
            }


            return _read_buf.read_exactly(frame_size() - 1).then([this] (temporary_buffer<char> tail) {
                temporary_buffer<char> full(frame_size());
                full.get_write()[0] = _version;
                std::copy(tail.get(), tail.get() + tail.size(), full.get_write() + 1);
                auto frame = parse_frame(std::move(full));
                // This is the very first frame, so reject obviously incorrect frames, to
                // avoid allocating large amounts of memory for the message body
                if (frame.length > 100'000) {
                    // The STARTUP message body is a [string map] containing just a few options,
                    // so it should be smaller that 100kB. See #4366.
                    throw exceptions::protocol_exception(format("Initial message size too large ({:d}), rejecting as invalid", uint32_t(frame.length)));
                }
                return make_ready_future<ret_type>(frame);
            });
        });
    } else {
        // Not the first frame, so we know the size.
        return _read_buf.read_exactly(frame_size()).then([this] (temporary_buffer<char> buf) {
            if (buf.empty()) {
                return make_ready_future<ret_type>();
            }
            return make_ready_future<ret_type>(parse_frame(std::move(buf)));
        });
    }
}

future<foreign_ptr<std::unique_ptr<cql_server::response>>>
    cql_server::connection::process_request_one(fragmented_temporary_buffer::istream fbuf, uint8_t op, uint16_t stream, service::client_state& client_state, tracing_request_type tracing_request, service_permit permit) {
    using auth_state = service::client_state::auth_state;

    auto cqlop = static_cast<cql_binary_opcode>(op);
    tracing::trace_state_props_set trace_props;

    trace_props.set_if<tracing::trace_state_props::log_slow_query>(tracing::tracing::get_local_tracing_instance().slow_query_tracing_enabled());
    trace_props.set_if<tracing::trace_state_props::full_tracing>(tracing_request != tracing_request_type::not_requested);
    tracing::trace_state_ptr trace_state;

    if (trace_props) {
        if (cqlop == cql_binary_opcode::QUERY ||
            cqlop == cql_binary_opcode::PREPARE ||
            cqlop == cql_binary_opcode::EXECUTE ||
            cqlop == cql_binary_opcode::BATCH) {
            trace_props.set_if<tracing::trace_state_props::write_on_close>(tracing_request == tracing_request_type::write_on_close);
            trace_state = tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, trace_props);
        }
    }

    cql_sg_stats::request_kind_stats& cql_stats = _server.get_cql_opcode_stats(cqlop);
    tracing::set_request_size(trace_state, fbuf.bytes_left());
    cql_stats.request_size += fbuf.bytes_left();
    ++cql_stats.count;

    auto linearization_buffer = std::make_unique<bytes_ostream>();
    auto linearization_buffer_ptr = linearization_buffer.get();
    return futurize_invoke([this, cqlop, stream, &fbuf, &client_state, linearization_buffer_ptr, permit = std::move(permit), trace_state] () mutable {
        // When using authentication, we need to ensure we are doing proper state transitions,
        // i.e. we cannot simply accept any query/exec ops unless auth is complete
        switch (client_state.get_auth_state()) {
            case auth_state::UNINITIALIZED:
                if (cqlop != cql_binary_opcode::STARTUP && cqlop != cql_binary_opcode::OPTIONS) {
                    throw exceptions::protocol_exception(format("Unexpected message {:d}, expecting STARTUP or OPTIONS", int(cqlop)));
                }
                break;
            case auth_state::AUTHENTICATION:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                if (cqlop != cql_binary_opcode::AUTH_RESPONSE && cqlop != cql_binary_opcode::CREDENTIALS) {
                    throw exceptions::protocol_exception(format("Unexpected message {:d}, expecting {}", int(cqlop), "SASL_RESPONSE"));
                }
                break;
            case auth_state::READY: default:
                if (cqlop == cql_binary_opcode::STARTUP) {
                    throw exceptions::protocol_exception("Unexpected message STARTUP, the connection is already initialized");
                }
                break;
        }

        tracing::set_username(trace_state, client_state.user());

        auto wrap_in_foreign = [] (future<std::unique_ptr<cql_server::response>> f) {
            return f.then([] (std::unique_ptr<cql_server::response> p) {
                return make_ready_future<result_with_foreign_response_ptr>(make_foreign(std::move(p)));
            });
        };
        auto in = request_reader(std::move(fbuf), *linearization_buffer_ptr);
        switch (cqlop) {
        case cql_binary_opcode::STARTUP:       return wrap_in_foreign(process_startup(stream, std::move(in), client_state, trace_state));
        case cql_binary_opcode::AUTH_RESPONSE: return wrap_in_foreign(process_auth_response(stream, std::move(in), client_state, trace_state));
        case cql_binary_opcode::OPTIONS:       return wrap_in_foreign(process_options(stream, std::move(in), client_state, trace_state));
        case cql_binary_opcode::QUERY:         return process_query(stream, std::move(in), client_state, std::move(permit), trace_state);
        case cql_binary_opcode::PREPARE:       return wrap_in_foreign(process_prepare(stream, std::move(in), client_state, trace_state));
        case cql_binary_opcode::EXECUTE:       return process_execute(stream, std::move(in), client_state, std::move(permit), trace_state);
        case cql_binary_opcode::BATCH:         return process_batch(stream, std::move(in), client_state, std::move(permit), trace_state);
        case cql_binary_opcode::REGISTER:      return wrap_in_foreign(process_register(stream, std::move(in), client_state, trace_state));
        default:                               throw exceptions::protocol_exception(format("Unknown opcode {:d}", int(cqlop)));
        }
    }).then_wrapped([this, cqlop, &cql_stats, stream, &client_state, linearization_buffer = std::move(linearization_buffer), trace_state] (future<result_with_foreign_response_ptr> f) {
        auto stop_trace = defer([&] {
            tracing::stop_foreground(trace_state);
        });
        --_server._stats.requests_serving;

        return utils::result_into_future<result_with_foreign_response_ptr>(utils::result_try([&] () -> result_with_foreign_response_ptr {
            result_with_foreign_response_ptr res = f.get();
            if (!res) {
                return res;
            }

            auto response = std::move(res).assume_value();
            auto res_op = response->opcode();

            // and modify state now that we've generated a response.
            switch (client_state.get_auth_state()) {
            case auth_state::UNINITIALIZED:
                if (cqlop == cql_binary_opcode::STARTUP) {
                    if (res_op == cql_binary_opcode::AUTHENTICATE) {
                        client_state.set_auth_state(auth_state::AUTHENTICATION);
                    } else if (res_op == cql_binary_opcode::READY) {
                        client_state.set_auth_state(auth_state::READY);
                    }
                }
                break;
            case auth_state::AUTHENTICATION:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                if (cqlop != cql_binary_opcode::AUTH_RESPONSE && cqlop != cql_binary_opcode::CREDENTIALS) {
                    throw exceptions::protocol_exception(format("Unexpected message {:d}, expecting AUTH_RESPONSE or CREDENTIALS", int(cqlop)));
                }
                if (res_op == cql_binary_opcode::READY || res_op == cql_binary_opcode::AUTH_SUCCESS) {
                    client_state.set_auth_state(auth_state::READY);
                }
                break;
            default:
            case auth_state::READY:
                break;
            }

            tracing::set_response_size(trace_state, response->size());
            cql_stats.response_size += response->size();
            return response;
        },  utils::result_catch<exceptions::unavailable_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in unavailable_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_unavailable_error(stream, ex.code(), ex.what(), ex.consistency, ex.required, ex.alive, trace_state);
        }), utils::result_catch<exceptions::read_timeout_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in read_timeout_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_read_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.data_present, trace_state);
        }), utils::result_catch<exceptions::read_failure_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in read_failure_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_read_failure_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.failures, ex.block_for, ex.data_present, trace_state);
        }), utils::result_catch<exceptions::mutation_write_timeout_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in mutation_write_timeout_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_mutation_write_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.type, trace_state);
        }), utils::result_catch<exceptions::mutation_write_failure_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in mutation_write_failure_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_mutation_write_failure_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.failures, ex.block_for, ex.type, trace_state);
        }), utils::result_catch<exceptions::already_exists_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in already_exists_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_already_exists_error(stream, ex.code(), ex.what(), ex.ks_name, ex.cf_name, trace_state);
        }), utils::result_catch<exceptions::prepared_query_not_found_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in unprepared_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_unprepared_error(stream, ex.code(), ex.what(), ex.id, trace_state);
        }), utils::result_catch<exceptions::function_execution_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in function_failure_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_function_failure_error(stream, ex.code(), ex.what(), ex.ks_name, ex.func_name, ex.args, trace_state);
        }), utils::result_catch<exceptions::rate_limit_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in rate_limit_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_rate_limit_error(stream, ex.code(), ex.what(), ex.op_type, ex.rejected_by_coordinator, trace_state, client_state);
        }), utils::result_catch<exceptions::cassandra_exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in cassandra_error, stream {}, code {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.code(), ex.what());
            // Note: the CQL protocol specifies that many types of errors have
            // mandatory parameters. These cassandra_exception subclasses MUST
            // be handled above. This default "cassandra_exception" case is
            // only appropriate for the specific types of errors which do not have
            // additional information, such as invalid_request_exception.
            // TODO: consider listing those types explicitly, instead of the
            // catch-all type cassandra_exception.
            try { ++_server._stats.errors[ex.code()]; } catch(...) {}
            return make_error(stream, ex.code(), ex.what(), trace_state);
        }), utils::result_catch<std::exception>([&] (const auto& ex) {
            clogger.debug("{}: request resulted in error, stream {}, message [{}]",
                _client_state.get_remote_address(), stream, ex.what());
            try { ++_server._stats.errors[exceptions::exception_code::SERVER_ERROR]; } catch(...) {}
            sstring msg = ex.what();
            try {
                std::rethrow_if_nested(ex);
            } catch (...) {
                std::ostringstream ss;
                ss << msg << ": " << std::current_exception();
                msg = ss.str();
            }
            return make_error(stream, exceptions::exception_code::SERVER_ERROR, msg, trace_state);
        }), utils::result_catch_dots([&] () {
            clogger.debug("{}: request resulted in unknown error, stream {}",
                _client_state.get_remote_address(), stream);
            try { ++_server._stats.errors[exceptions::exception_code::SERVER_ERROR]; } catch(...) {}
            return make_error(stream, exceptions::exception_code::SERVER_ERROR, "unknown error", trace_state);
        })));
    });
}

cql_server::connection::connection(cql_server& server, socket_address server_addr, connected_socket&& fd, socket_address addr)
    : generic_server::connection{server, std::move(fd)}
    , _server(server)
    , _server_addr(server_addr)
    , _client_state(service::client_state::external_tag{}, server._auth_service, &server._sl_controller, server.timeout_config(), addr)
{
    _shedding_timer.set_callback([this] {
        clogger.debug("Shedding all incoming requests due to overload");
        _shed_incoming_requests = true;
    });
}

cql_server::connection::~connection() {
}

void cql_server::connection::on_connection_close()
{
    _server._notifier->unregister_connection(this);
}

std::pair<net::inet_address, int> cql_server::connection::make_client_key(const service::client_state& cli_state) {
    return {cli_state.get_client_address().addr(),
            cli_state.get_client_port()};
}

client_data cql_server::connection::make_client_data() const {
    client_data cd;
    std::tie(cd.ip, cd.port) = make_client_key(_client_state);
    cd.shard_id = this_shard_id();
    cd.protocol_version = _version;
    cd.driver_name = _client_state.get_driver_name();
    cd.driver_version = _client_state.get_driver_version();
    if (const auto user_ptr = _client_state.user(); user_ptr) {
        cd.username = user_ptr->name;
    }
    if (_ready) {
        cd.connection_stage = client_connection_stage::ready;
    } else if (_authenticating) {
        cd.connection_stage = client_connection_stage::authenticating;
    }
    return cd;
}

thread_local cql_server::connection::execution_stage_type
        cql_server::connection::_process_request_stage{"transport", &connection::process_request_one};

void cql_server::connection::handle_error(future<>&& f) {
    try {
        f.get();
    } catch (const exceptions::cassandra_exception& ex) {
        clogger.debug("{}: connection error, code {}, message [{}]", _client_state.get_remote_address(), ex.code(), ex.what());
        try { ++_server._stats.errors[ex.code()]; } catch(...) {}
        write_response(make_error(0, ex.code(), ex.what(), tracing::trace_state_ptr()));
    } catch (std::exception& ex) {
        clogger.debug("{}: connection error, message [{}]", _client_state.get_remote_address(), ex.what());
        try { ++_server._stats.errors[exceptions::exception_code::SERVER_ERROR]; } catch(...) {}
        write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, ex.what(), tracing::trace_state_ptr()));
    } catch (...) {
        clogger.debug("{}: connection error, unknown error", _client_state.get_remote_address());
        try { ++_server._stats.errors[exceptions::exception_code::SERVER_ERROR]; } catch(...) {}
        write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, "unknown error", tracing::trace_state_ptr()));
    }
}

future<> cql_server::connection::process_request() {
    return read_frame().then_wrapped([this] (future<std::optional<cql_binary_frame_v3>>&& v) {
        auto maybe_frame = v.get();
        if (!maybe_frame) {
            // eof
            return make_ready_future<>();
        }

        auto& f = *maybe_frame;

        const bool allow_shedding = _client_state.get_workload_type() == service::client_state::workload_type::interactive;
        if (allow_shedding && _shed_incoming_requests) {
            ++_server._stats.requests_shed;
            return _read_buf.skip(f.length).then([this, stream = f.stream] {
                const char* message = "request shed due to coordinator overload";
                clogger.debug("{}: {}, stream {}", _client_state.get_remote_address(), message, uint16_t(stream));
                write_response(make_error(stream, exceptions::exception_code::OVERLOADED,
                    message, tracing::trace_state_ptr()));
                return make_ready_future<>();
            });
        }

        tracing_request_type tracing_requested = tracing_request_type::not_requested;
        if (f.flags & cql_frame_flags::tracing) {
            // If tracing is requested for a specific CQL command - flush
            // tracing info right after the command is over.
            tracing_requested = tracing_request_type::write_on_close;
        } else if (tracing::tracing::get_local_tracing_instance().trace_next_query()) {
            tracing_requested = tracing_request_type::no_write_on_close;
        }

        auto op = f.opcode;
        auto stream = f.stream;
        auto mem_estimate = f.length * 2 + 8000; // Allow for extra copies and bookkeeping
        if (mem_estimate > _server._max_request_size) {
            const auto message = format("request size too large (frame size {:d}; estimate {:d}; allowed {:d})",
                uint32_t(f.length), mem_estimate, _server._max_request_size);
            clogger.debug("{}: {}, request dropped", _client_state.get_remote_address(), message);
            write_response(make_error(stream, exceptions::exception_code::INVALID, message, tracing::trace_state_ptr()));
            return std::exchange(_ready_to_respond, make_ready_future<>())
                .then([this] { return _read_buf.close(); })
                .then([this] { return util::skip_entire_stream(_read_buf); });
        }

        if (_server._stats.requests_serving > _server._max_concurrent_requests) {
            ++_server._stats.requests_shed;
            return _read_buf.skip(f.length).then([this, stream = f.stream] {
                const auto message = format("too many in-flight requests (configured via max_concurrent_requests_per_shard): {}",
                                            _server._stats.requests_serving);
                clogger.debug("{}: {}, request dropped", _client_state.get_remote_address(), message);
                write_response(make_error(stream, exceptions::exception_code::OVERLOADED,
                    message,
                    tracing::trace_state_ptr()));
                return make_ready_future<>();
            });
        }

        const auto shedding_timeout = std::chrono::milliseconds(50);
        auto fut = allow_shedding
                ? get_units(_server._memory_available, mem_estimate, shedding_timeout).then_wrapped([this, length = f.length] (auto f) {
                    try {
                        return make_ready_future<semaphore_units<>>(f.get());
                    } catch (semaphore_timed_out& sto) {
                        // Cancel shedding in case no more requests are going to do that on completion
                        if (_pending_requests_gate.get_count() == 0) {
                            _shed_incoming_requests = false;
                        }
                        return _read_buf.skip(length).then([sto = std::move(sto)] () mutable {
                            return make_exception_future<semaphore_units<>>(std::move(sto));
                        });
                    }
                })
                : get_units(_server._memory_available, mem_estimate);
        if (_server._memory_available.waiters()) {
            if (allow_shedding && !_shedding_timer.armed()) {
                _shedding_timer.arm(shedding_timeout);
            }
            ++_server._stats.requests_blocked_memory;
        }

        return fut.then_wrapped([this, length = f.length, flags = f.flags, op, stream, tracing_requested] (auto mem_permit_fut) {
          if (mem_permit_fut.failed()) {
              // Ignore semaphore errors - they are expected if load shedding took place
              mem_permit_fut.ignore_ready_future();
              return make_ready_future<>();
          }
          semaphore_units<> mem_permit = mem_permit_fut.get();
          return this->read_and_decompress_frame(length, flags).then([this, op, stream, tracing_requested, mem_permit = make_service_permit(std::move(mem_permit))] (fragmented_temporary_buffer buf) mutable {

            ++_server._stats.requests_served;
            ++_server._stats.requests_serving;

            _pending_requests_gate.enter();
            auto leave = defer([this] {
                _shedding_timer.cancel();
                _shed_incoming_requests = false;
                _pending_requests_gate.leave();
            });
            auto istream = buf.get_istream();


            // Parallelize only the performance sensitive requests:
            // QUERY, PREPARE, EXECUTE, BATCH
            bool should_paralelize = (op == uint8_t(cql_binary_opcode::QUERY) ||
                    op == uint8_t(cql_binary_opcode::PREPARE) ||
                    op == uint8_t (cql_binary_opcode::EXECUTE) ||
                    op == uint8_t(cql_binary_opcode::BATCH));

            future<foreign_ptr<std::unique_ptr<cql_server::response>>> request_process_future = should_paralelize ?
                    _process_request_stage(this, istream, op, stream, seastar::ref(_client_state), tracing_requested, mem_permit) :
                    process_request_one(istream, op, stream, seastar::ref(_client_state), tracing_requested, mem_permit);

            future<> request_response_future = request_process_future.then_wrapped([this, buf = std::move(buf), mem_permit, leave = std::move(leave), stream] (future<foreign_ptr<std::unique_ptr<cql_server::response>>> response_f) mutable {
                try {
                    if (response_f.failed()) {
                        const auto message = format("request processing failed, error [{}]", response_f.get_exception());
                        clogger.error("{}: {}", _client_state.get_remote_address(), message);
                        write_response(make_error(stream, exceptions::exception_code::SERVER_ERROR,
                                                  message,
                                                  tracing::trace_state_ptr()));
                    } else {
                        write_response(response_f.get(), std::move(mem_permit), _compression);
                    }
                    _ready_to_respond = _ready_to_respond.finally([leave = std::move(leave)] {});
                } catch (...) {
                    clogger.error("{}: request processing failed: {}",
                                  _client_state.get_remote_address(), std::current_exception());
                }
            });

            if (should_paralelize) {
                return make_ready_future<>();
            } else {
                return request_response_future;
            }
          });
        });
    });
}

// Contiguous buffers for use with compression primitives.
// Be careful when dealing with them, because they are shared and
// can be modified on preemption points.
// See the comments on reusable_buffer for a discussion.
static utils::reusable_buffer_guard input_buffer_guard() {
    using namespace std::chrono_literals;
    static thread_local utils::reusable_buffer<lowres_clock> buf(600s);
    return buf;
}
static utils::reusable_buffer_guard output_buffer_guard() {
    using namespace std::chrono_literals;
    static thread_local utils::reusable_buffer<lowres_clock> buf(600s);
    return buf;
}

future<fragmented_temporary_buffer> cql_server::connection::read_and_decompress_frame(size_t length, uint8_t flags)
{
    if (flags & cql_frame_flags::compression) {
        if (_compression == cql_compression::lz4) {
            if (length < 4) {
                throw std::runtime_error(fmt::format("CQL frame truncated: expected to have at least 4 bytes, got {}", length));
            }
            return _buffer_reader.read_exactly(_read_buf, length).then([] (fragmented_temporary_buffer buf) {
                auto input_buffer = input_buffer_guard();
                auto output_buffer = output_buffer_guard();
                auto v = fragmented_temporary_buffer::view(buf);
                int32_t uncomp_len = read_simple<int32_t>(v);
                if (uncomp_len < 0) {
                    throw std::runtime_error("CQL frame uncompressed length is negative: " + std::to_string(uncomp_len));
                }
                auto in = input_buffer.get_linearized_view(v);
                return output_buffer.make_fragmented_temporary_buffer(uncomp_len, [&in] (bytes_mutable_view out) {
                    auto ret = LZ4_decompress_safe(reinterpret_cast<const char*>(in.data()), reinterpret_cast<char*>(out.data()), in.size(), out.size());
                    if (ret < 0) {
                        throw std::runtime_error("CQL frame LZ4 uncompression failure");
                    }
                    if (static_cast<size_t>(ret) != out.size()) {  // ret is known to be positive here
                        throw std::runtime_error("Malformed CQL frame - provided uncompressed size different than real uncompressed size");
                    }
                    return static_cast<size_t>(ret);
                });
            });
        } else if (_compression == cql_compression::snappy) {
            return _buffer_reader.read_exactly(_read_buf, length).then([] (fragmented_temporary_buffer buf) {
                auto input_buffer = input_buffer_guard();
                auto output_buffer = output_buffer_guard();
                auto in = input_buffer.get_linearized_view(fragmented_temporary_buffer::view(buf));
                size_t uncomp_len;
                if (snappy_uncompressed_length(reinterpret_cast<const char*>(in.data()), in.size(), &uncomp_len) != SNAPPY_OK) {
                    throw std::runtime_error("CQL frame Snappy uncompressed size is unknown");
                }
                return output_buffer.make_fragmented_temporary_buffer(uncomp_len, [&in] (bytes_mutable_view out) {
                    size_t output_len = out.size();
                    if (snappy_uncompress(reinterpret_cast<const char*>(in.data()), in.size(), reinterpret_cast<char*>(out.data()), &output_len) != SNAPPY_OK) {
                        throw std::runtime_error("CQL frame Snappy uncompression failure");
                    }
                    if (output_len != out.size()) {
                        throw std::runtime_error("Malformed CQL frame - provided uncompressed size different than real uncompressed size");
                    }
                    return output_len;
                });
            });
        } else {
            throw exceptions::protocol_exception(format("Unknown compression algorithm"));
        }
    }
    return _buffer_reader.read_exactly(_read_buf, length);
}

future<std::unique_ptr<cql_server::response>> cql_server::connection::process_startup(uint16_t stream, request_reader in, service::client_state& client_state,
        tracing::trace_state_ptr trace_state) {
    auto options = in.read_string_map();
    auto compression_opt = options.find("COMPRESSION");
    if (compression_opt != options.end()) {
         auto compression = compression_opt->second;
         std::transform(compression.begin(), compression.end(), compression.begin(), ::tolower);
         if (compression == "lz4") {
             _compression = cql_compression::lz4;
         } else if (compression == "snappy") {
             _compression = cql_compression::snappy;
         } else {
             throw exceptions::protocol_exception(format("Unknown compression algorithm: {}", compression));
         }
    }

    if (auto driver_ver_opt = options.find("DRIVER_VERSION"); driver_ver_opt != options.end()) {
        _client_state.set_driver_version(driver_ver_opt->second);
    }
    if (auto driver_name_opt = options.find("DRIVER_NAME"); driver_name_opt != options.end()) {
        _client_state.set_driver_name(driver_name_opt->second);
    }

    cql_protocol_extension_enum_set cql_proto_exts;
    for (cql_protocol_extension ext : supported_cql_protocol_extensions()) {
        if (options.contains(protocol_extension_name(ext))) {
            cql_proto_exts.set(ext);
        }
    }
    _client_state.set_protocol_extensions(std::move(cql_proto_exts));
    std::unique_ptr<cql_server::response> res;
    if (auto& a = client_state.get_auth_service()->underlying_authenticator(); a.require_authentication()) {
        _authenticating = true;
        auto opt_user = co_await a.authenticate([this]() -> future<std::optional<auth::certificate_info>> {
            auto dn_info = co_await tls::get_dn_information(this->_fd);
            if (dn_info) {
                co_return auth::certificate_info{ dn_info->subject, [this]() -> future<std::string> {
                    auto altnames = co_await tls::get_alt_name_information(this->_fd);
                    auto res = fmt::format("{}", fmt::join(altnames, ","));
                    co_return res;
                } };
            }
            co_return std::nullopt;
        });
        if (opt_user) {
            client_state.set_login(std::move(*opt_user));
            co_await client_state.check_user_can_login();
            co_await client_state.maybe_update_per_service_level_params();
            res = make_ready(stream, trace_state);
        } else {
            res = make_autheticate(stream, a.qualified_java_name(), trace_state);
        }
    } else {
        _ready = true;
        res = make_ready(stream, trace_state);
    }

    co_return res;
}

future<std::unique_ptr<cql_server::response>> cql_server::connection::process_auth_response(uint16_t stream, request_reader in, service::client_state& client_state,
        tracing::trace_state_ptr trace_state) {
    auto sasl_challenge = client_state.get_auth_service()->underlying_authenticator().new_sasl_challenge();
    auto buf = in.read_raw_bytes_view(in.bytes_left());
    auto challenge = sasl_challenge->evaluate_response(buf);
    if (sasl_challenge->is_complete()) {
        return sasl_challenge->get_authenticated_user().then([this, sasl_challenge, stream, &client_state, challenge = std::move(challenge), trace_state](auth::authenticated_user user) mutable {
            client_state.set_login(std::move(user));
            auto f = client_state.check_user_can_login();
            f = f.then([&client_state] {
                return client_state.maybe_update_per_service_level_params();
            });
            return f.then([this, stream, challenge = std::move(challenge), trace_state]() mutable {
                return make_ready_future<std::unique_ptr<cql_server::response>>(make_auth_success(stream, std::move(challenge), trace_state));
            });
        });
    }
    return make_ready_future<std::unique_ptr<cql_server::response>>(make_auth_challenge(stream, std::move(challenge), trace_state));
}

future<std::unique_ptr<cql_server::response>> cql_server::connection::process_options(uint16_t stream, request_reader in, service::client_state& client_state,
        tracing::trace_state_ptr trace_state) {
    return make_ready_future<std::unique_ptr<cql_server::response>>(make_supported(stream, std::move(trace_state)));
}

std::unique_ptr<cql_server::response>
make_result(int16_t stream, messages::result_message& msg, const tracing::trace_state_ptr& tr_state,
        cql_protocol_version_type version, bool skip_metadata = false);

template<typename Process>
future<cql_server::result_with_foreign_response_ptr>
cql_server::connection::process_on_shard(::shared_ptr<messages::result_message::bounce_to_shard> bounce_msg, uint16_t stream, fragmented_temporary_buffer::istream is,
        service::client_state& cs, service_permit permit, tracing::trace_state_ptr trace_state, Process process_fn) {
    return _server.container().invoke_on(*bounce_msg->move_to_shard(), _server._config.bounce_request_smp_service_group,
            [this, is = std::move(is), cs = cs.move_to_other_shard(), stream, permit = std::move(permit), process_fn,
             gt = tracing::global_trace_state_ptr(std::move(trace_state)),
             cached_vals = std::move(bounce_msg->take_cached_pk_function_calls())] (cql_server& server) {
        service::client_state client_state = cs.get();
        return do_with(bytes_ostream(), std::move(client_state), std::move(cached_vals),
                [this, &server, is = std::move(is), stream, process_fn,
                 trace_state = tracing::trace_state_ptr(gt)] (bytes_ostream& linearization_buffer,
                    service::client_state& client_state,
                    cql3::computed_function_values& cached_vals) mutable {
            request_reader in(is, linearization_buffer);
            return process_fn(client_state, server._query_processor, in, stream, _version,
                    /* FIXME */empty_service_permit(), std::move(trace_state), false, std::move(cached_vals)).then([] (auto msg) {
                // result here has to be foreign ptr
                return std::get<cql_server::result_with_foreign_response_ptr>(std::move(msg));
            });
        });
    });
}

using process_fn_return_type = std::variant<
    cql_server::result_with_foreign_response_ptr,
    ::shared_ptr<messages::result_message::bounce_to_shard>>;

static inline cql_server::result_with_foreign_response_ptr convert_error_message_to_coordinator_result(messages::result_message* msg) {
    return std::move(*dynamic_cast<messages::result_message::exception*>(msg)).get_exception();
}

template<typename Process>
future<cql_server::result_with_foreign_response_ptr>
cql_server::connection::process(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit,
        tracing::trace_state_ptr trace_state, Process process_fn) {
    fragmented_temporary_buffer::istream is = in.get_stream();

    return process_fn(client_state, _server._query_processor, in, stream,
            _version, permit, trace_state, true, {})
            .then([stream, &client_state, this, is, permit, process_fn, trace_state]
                   (process_fn_return_type msg) mutable {
        auto* bounce_msg = std::get_if<shared_ptr<messages::result_message::bounce_to_shard>>(&msg);
        if (bounce_msg) {
            return process_on_shard(*bounce_msg, stream, is, client_state, std::move(permit), trace_state, process_fn);
        }
        auto ptr = std::get<cql_server::result_with_foreign_response_ptr>(std::move(msg));
        return make_ready_future<cql_server::result_with_foreign_response_ptr>(std::move(ptr));
    });
}

static future<process_fn_return_type>
process_query_internal(service::client_state& client_state, distributed<cql3::query_processor>& qp, request_reader in,
        uint16_t stream, cql_protocol_version_type version,
        service_permit permit, tracing::trace_state_ptr trace_state, bool init_trace, cql3::computed_function_values cached_pk_fn_calls) {
    auto query = in.read_long_string_view();
    auto q_state = std::make_unique<cql_query_state>(client_state, trace_state, std::move(permit));
    auto& query_state = q_state->query_state;
    q_state->options = in.read_options(version, qp.local().get_cql_config());
    auto& options = *q_state->options;
    if (!cached_pk_fn_calls.empty()) {
        options.set_cached_pk_function_calls(std::move(cached_pk_fn_calls));
    }
    auto skip_metadata = options.skip_metadata();

    if (init_trace) {
        tracing::set_page_size(trace_state, options.get_page_size());
        tracing::set_consistency_level(trace_state, options.get_consistency());
        tracing::set_optional_serial_consistency_level(trace_state, options.get_serial_consistency());
        tracing::add_query(trace_state, query);
        tracing::set_user_timestamp(trace_state, options.get_specific_options().timestamp);

        tracing::begin(trace_state, "Execute CQL3 query", client_state.get_client_address());
    }

    return qp.local().execute_direct_without_checking_exception_message(query, query_state, options).then([q_state = std::move(q_state), stream, skip_metadata, version] (auto msg) {
        if (msg->move_to_shard()) {
            return process_fn_return_type(dynamic_pointer_cast<messages::result_message::bounce_to_shard>(msg));
        } else if (msg->is_exception()) {
            return process_fn_return_type(convert_error_message_to_coordinator_result(msg.get()));
        } else {
            tracing::trace(q_state->query_state.get_trace_state(), "Done processing - preparing a result");
            return process_fn_return_type(make_foreign(make_result(stream, *msg, q_state->query_state.get_trace_state(), version, skip_metadata)));
        }
    });
}

future<cql_server::result_with_foreign_response_ptr>
cql_server::connection::process_query(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state) {
    return process(stream, in, client_state, std::move(permit), std::move(trace_state), process_query_internal);
}

future<std::unique_ptr<cql_server::response>> cql_server::connection::process_prepare(uint16_t stream, request_reader in, service::client_state& client_state,
        tracing::trace_state_ptr trace_state) {

    auto query = sstring(in.read_long_string_view());

    tracing::add_query(trace_state, query);
    tracing::begin(trace_state, "Preparing CQL3 query", client_state.get_client_address());

    return _server._query_processor.invoke_on_others([query, &client_state] (auto& qp) mutable {
            return qp.prepare(std::move(query), client_state).discard_result();
    }).then([this, query, stream, &client_state, trace_state] () mutable {
        tracing::trace(trace_state, "Done preparing on remote shards");
        return _server._query_processor.local().prepare(std::move(query), client_state).then([this, stream, trace_state] (auto msg) {
            tracing::trace(trace_state, "Done preparing on a local shard - preparing a result. ID is [{}]", seastar::value_of([&msg] {
                return messages::result_message::prepared::cql::get_id(msg);
            }));
            return make_result(stream, *msg, trace_state, _version);
        });
    });
}

static future<process_fn_return_type>
process_execute_internal(service::client_state& client_state, distributed<cql3::query_processor>& qp, request_reader in,
        uint16_t stream, cql_protocol_version_type version,
        service_permit permit, tracing::trace_state_ptr trace_state, bool init_trace, cql3::computed_function_values cached_pk_fn_calls) {
    cql3::prepared_cache_key_type cache_key(in.read_short_bytes());
    auto& id = cql3::prepared_cache_key_type::cql_id(cache_key);
    bool needs_authorization = false;

    // First, try to lookup in the cache of already authorized statements. If the corresponding entry is not found there
    // look for the prepared statement and then authorize it.
    auto prepared = qp.local().get_prepared(client_state.user(), cache_key);
    if (!prepared) {
        needs_authorization = true;
        prepared = qp.local().get_prepared(cache_key);
    }

    if (!prepared) {
        throw exceptions::prepared_query_not_found_exception(id);
    }

    auto q_state = std::make_unique<cql_query_state>(client_state, trace_state, std::move(permit));
    auto& query_state = q_state->query_state;
    q_state->options = in.read_options(version, qp.local().get_cql_config());
    auto& options = *q_state->options;
    if (!cached_pk_fn_calls.empty()) {
        options.set_cached_pk_function_calls(std::move(cached_pk_fn_calls));
    }
    auto skip_metadata = options.skip_metadata();

    if (init_trace) {
        tracing::set_page_size(trace_state, options.get_page_size());
        tracing::set_consistency_level(trace_state, options.get_consistency());
        tracing::set_optional_serial_consistency_level(trace_state, options.get_serial_consistency());
        tracing::add_query(trace_state, prepared->statement->raw_cql_statement);
        tracing::add_prepared_statement(trace_state, prepared);

        tracing::begin(trace_state, seastar::value_of([&id] { return seastar::format("Execute CQL3 prepared query [{}]", id); }),
                client_state.get_client_address());
    }

    auto stmt = prepared->statement;
    tracing::trace(query_state.get_trace_state(), "Checking bounds");
    if (stmt->get_bound_terms() != options.get_values_count()) {
        const auto msg = format("Invalid amount of bind variables: expected {:d} received {:d}",
                stmt->get_bound_terms(),
                options.get_values_count());
        tracing::trace(query_state.get_trace_state(), "{}", msg);
        throw exceptions::invalid_request_exception(msg);
    }

    options.prepare(prepared->bound_names);

    if (init_trace) {
        tracing::add_prepared_query_options(trace_state, options);
    }

    tracing::trace(trace_state, "Processing a statement");
    return qp.local().execute_prepared_without_checking_exception_message(query_state, std::move(stmt), options, std::move(prepared), std::move(cache_key), needs_authorization)
            .then([trace_state = query_state.get_trace_state(), skip_metadata, q_state = std::move(q_state), stream, version] (auto msg) {
        if (msg->move_to_shard()) {
            return process_fn_return_type(dynamic_pointer_cast<messages::result_message::bounce_to_shard>(msg));
        } else if (msg->is_exception()) {
            return process_fn_return_type(convert_error_message_to_coordinator_result(msg.get()));
        } else {
            tracing::trace(q_state->query_state.get_trace_state(), "Done processing - preparing a result");
            return process_fn_return_type(make_foreign(make_result(stream, *msg, q_state->query_state.get_trace_state(), version, skip_metadata)));
        }
    });
}

future<cql_server::result_with_foreign_response_ptr> cql_server::connection::process_execute(uint16_t stream, request_reader in,
        service::client_state& client_state, service_permit permit, tracing::trace_state_ptr trace_state) {
    return process(stream, in, client_state, std::move(permit), std::move(trace_state), process_execute_internal);
}

static future<process_fn_return_type>
process_batch_internal(service::client_state& client_state, distributed<cql3::query_processor>& qp, request_reader in,
        uint16_t stream, cql_protocol_version_type version,
        service_permit permit, tracing::trace_state_ptr trace_state, bool init_trace, cql3::computed_function_values cached_pk_fn_calls) {
    const auto type = in.read_byte();
    const unsigned n = in.read_short();

    std::vector<cql3::statements::batch_statement::single_statement> modifications;
    std::vector<cql3::raw_value_view_vector_with_unset> values;
    std::unordered_map<cql3::prepared_cache_key_type, cql3::authorized_prepared_statements_cache::value_type> pending_authorization_entries;

    modifications.reserve(n);
    values.reserve(n);

    if (init_trace) {
        tracing::begin(trace_state, "Execute batch of CQL3 queries", client_state.get_client_address());
    }

    for ([[gnu::unused]] auto i : boost::irange(0u, n)) {
        const auto kind = in.read_byte();

        std::unique_ptr<cql3::statements::prepared_statement> stmt_ptr;
        cql3::statements::prepared_statement::checked_weak_ptr ps;
        bool needs_authorization(kind == 0);

        switch (kind) {
        case 0: {
            auto query = in.read_long_string_view();
            stmt_ptr = qp.local().get_statement(query, client_state);
            ps = stmt_ptr->checked_weak_from_this();
            if (init_trace) {
                tracing::add_query(trace_state, query);
            }
            break;
        }
        case 1: {
            cql3::prepared_cache_key_type cache_key(in.read_short_bytes());
            auto& id = cql3::prepared_cache_key_type::cql_id(cache_key);

            // First, try to lookup in the cache of already authorized statements. If the corresponding entry is not found there
            // look for the prepared statement and then authorize it.
            ps = qp.local().get_prepared(client_state.user(), cache_key);
            if (!ps) {
                ps = qp.local().get_prepared(cache_key);
                if (!ps) {
                    throw exceptions::prepared_query_not_found_exception(id);
                }
                // authorize a particular prepared statement only once
                needs_authorization = pending_authorization_entries.emplace(std::move(cache_key), ps->checked_weak_from_this()).second;
            }
            if (init_trace) {
                tracing::add_query(trace_state, ps->statement->raw_cql_statement);
            }
            break;
        }
        default:
            throw exceptions::protocol_exception(
                    "Invalid query kind in BATCH messages. Must be 0 or 1 but got "
                            + std::to_string(int(kind)));
        }

        if (dynamic_cast<cql3::statements::modification_statement*>(ps->statement.get()) == nullptr) {
            throw exceptions::invalid_request_exception("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
        }

        ::shared_ptr<cql3::statements::modification_statement> modif_statement_ptr = static_pointer_cast<cql3::statements::modification_statement>(ps->statement);
        if (init_trace) {
            tracing::add_table_name(trace_state, modif_statement_ptr->keyspace(), modif_statement_ptr->column_family());
            tracing::add_prepared_statement(trace_state, ps);
        }

        modifications.emplace_back(std::move(modif_statement_ptr), needs_authorization);

        std::vector<cql3::raw_value_view> tmp;
        cql3::unset_bind_variable_vector unset;
        in.read_value_view_list(version, tmp, unset);

        auto stmt = ps->statement;
        if (stmt->get_bound_terms() != tmp.size()) {
            throw exceptions::invalid_request_exception(format("There were {:d} markers(?) in CQL but {:d} bound variables",
                            stmt->get_bound_terms(), tmp.size()));
        }
        values.emplace_back(cql3::raw_value_view_vector_with_unset(std::move(tmp), std::move(unset)));
    }

    auto q_state = std::make_unique<cql_query_state>(client_state, trace_state, std::move(permit));
    auto& query_state = q_state->query_state;
    // #563. CQL v2 encodes query_options in v1 format for batch requests.
    q_state->options = std::make_unique<cql3::query_options>(cql3::query_options::make_batch_options(std::move(*in.read_options(version,
                                                                     qp.local().get_cql_config())), std::move(values)));
    auto& options = *q_state->options;
    if (!cached_pk_fn_calls.empty()) {
        options.set_cached_pk_function_calls(std::move(cached_pk_fn_calls));
    }

    if (init_trace) {
        tracing::set_consistency_level(trace_state, options.get_consistency());
        tracing::set_optional_serial_consistency_level(trace_state, options.get_serial_consistency());
        tracing::add_prepared_query_options(trace_state, options);
        tracing::trace(trace_state, "Creating a batch statement");
    }

    auto batch = ::make_shared<cql3::statements::batch_statement>(cql3::statements::batch_statement::type(type), std::move(modifications), cql3::attributes::none(), qp.local().get_cql_stats());
    return qp.local().execute_batch_without_checking_exception_message(batch, query_state, options, std::move(pending_authorization_entries))
            .then([stream, batch, q_state = std::move(q_state), trace_state = query_state.get_trace_state(), version] (auto msg) {
        if (msg->move_to_shard()) {
            return process_fn_return_type(dynamic_pointer_cast<messages::result_message::bounce_to_shard>(msg));
        } else if (msg->is_exception()) {
            return process_fn_return_type(convert_error_message_to_coordinator_result(msg.get()));
        } else {
            tracing::trace(q_state->query_state.get_trace_state(), "Done processing - preparing a result");
            return process_fn_return_type(make_foreign(make_result(stream, *msg, trace_state, version)));
        }
    });
}

future<cql_server::result_with_foreign_response_ptr>
cql_server::connection::process_batch(uint16_t stream, request_reader in, service::client_state& client_state, service_permit permit,
        tracing::trace_state_ptr trace_state) {
    return process(stream, in, client_state, permit, std::move(trace_state), process_batch_internal);
}

future<std::unique_ptr<cql_server::response>>
cql_server::connection::process_register(uint16_t stream, request_reader in, service::client_state& client_state,
        tracing::trace_state_ptr trace_state) {
    std::vector<sstring> event_types;
    in.read_string_list(event_types);
    for (auto&& event_type : event_types) {
        auto et = parse_event_type(event_type);
        _server._notifier->register_event(et, this);
    }
    _ready = true;
    return make_ready_future<std::unique_ptr<cql_server::response>>(make_ready(stream, std::move(trace_state)));
}

std::unique_ptr<cql_server::response> cql_server::connection::make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(required);
    response->write_int(alive);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_byte(data_present);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state) const
{
    if (_version < 4) {
        return make_read_timeout_error(stream, exceptions::exception_code::READ_TIMEOUT, std::move(msg), cl, received, blockfor, data_present, tr_state);
    }
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_int(numfailures);
    response->write_byte(data_present);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_string(format("{}", type));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state) const
{
    if (_version < 4) {
        return make_mutation_write_timeout_error(stream, exceptions::exception_code::WRITE_TIMEOUT, std::move(msg), cl, received, blockfor, type, tr_state);
    }
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_int(numfailures);
    response->write_string(format("{}", type));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_string(ks_name);
    response->write_string(cf_name);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_short_bytes(id);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_function_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring func_name, std::vector<sstring> args, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_string(ks_name);
    response->write_string(func_name);
    response->write_string_list(args);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_rate_limit_error(int16_t stream, exceptions::exception_code err, sstring msg, db::operation_type op_type, bool rejected_by_coordinator, const tracing::trace_state_ptr& tr_state, const service::client_state& client_state) const
{
    if (!client_state.is_protocol_extension_set(cql_protocol_extension::RATE_LIMIT_ERROR)) {
        return make_error(stream, exceptions::exception_code::CONFIG_ERROR, std::move(msg), tr_state);
    }

    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_byte(static_cast<uint8_t>(op_type));
    response->write_byte(static_cast<uint8_t>(rejected_by_coordinator));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state) const
{
    return std::make_unique<cql_server::response>(stream, cql_binary_opcode::READY, tr_state);
}

std::unique_ptr<cql_server::response> cql_server::connection::make_autheticate(int16_t stream, std::string_view clz, const tracing::trace_state_ptr& tr_state) const
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTHENTICATE, tr_state);
    response->write_string(clz);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_auth_success(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) const {
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTH_SUCCESS, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_auth_challenge(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) const {
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTH_CHALLENGE, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state) const
{
    std::multimap<sstring, sstring> opts;
    opts.insert({"CQL_VERSION", cql3::query_processor::CQL_VERSION});
    opts.insert({"COMPRESSION", "lz4"});
    opts.insert({"COMPRESSION", "snappy"});
    if (_server._config.allow_shard_aware_drivers) {
        opts.insert({"SCYLLA_SHARD", format("{:d}", this_shard_id())});
        opts.insert({"SCYLLA_NR_SHARDS", format("{:d}", smp::count)});
        opts.insert({"SCYLLA_SHARDING_ALGORITHM", dht::cpu_sharding_algorithm_name()});
        if (_server._config.shard_aware_transport_port) {
            opts.insert({"SCYLLA_SHARD_AWARE_PORT", format("{:d}", *_server._config.shard_aware_transport_port)});
        }
        if (_server._config.shard_aware_transport_port_ssl) {
            opts.insert({"SCYLLA_SHARD_AWARE_PORT_SSL", format("{:d}", *_server._config.shard_aware_transport_port_ssl)});
        }
        opts.insert({"SCYLLA_SHARDING_IGNORE_MSB", format("{:d}", _server._config.sharding_ignore_msb)});
        opts.insert({"SCYLLA_PARTITIONER", _server._config.partitioner_name});
    }
    for (cql_protocol_extension ext : supported_cql_protocol_extensions()) {
        const sstring ext_key_name = protocol_extension_name(ext);
        std::vector<sstring> params = additional_options_for_proto_ext(ext);
        if (params.empty()) {
            opts.emplace(ext_key_name, "");
        } else {
            for (sstring val : params) {
                opts.emplace(ext_key_name, std::move(val));
            }
        }
    }
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::SUPPORTED, tr_state);
    response->write_string_multimap(std::move(opts));
    return response;
}

class cql_server::fmt_visitor : public messages::result_message::visitor_base {
private:
    uint8_t _version;
    cql_server::response& _response;
    bool _skip_metadata;
public:
    fmt_visitor(uint8_t version, cql_server::response& response, bool skip_metadata)
        : _version{version}
        , _response{response}
        , _skip_metadata{skip_metadata}
    { }

    virtual void visit(const messages::result_message::void_message&) override {
        _response.write_int(0x0001);
    }

    virtual void visit(const messages::result_message::set_keyspace& m) override {
        _response.write_int(0x0003);
        _response.write_string(m.get_keyspace());
    }

    virtual void visit(const messages::result_message::prepared::cql& m) override {
        _response.write_int(0x0004);
        _response.write_short_bytes(m.get_id());
        _response.write(m.metadata(), _version);
        _response.write(*m.result_metadata());
    }

    virtual void visit(const messages::result_message::schema_change& m) override {
        auto change = m.get_change();
        switch (change->type) {
        case event::event_type::SCHEMA_CHANGE: {
            auto sc = static_pointer_cast<event::schema_change>(change);
            _response.write_int(0x0005);
            _response.serialize(*sc, _version);
            break;
        }
        default:
            SCYLLA_ASSERT(0);
        }
    }

    virtual void visit(const messages::result_message::rows& m) override {
        _response.write_int(0x0002);
        auto& rs = m.rs();
        _response.write(rs.get_metadata(), _skip_metadata);
        auto row_count_plhldr = _response.write_int_placeholder();

        class visitor {
            cql_server::response& _response;
            int64_t _row_count = 0;
        public:
            visitor(cql_server::response& r) : _response(r) { }

            void start_row() {
                _row_count++;
            }
            void accept_value(std::optional<managed_bytes_view> cell) {
                _response.write_value(cell);
            }
            void end_row() { }

            int64_t row_count() const { return _row_count; }
        };

        auto v = visitor(_response);
        rs.visit(v);
        row_count_plhldr.write(v.row_count()); // even though the placeholder is for int32_t we won't overflow because of memory limits
    }
};

std::unique_ptr<cql_server::response>
make_result(int16_t stream, messages::result_message& msg, const tracing::trace_state_ptr& tr_state,
        cql_protocol_version_type version, bool skip_metadata) {
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::RESULT, tr_state);
    if (__builtin_expect(!msg.warnings().empty() && version > 3, false)) {
        response->set_frame_flag(cql_frame_flags::warning);
        response->write_string_list(msg.warnings());
    }
    if (msg.custom_payload()) {
        response->set_frame_flag(cql_frame_flags::custom_payload);
        response->write_string_bytes_map(msg.custom_payload().value());
    }
    cql_server::fmt_visitor fmt{version, *response, skip_metadata};
    msg.accept(fmt);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_topology_change_event(const event::topology_change& event) const
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("TOPOLOGY_CHANGE");
    response->write_string(to_string(event.change));
    response->write_inet(event.node);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_status_change_event(const event::status_change& event) const
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("STATUS_CHANGE");
    response->write_string(to_string(event.status));
    response->write_inet(event.node);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_schema_change_event(const event::schema_change& event) const
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("SCHEMA_CHANGE");
    response->serialize(event, _version);
    return response;
}

void cql_server::connection::write_response(foreign_ptr<std::unique_ptr<cql_server::response>>&& response, service_permit permit, cql_compression compression)
{
    _ready_to_respond = _ready_to_respond.then([this, compression, response = std::move(response), permit = std::move(permit)] () mutable {
        auto message = response->make_message(_version, compression);
        message.on_delete([response = std::move(response)] { });
        return _write_buf.write(std::move(message)).then([this] {
            return _write_buf.flush();
        });
    });
}

scattered_message<char> cql_server::response::make_message(uint8_t version, cql_compression compression) {
    if (compression != cql_compression::none) {
        compress(compression);
    }
    scattered_message<char> msg;
    auto frame = make_frame(version, _body.size());
    msg.append(std::move(frame));
    for (auto&& fragment : _body.fragments()) {
        msg.append_static(reinterpret_cast<const char*>(fragment.data()), fragment.size());
    }
    return msg;
}

void cql_server::response::compress(cql_compression compression)
{
    switch (compression) {
    case cql_compression::lz4:
        compress_lz4();
        break;
    case cql_compression::snappy:
        compress_snappy();
        break;
    default:
        throw std::invalid_argument("Invalid CQL compression algorithm");
    }
    set_frame_flag(cql_frame_flags::compression);
}

void cql_server::response::compress_lz4()
{
    auto input_buffer = input_buffer_guard();
    auto output_buffer = output_buffer_guard();

    auto in = input_buffer.get_linearized_view(_body);
    size_t output_len = LZ4_COMPRESSBOUND(in.size()) + 4;
    _body = output_buffer.make_bytes_ostream(output_len, [&in] (bytes_mutable_view out) {
        out.data()[0] = (in.size() >> 24) & 0xFF;
        out.data()[1] = (in.size() >> 16) & 0xFF;
        out.data()[2] = (in.size() >> 8) & 0xFF;
        out.data()[3] = in.size() & 0xFF;
        auto ret = LZ4_compress_default(reinterpret_cast<const char*>(in.data()), reinterpret_cast<char*>(out.data() + 4), in.size(), out.size() - 4);
        if (ret == 0) {
            throw std::runtime_error("CQL frame LZ4 compression failure");
        }
        return static_cast<size_t>(ret) + 4;
    });
}

void cql_server::response::compress_snappy()
{
    auto input_buffer = input_buffer_guard();
    auto output_buffer = output_buffer_guard();

    auto in = input_buffer.get_linearized_view(_body);
    size_t output_len = snappy_max_compressed_length(in.size());
    _body = output_buffer.make_bytes_ostream(output_len, [&in] (bytes_mutable_view out) {
        size_t actual_len = out.size();
        if (snappy_compress(reinterpret_cast<const char*>(in.data()), in.size(), reinterpret_cast<char*>(out.data()), &actual_len) != SNAPPY_OK) {
            throw std::runtime_error("CQL frame Snappy compression failure");
        }
        return actual_len;
    });
}

void cql_server::response::serialize(const event::schema_change& event, uint8_t version)
{
    write_string(to_string(event.change));
    write_string(to_string(event.target));
    write_string(event.keyspace);
    switch (event.target) {
    case event::schema_change::target_type::KEYSPACE:
        break;
    case event::schema_change::target_type::TYPE:
    case event::schema_change::target_type::TABLE:
        write_string(event.arguments[0]);
        break;
    case event::schema_change::target_type::FUNCTION:
    case event::schema_change::target_type::AGGREGATE:
        write_string(event.arguments[0]);
        write_string_list(std::vector<sstring>(event.arguments.begin() + 1, event.arguments.end()));
        break;
    }
}

void cql_server::response::write_byte(uint8_t b)
{
    auto s = reinterpret_cast<const int8_t*>(&b);
    _body.write(bytes_view(s, sizeof(b)));
}

void cql_server::response::write_int(int32_t n)
{
    auto u = htonl(n);
    auto *s = reinterpret_cast<const int8_t*>(&u);
    _body.write(bytes_view(s, sizeof(u)));
}

cql_server::response::placeholder<int32_t> cql_server::response::write_int_placeholder() {
    return placeholder<int32_t>(_body.write_place_holder(sizeof(int32_t)));
}

void cql_server::response::write_long(int64_t n)
{
    auto u = htonq(n);
    auto *s = reinterpret_cast<const int8_t*>(&u);
    _body.write(bytes_view(s, sizeof(u)));
}

void cql_server::response::write_short(uint16_t n)
{
    auto u = htons(n);
    auto *s = reinterpret_cast<const int8_t*>(&u);
    _body.write(bytes_view(s, sizeof(u)));
}

template<typename T>
inline
T cast_if_fits(size_t v) {
    size_t max = std::numeric_limits<T>::max();
    if (v > max) {
        throw std::runtime_error(format("Value too large, {:d} > {:d}", v, max));
    }
    return static_cast<T>(v);
}

void cql_server::response::write_string(std::string_view s)
{
    write_short(cast_if_fits<uint16_t>(s.size()));
    _body.write(bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size()));
}

void cql_server::response::write_bytes_as_string(bytes_view s)
{
    write_short(cast_if_fits<uint16_t>(s.size()));
    _body.write(s);
}

void cql_server::response::write_long_string(const sstring& s)
{
    write_int(cast_if_fits<int32_t>(s.size()));
    _body.write(bytes_view(reinterpret_cast<const int8_t*>(s.data()), s.size()));
}

void cql_server::response::write_string_list(std::vector<sstring> string_list)
{
    write_short(cast_if_fits<uint16_t>(string_list.size()));
    for (auto&& s : string_list) {
        write_string(s);
    }
}

void cql_server::response::write_bytes(bytes b)
{
    write_int(cast_if_fits<int32_t>(b.size()));
    _body.write(b);
}

void cql_server::response::write_short_bytes(bytes b)
{
    write_short(cast_if_fits<uint16_t>(b.size()));
    _body.write(b);
}

void cql_server::response::write_inet(socket_address inet)
{
    auto addr = inet.addr();
    write_byte(uint8_t(addr.size()));
    auto * p = static_cast<const int8_t*>(addr.data());
    _body.write(bytes_view(p, addr.size()));
    write_int(inet.port());
}

void cql_server::response::write_consistency(db::consistency_level c)
{
    write_short(consistency_to_wire(c));
}

void cql_server::response::write_string_map(std::map<sstring, sstring> string_map)
{
    write_short(cast_if_fits<uint16_t>(string_map.size()));
    for (auto&& s : string_map) {
        write_string(s.first);
        write_string(s.second);
    }
}

void cql_server::response::write_string_multimap(std::multimap<sstring, sstring> string_map)
{
    std::vector<sstring> keys;
    for (auto it = string_map.begin(), end = string_map.end(); it != end; it = string_map.upper_bound(it->first)) {
        keys.push_back(it->first);
    }
    write_short(cast_if_fits<uint16_t>(keys.size()));
    for (auto&& key : keys) {
        std::vector<sstring> values;
        auto range = string_map.equal_range(key);
        for (auto it = range.first; it != range.second; ++it) {
            values.push_back(it->second);
        }
        write_string(key);
        write_string_list(values);
    }
}

void cql_server::response::write_string_bytes_map(const std::unordered_map<sstring, bytes>& map)
{
    write_short(cast_if_fits<uint16_t>(map.size()));
    for (auto&& s : map) {
        write_string(s.first);
        write_bytes(s.second);
    }
}

void cql_server::response::write_value(bytes_opt value)
{
    if (!value) {
        write_int(-1);
        return;
    }

    write_int(value->size());
    _body.write(*value);
}

void cql_server::response::write_value(std::optional<managed_bytes_view> value)
{
    if (!value) {
        write_int(-1);
        return;
    }

    write_int(value->size_bytes());
    while (!value->empty()) {
        _body.write(value->current_fragment());
        value->remove_current();
    }
}

class type_codec {
private:
    enum class type_id : int16_t {
        CUSTOM    = 0x0000,
        ASCII     = 0x0001,
        BIGINT    = 0x0002,
        BLOB      = 0x0003,
        BOOLEAN   = 0x0004,
        COUNTER   = 0x0005,
        DECIMAL   = 0x0006,
        DOUBLE    = 0x0007,
        FLOAT     = 0x0008,
        INT       = 0x0009,
        TIMESTAMP = 0x000B,
        UUID      = 0x000C,
        VARCHAR   = 0x000D,
        VARINT    = 0x000E,
        TIMEUUID  = 0x000F,
        INET      = 0x0010,
        DATE      = 0x0011,
        TIME      = 0x0012,
        SMALLINT  = 0x0013,
        TINYINT   = 0x0014,
        DURATION  = 0x0015,
        LIST      = 0x0020,
        MAP       = 0x0021,
        SET       = 0x0022,
        UDT       = 0x0030,
        TUPLE     = 0x0031,
    };

    using type_id_to_type_type = std::unordered_map<data_type, type_id>;

    static thread_local const type_id_to_type_type type_id_to_type;
public:
    static void encode(cql_server::response& r, data_type type) {
        type = type->underlying_type();

        // For compatibility sake, we still return DateType as the timestamp type in resultSet metadata (#5723)
        if (type == date_type) {
            type = timestamp_type;
        }

        auto i = type_id_to_type.find(type);
        if (i != type_id_to_type.end()) {
            r.write_short(static_cast<std::underlying_type<type_id>::type>(i->second));
            return;
        }

        if (type->is_reversed()) {
            fail(unimplemented::cause::REVERSED);
        }
        if (type->is_user_type()) {
            r.write_short(uint16_t(type_id::UDT));
            auto udt = static_pointer_cast<const user_type_impl>(type);
            r.write_string(udt->_keyspace);
            r.write_bytes_as_string(udt->_name);
            r.write_short(udt->size());
            for (auto&& i : boost::irange<size_t>(0, udt->size())) {
                r.write_bytes_as_string(udt->field_name(i));
                encode(r, udt->field_type(i));
            }
            return;
        }
        if (type->is_tuple()) {
            r.write_short(uint16_t(type_id::TUPLE));
            auto ttype = static_pointer_cast<const tuple_type_impl>(type);
            r.write_short(ttype->size());
            for (auto&& t : ttype->all_types()) {
                encode(r, t);
            }
            return;
        }
        if (type->is_collection()) {
            auto&& ctype = static_cast<const collection_type_impl*>(type.get());
            if (ctype->get_kind() == abstract_type::kind::map) {
                r.write_short(uint16_t(type_id::MAP));
                auto&& mtype = static_cast<const map_type_impl*>(ctype);
                encode(r, mtype->get_keys_type());
                encode(r, mtype->get_values_type());
            } else if (ctype->get_kind() == abstract_type::kind::set) {
                r.write_short(uint16_t(type_id::SET));
                auto&& stype = static_cast<const set_type_impl*>(ctype);
                encode(r, stype->get_elements_type());
            } else if (ctype->get_kind() == abstract_type::kind::list) {
                r.write_short(uint16_t(type_id::LIST));
                auto&& ltype = static_cast<const list_type_impl*>(ctype);
                encode(r, ltype->get_elements_type());
            } else {
                abort();
            }
            return;
        }
        abort();
    }
};

thread_local const type_codec::type_id_to_type_type type_codec::type_id_to_type {
    { ascii_type, type_id::ASCII },
    { long_type, type_id::BIGINT },
    { bytes_type, type_id::BLOB },
    { boolean_type, type_id::BOOLEAN },
    { counter_type, type_id::COUNTER },
    { decimal_type, type_id::DECIMAL },
    { double_type, type_id::DOUBLE },
    { float_type, type_id::FLOAT },
    { int32_type, type_id::INT },
    { byte_type, type_id::TINYINT },
    { duration_type, type_id::DURATION },
    { short_type, type_id::SMALLINT },
    { timestamp_type, type_id::TIMESTAMP },
    { uuid_type, type_id::UUID },
    { utf8_type, type_id::VARCHAR },
    { varint_type, type_id::VARINT },
    { timeuuid_type, type_id::TIMEUUID },
    { simple_date_type, type_id::DATE },
    { time_type, type_id::TIME },
    { inet_addr_type, type_id::INET },
};

void cql_server::response::write(const cql3::metadata& m, bool no_metadata) {
    auto flags = m.flags();
    bool global_tables_spec = m.flags().contains<cql3::metadata::flag::GLOBAL_TABLES_SPEC>();
    bool has_more_pages = m.flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>();

    if (no_metadata) {
        flags.set<cql3::metadata::flag::NO_METADATA>();
    }

    write_int(flags.mask());
    write_int(m.column_count());

    if (has_more_pages) {
        write_value(m.paging_state()->serialize());
    }

    if (no_metadata) {
        return;
    }

    auto names_i = m.get_names().begin();

    if (global_tables_spec) {
        auto first_spec = *names_i;
        write_string(first_spec->ks_name);
        write_string(first_spec->cf_name);
    }

    for (uint32_t i = 0; i < m.column_count(); ++i, ++names_i) {
        lw_shared_ptr<cql3::column_specification> name = *names_i;
        if (!global_tables_spec) {
            write_string(name->ks_name);
            write_string(name->cf_name);
        }
        write_string(name->name->text());
        type_codec::encode(*this, name->type);
    }
}

void cql_server::response::write(const cql3::prepared_metadata& m, uint8_t version)
{
    bool global_tables_spec = m.flags().contains<cql3::prepared_metadata::flag::GLOBAL_TABLES_SPEC>();

    write_int(m.flags().mask());
    write_int(m.names().size());

    if (version >= 4) {
        if (!global_tables_spec) {
            write_int(0);
        } else {
            write_int(m.partition_key_bind_indices().size());
            for (uint16_t bind_index : m.partition_key_bind_indices()) {
                write_short(bind_index);
            }
        }
    }

    if (global_tables_spec) {
        write_string(m.names()[0]->ks_name);
        write_string(m.names()[0]->cf_name);
    }

    for (auto const& name : m.names()) {
        if (!global_tables_spec) {
            write_string(name->ks_name);
            write_string(name->cf_name);
        }
        write_string(name->name->text());
        type_codec::encode(*this, name->type);
    }
}

future<utils::chunked_vector<client_data>> cql_server::get_client_data() {
    utils::chunked_vector<client_data> ret;
    co_await for_each_gently([&ret] (const generic_server::connection& c) -> future<> {
        const connection& conn = dynamic_cast<const connection&>(c);
        ret.emplace_back(conn.make_client_data());
        return make_ready_future<>();
    });
    co_return ret;
}

future<> cql_server::update_connections_service_level_params() {
    return for_each_gently([] (generic_server::connection& conn) -> future<> {
        connection& cql_conn = dynamic_cast<connection&>(conn);
        return cql_conn.get_client_state().maybe_update_per_service_level_params();
    });
}

future<std::vector<connection_service_level_params>> cql_server::get_connections_service_level_params() {
    std::vector<connection_service_level_params> sl_params;
    co_await for_each_gently([&sl_params] (const generic_server::connection& conn) -> future<> {
        auto& cql_conn = dynamic_cast<const connection&>(conn);
        auto& client_state = cql_conn.get_client_state();
        auto& user = client_state.user();
        auto role_name = user 
                ? (user->name ? *(user->name) : "ANONYMOUS") 
                : "UNAUTHENTICATED";

        sl_params.emplace_back(std::move(role_name), client_state.get_timeout_config(), client_state.get_workload_type());
        co_return;
    });
    co_return sl_params;
}

}
