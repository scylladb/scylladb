/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "server.hh"

#include <boost/bimap/unordered_set_of.hpp>
#include <boost/range/irange.hpp>
#include <boost/bimap.hpp>
#include <boost/assign.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "cql3/statements/batch_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "db/consistency_level_type.hh"
#include "db/write_type.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "utils/UUID.hh"
#include "net/byteorder.hh"
#include <seastar/core/metrics.hh>
#include <seastar/net/byteorder.hh>
#include <seastar/util/lazy.hh>
#include <seastar/core/execution_stage.hh>

#include "enum_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "exceptions/exceptions.hh"

#include "auth/authenticator.hh"

#include <cassert>
#include <string>

#include <snappy-c.h>
#include <lz4.h>

#include "response.hh"
#include "request.hh"

namespace cql_transport {

static logging::logger clogger("cql_server");

cql_server::connection::processing_result::processing_result(response_type r)
    : cql_response(make_foreign(std::move(r.first)))
    , keyspace(r.second.is_dirty() ? make_foreign(std::make_unique<sstring>(std::move(r.second.get_raw_keyspace()))) : nullptr)
    , user(r.second.user_is_dirty() ? make_foreign(r.second.user()) : nullptr)
    , auth_state(r.second.get_auth_state())
{}

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
    throw std::invalid_argument("unknown change type");
}

sstring to_string(const event::schema_change::target_type t) {
    switch (t) {
    case event::schema_change::target_type::KEYSPACE: return "KEYSPACE";
    case event::schema_change::target_type::TABLE:    return "TABLE";
    case event::schema_change::target_type::TYPE:     return "TYPE";
    }
    throw std::invalid_argument("unknown target type");
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
        throw exceptions::protocol_exception(sprint("Invalid value '%s' for Event.Type", value));
    }
}

cql_load_balance parse_load_balance(sstring value)
{
    if (value == "none") {
        return cql_load_balance::none;
    } else if (value == "round-robin") {
        return cql_load_balance::round_robin;
    } else {
        throw std::invalid_argument("Unknown load balancing algorithm: " + value);
    }
}

cql_server::cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp, cql_load_balance lb, auth::service& auth_service,
        cql_server_config config)
    : _proxy(proxy)
    , _query_processor(qp)
    , _config(config)
    , _max_request_size(config.max_request_size)
    , _memory_available(_max_request_size)
    , _notifier(std::make_unique<event_notifier>())
    , _lb(lb)
    , _auth_service(auth_service)
{
    namespace sm = seastar::metrics;

    _metrics.add_group("transport", {
        sm::make_derive("cql-connections", _connects,
                        sm::description("Counts a number of client connections.")),

        sm::make_gauge("current_connections", _connections,
                        sm::description("Holds a current number of client connections.")),

        sm::make_derive("requests_served", _requests_served,
                        sm::description("Counts a number of served requests.")),

        sm::make_gauge("requests_serving", _requests_serving,
                        sm::description("Holds a number of requests that are being processed right now.")),

        sm::make_gauge("requests_blocked_memory_current", [this] { return _memory_available.waiters(); },
                        sm::description(
                            seastar::format("Holds the number of requests that are currently blocked due to reaching the memory quota limit ({}B). "
                                            "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"CQL transport\" component.", _max_request_size))),
        sm::make_derive("requests_blocked_memory", _requests_blocked_memory,
                        sm::description(
                            seastar::format("Holds an incrementing counter with the requests that ever blocked due to reaching the memory quota limit ({}B). "
                                            "The first derivative of this value shows how often we block due to memory exhaustion in the \"CQL transport\" component.", _max_request_size))),

    });
}

future<> cql_server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    clogger.debug("cql_server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        clogger.debug("cql_server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    clogger.debug("cql_server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            clogger.debug("cql_server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        return std::move(_stopped);
    });
}

future<>
cql_server::listen(ipv4_addr addr, std::shared_ptr<seastar::tls::credentials_builder> creds, bool keepalive) {
    listen_options lo;
    lo.reuse_address = true;
    server_socket ss;
    try {
        ss = creds
          ? seastar::tls::listen(creds->build_server_credentials(), make_ipv4_address(addr), lo)
          : engine().listen(make_ipv4_address(addr), lo);
    } catch (...) {
        throw std::runtime_error(sprint("CQLServer error while listening on %s -> %s", make_ipv4_address(addr), std::current_exception()));
    }
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1, keepalive, addr)).discard_result();
    return make_ready_future<>();
}

future<>
cql_server::do_accepts(int which, bool keepalive, ipv4_addr server_addr) {
    return repeat([this, which, keepalive, server_addr] {
        ++_connections_being_accepted;
        return _listeners[which].accept().then_wrapped([this, which, keepalive, server_addr] (future<connected_socket, socket_address> f_cs_sa) mutable {
            --_connections_being_accepted;
            if (_stopping) {
                f_cs_sa.ignore_ready_future();
                maybe_idle();
                return stop_iteration::yes;
            }
            auto cs_sa = f_cs_sa.get();
            auto fd = std::get<0>(std::move(cs_sa));
            auto addr = std::get<1>(std::move(cs_sa));
            fd.set_nodelay(true);
            fd.set_keepalive(keepalive);
            auto conn = make_shared<connection>(*this, server_addr, std::move(fd), std::move(addr));
            ++_connects;
            ++_connections;
            conn->process().then_wrapped([this, conn] (future<> f) {
                --_connections;
                try {
                    f.get();
                } catch (...) {
                    clogger.debug("connection error: {}", std::current_exception());
                }
            });
            return stop_iteration::no;
        }).handle_exception([] (auto ep) {
            clogger.debug("accept failed: {}", ep);
            return stop_iteration::no;
        });
    });
}

unsigned
cql_server::connection::frame_size() const {
    if (_version < 3) {
        return 8;
    } else {
        return 9;
    }
}

cql_binary_frame_v3
cql_server::connection::parse_frame(temporary_buffer<char> buf) {
    if (buf.size() != frame_size()) {
        throw cql_frame_error();
    }
    cql_binary_frame_v3 v3;
    switch (_version) {
    case 1:
    case 2: {
        auto raw = reinterpret_cast<const cql_binary_frame_v1*>(buf.get());
        auto cooked = net::ntoh(*raw);
        v3.version = cooked.version;
        v3.flags = cooked.flags;
        v3.opcode = cooked.opcode;
        v3.stream = cooked.stream;
        v3.length = cooked.length;
        break;
    }
    case 3:
    case 4: {
        v3 = net::ntoh(*reinterpret_cast<const cql_binary_frame_v3*>(buf.get()));
        break;
    }
    default:
        throw exceptions::protocol_exception(sprint("Invalid or unsupported protocol version: %d", _version));
    }
    if (v3.version != _version) {
        throw exceptions::protocol_exception(sprint("Invalid message version. Got %d but previous messages on this connection had version %d", v3.version, _version));

    }
    return v3;
}

future<std::experimental::optional<cql_binary_frame_v3>>
cql_server::connection::read_frame() {
    using ret_type = std::experimental::optional<cql_binary_frame_v3>;
    if (!_version) {
        // We don't know the frame size before reading the first frame,
        // so read just one byte, and then read the rest of the frame.
        return _read_buf.read_exactly(1).then([this] (temporary_buffer<char> buf) {
            if (buf.empty()) {
                return make_ready_future<ret_type>();
            }
            _version = buf[0];
            init_cql_serialization_format();
            if (_version < 1 || _version > current_version) {
                auto client_version = _version;
                _version = current_version;
                throw exceptions::protocol_exception(sprint("Invalid or unsupported protocol version: %d", client_version));
            }
            return _read_buf.read_exactly(frame_size() - 1).then([this] (temporary_buffer<char> tail) {
                temporary_buffer<char> full(frame_size());
                full.get_write()[0] = _version;
                std::copy(tail.get(), tail.get() + tail.size(), full.get_write() + 1);
                return make_ready_future<ret_type>(parse_frame(std::move(full)));
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

future<cql_server::connection::processing_result>
    cql_server::connection::process_request_one(fragmented_temporary_buffer::istream fbuf, uint8_t op, uint16_t stream, service::client_state client_state, tracing_request_type tracing_request) {
    using auth_state = service::client_state::auth_state;

    auto cqlop = static_cast<cql_binary_opcode>(op);
    tracing::trace_state_props_set trace_props;

    trace_props.set_if<tracing::trace_state_props::log_slow_query>(tracing::tracing::get_local_tracing_instance().slow_query_tracing_enabled());
    trace_props.set_if<tracing::trace_state_props::full_tracing>(tracing_request != tracing_request_type::not_requested);

    if (trace_props) {
        if (cqlop == cql_binary_opcode::QUERY ||
            cqlop == cql_binary_opcode::PREPARE ||
            cqlop == cql_binary_opcode::EXECUTE ||
            cqlop == cql_binary_opcode::BATCH) {
            trace_props.set_if<tracing::trace_state_props::write_on_close>(tracing_request == tracing_request_type::write_on_close);
            client_state.create_tracing_session(tracing::trace_type::QUERY, trace_props);
        }
    }

    tracing::set_request_size(client_state.get_trace_state(), fbuf.bytes_left());

    auto linearization_buffer = std::make_unique<bytes_ostream>();
    auto linearization_buffer_ptr = linearization_buffer.get();
    return futurize_apply([this, cqlop, stream, &fbuf, client_state, linearization_buffer_ptr] () mutable {
        // When using authentication, we need to ensure we are doing proper state transitions,
        // i.e. we cannot simply accept any query/exec ops unless auth is complete
        switch (client_state.get_auth_state()) {
            case auth_state::UNINITIALIZED:
                if (cqlop != cql_binary_opcode::STARTUP && cqlop != cql_binary_opcode::OPTIONS) {
                    throw exceptions::protocol_exception(sprint("Unexpected message %d, expecting STARTUP or OPTIONS", int(cqlop)));
                }
                break;
            case auth_state::AUTHENTICATION:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                if (cqlop != cql_binary_opcode::AUTH_RESPONSE && cqlop != cql_binary_opcode::CREDENTIALS) {
                    throw exceptions::protocol_exception(sprint("Unexpected message %d, expecting %s", int(cqlop), _version == 1 ? "CREDENTIALS" : "SASL_RESPONSE"));
                }
                break;
            case auth_state::READY: default:
                if (cqlop == cql_binary_opcode::STARTUP) {
                    throw exceptions::protocol_exception("Unexpected message STARTUP, the connection is already initialized");
                }
                break;
        }

        const auto user = [&client_state]() -> stdx::optional<auth::authenticated_user> {
            const auto user = client_state.user();
            if (!user) {
                return {};
            }

            return *user;
        }();

        tracing::set_username(client_state.get_trace_state(), user);

        auto in = request_reader(std::move(fbuf), *linearization_buffer_ptr);
        switch (cqlop) {
        case cql_binary_opcode::STARTUP:       return process_startup(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::AUTH_RESPONSE: return process_auth_response(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::OPTIONS:       return process_options(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::QUERY:         return process_query(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::PREPARE:       return process_prepare(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::EXECUTE:       return process_execute(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::BATCH:         return process_batch(stream, std::move(in), std::move(client_state));
        case cql_binary_opcode::REGISTER:      return process_register(stream, std::move(in), std::move(client_state));
        default:                               throw exceptions::protocol_exception(sprint("Unknown opcode %d", int(cqlop)));
        }
    }).then_wrapped([this, cqlop, stream, client_state, linearization_buffer = std::move(linearization_buffer)] (future<response_type> f) -> processing_result {
        auto stop_trace = defer([&] {
            tracing::stop_foreground(client_state.get_trace_state());
        });
        --_server._requests_serving;
        try {
            response_type response = f.get0();
            service::client_state& resp_client_state = response.second;
            auto res_op = response.first->opcode();

            // and modify state now that we've generated a response.
            switch (client_state.get_auth_state()) {
                case auth_state::UNINITIALIZED:
                    if (cqlop == cql_binary_opcode::STARTUP) {
                        if (res_op == cql_binary_opcode::AUTHENTICATE) {
                            resp_client_state.set_auth_state(auth_state::AUTHENTICATION);
                        } else if (res_op == cql_binary_opcode::READY) {
                            resp_client_state.set_auth_state(auth_state::READY);
                        }
                    }
                    break;
                case auth_state::AUTHENTICATION:
                    // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                    assert(cqlop == cql_binary_opcode::AUTH_RESPONSE || cqlop == cql_binary_opcode::CREDENTIALS);
                    if (res_op == cql_binary_opcode::READY || res_op == cql_binary_opcode::AUTH_SUCCESS) {
                        resp_client_state.set_auth_state(auth_state::READY);
                    }
                    break;
                default:
                case auth_state::READY:
                    break;
            }
            tracing::set_response_size(client_state.get_trace_state(), response.first->size());
            return processing_result(std::move(response));
        } catch (const exceptions::unavailable_exception& ex) {
            return processing_result(std::make_pair(make_unavailable_error(stream, ex.code(), ex.what(), ex.consistency, ex.required, ex.alive, client_state.get_trace_state()), client_state));
        } catch (const exceptions::read_timeout_exception& ex) {
            return processing_result(std::make_pair(make_read_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.data_present, client_state.get_trace_state()), client_state));
        } catch (const exceptions::read_failure_exception& ex) {
            return processing_result(std::make_pair(make_read_failure_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.failures, ex.block_for, ex.data_present, client_state.get_trace_state()), client_state));
        } catch (const exceptions::mutation_write_timeout_exception& ex) {
            return processing_result(std::make_pair(make_mutation_write_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.type, client_state.get_trace_state()), client_state));
        } catch (const exceptions::mutation_write_failure_exception& ex) {
            return processing_result(std::make_pair(make_mutation_write_failure_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.failures, ex.block_for, ex.type, client_state.get_trace_state()), client_state));
        } catch (const exceptions::already_exists_exception& ex) {
            return processing_result(std::make_pair(make_already_exists_error(stream, ex.code(), ex.what(), ex.ks_name, ex.cf_name, client_state.get_trace_state()), client_state));
        } catch (const exceptions::prepared_query_not_found_exception& ex) {
            return processing_result(std::make_pair(make_unprepared_error(stream, ex.code(), ex.what(), ex.id, client_state.get_trace_state()), client_state));
        } catch (const exceptions::cassandra_exception& ex) {
            return processing_result(std::make_pair(make_error(stream, ex.code(), ex.what(), client_state.get_trace_state()), client_state));
        } catch (std::exception& ex) {
            return processing_result(std::make_pair(make_error(stream, exceptions::exception_code::SERVER_ERROR, ex.what(), client_state.get_trace_state()), client_state));
        } catch (...) {
            return processing_result(std::make_pair(make_error(stream, exceptions::exception_code::SERVER_ERROR, "unknown error", client_state.get_trace_state()), client_state));
        }
    });
}

cql_server::connection::connection(cql_server& server, ipv4_addr server_addr, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _server_addr(server_addr)
    , _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
    , _client_state(service::client_state::external_tag{}, server._auth_service, addr)
{
    ++_server._total_connections;
    ++_server._current_connections;
    _server._connections_list.push_back(*this);
}

cql_server::connection::~connection() {
    --_server._current_connections;
    _server._connections_list.erase(_server._connections_list.iterator_to(*this));
    _server.maybe_idle();
}

future<> cql_server::connection::process()
{
    return do_until([this] {
        return _read_buf.eof();
    }, [this] {
        return with_gate(_pending_requests_gate, [this] {
            return process_request();
        });
    }).then_wrapped([this] (future<> f) {
        try {
            f.get();
        } catch (const exceptions::cassandra_exception& ex) {
            write_response(make_error(0, ex.code(), ex.what(), tracing::trace_state_ptr()));
        } catch (std::exception& ex) {
            write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, ex.what(), tracing::trace_state_ptr()));
        } catch (...) {
            write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, "unknown error", tracing::trace_state_ptr()));
        }
    }).finally([this] {
        return _pending_requests_gate.close().then([this] {
            _server._notifier->unregister_connection(this);
            return _ready_to_respond.finally([this] {
                return _write_buf.close();
            });
        });
    });
}

future<> cql_server::connection::shutdown()
{
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
    }
    return make_ready_future<>();
}

thread_local cql_server::connection::execution_stage_type
        cql_server::connection::_process_request_stage{"transport", &connection::process_request_one};

void cql_server::connection::update_client_state(processing_result& response) {
    if (response.keyspace) {
        if (response.keyspace.get_owner_shard() != engine().cpu_id()) {
            _client_state.set_raw_keyspace(*response.keyspace);
        } else {
            // Avoid extra copy if we are on the same shard
            _client_state.set_raw_keyspace(std::move(*response.keyspace));
        }
    }

    if (response.user) {
        if (response.user.get_owner_shard() != engine().cpu_id()) {
            if (!_client_state.user() || *_client_state.user() != *response.user) {
                _client_state.set_login(make_shared<auth::authenticated_user>(*response.user));
            }
        } else if (!_client_state.user()) {
            // If we are on the same shard there is no need to copy unless _client_state._user == nullptr
            _client_state.set_login(response.user.release());
        }
    }

    if (_client_state.get_auth_state() != response.auth_state) {
        _client_state.set_auth_state(response.auth_state);
    }
}

future<> cql_server::connection::process_request() {
    return read_frame().then_wrapped([this] (future<std::experimental::optional<cql_binary_frame_v3>>&& v) {
        auto maybe_frame = std::get<0>(v.get());
        if (!maybe_frame) {
            // eof
            return make_ready_future<>();
        }

        auto& f = *maybe_frame;
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
            throw exceptions::invalid_request_exception(sprint(
                    "request size too large (frame size %d; estimate %d; allowed %d",
                    f.length, mem_estimate, _server._max_request_size));
        }

        auto fut = get_units(_server._memory_available, mem_estimate);
        if (_server._memory_available.waiters()) {
            ++_server._requests_blocked_memory;
        }

        return fut.then([this, length = f.length, flags = f.flags, op, stream, tracing_requested] (semaphore_units<> mem_permit) {
          return this->read_and_decompress_frame(length, flags).then([this, op, stream, tracing_requested, mem_permit = std::move(mem_permit)] (fragmented_temporary_buffer buf) mutable {

            ++_server._requests_served;
            ++_server._requests_serving;

            _pending_requests_gate.enter();
            auto leave = defer([this] { _pending_requests_gate.leave(); });
            // Replacing the immediately-invoked lambda below with just its body costs 5-10 usec extra per invocation.
            // Cause not understood.
            auto istream = buf.get_istream();
            [&] {
                auto cpu = pick_request_cpu();
                return [&] {
                    if (cpu == engine().cpu_id()) {
                        return _process_request_stage(this, istream, op, stream, service::client_state(service::client_state::request_copy_tag{}, _client_state, _client_state.get_timestamp()), tracing_requested);
                    } else {
                        // We should avoid sending non-trivial objects across shards.
                        static_assert(std::is_trivially_destructible_v<fragmented_temporary_buffer::istream>);
                        static_assert(std::is_trivially_copyable_v<fragmented_temporary_buffer::istream>);
                        return smp::submit_to(cpu, [this, istream, op, stream, client_state = _client_state, tracing_requested, ts = _client_state.get_timestamp()] () mutable {
                            return _process_request_stage(this, istream, op, stream, service::client_state(service::client_state::request_copy_tag{}, client_state, ts), tracing_requested);
                        });
                    }
                }().then_wrapped([this, buf = std::move(buf), mem_permit = std::move(mem_permit), leave = std::move(leave)] (future<processing_result> response_f) {
                  try {
                    auto response = response_f.get0();
                    update_client_state(response);
                    write_response(std::move(response.cql_response), _compression);
                  } catch (...) {
                    clogger.error("request processing failed: {}", std::current_exception());
                  }
                });
            }();

            return make_ready_future<>();
          });
        });
    });
}

static inline bytes_view to_bytes_view(temporary_buffer<char>& b)
{
    using byte = bytes_view::value_type;
    return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
}

namespace compression_buffers {

// Reusable buffers for compression and decompression. Cleared every
// clear_buffers_trigger uses.
static constexpr size_t clear_buffers_trigger = 100'000;
static thread_local size_t buffer_use_count = 0;
static thread_local utils::reusable_buffer input_buffer;
static thread_local utils::reusable_buffer output_buffer;

void on_compression_buffer_use() {
    if (++buffer_use_count == clear_buffers_trigger) {
        input_buffer.clear();
        output_buffer.clear();
        buffer_use_count = 0;
    }
}

}

future<fragmented_temporary_buffer> cql_server::connection::read_and_decompress_frame(size_t length, uint8_t flags)
{
    using namespace compression_buffers;
    if (flags & cql_frame_flags::compression) {
        if (_compression == cql_compression::lz4) {
            if (length < 4) {
                throw std::runtime_error("Truncated frame");
            }
            return _buffer_reader.read_exactly(_read_buf, length).then([this] (fragmented_temporary_buffer buf) {
                auto linearization_buffer = bytes_ostream();
                int32_t uncomp_len = request_reader(buf.get_istream(), linearization_buffer).read_int();
                if (uncomp_len < 0) {
                    throw std::runtime_error("CQL frame uncompressed length is negative: " + std::to_string(uncomp_len));
                }
                buf.remove_prefix(4);
                auto in = input_buffer.get_linearized_view(fragmented_temporary_buffer::view(buf));
              auto uncomp = output_buffer.make_fragmented_temporary_buffer(uncomp_len, fragmented_temporary_buffer::default_fragment_size, [&] (bytes_mutable_view out) {
                auto ret = LZ4_decompress_safe(reinterpret_cast<const char*>(in.data()), reinterpret_cast<char*>(out.data()),
                                               in.size(), out.size());
                if (ret < 0) {
                    throw std::runtime_error("CQL frame LZ4 uncompression failure");
                }
                return out.size();
              });
                on_compression_buffer_use();
                return uncomp;
            });
        } else if (_compression == cql_compression::snappy) {
            return _buffer_reader.read_exactly(_read_buf, length).then([this] (fragmented_temporary_buffer buf) {
                auto in = input_buffer.get_linearized_view(fragmented_temporary_buffer::view(buf));
                size_t uncomp_len;
                if (snappy_uncompressed_length(reinterpret_cast<const char*>(in.data()), in.size(), &uncomp_len) != SNAPPY_OK) {
                    throw std::runtime_error("CQL frame Snappy uncompressed size is unknown");
                }
              auto uncomp = output_buffer.make_fragmented_temporary_buffer(uncomp_len, fragmented_temporary_buffer::default_fragment_size, [&] (bytes_mutable_view out) {
                size_t output_len = out.size();
                if (snappy_uncompress(reinterpret_cast<const char*>(in.data()), in.size(), reinterpret_cast<char*>(out.data()), &output_len) != SNAPPY_OK) {
                    throw std::runtime_error("CQL frame Snappy uncompression failure");
                }
                return output_len;
              });
                on_compression_buffer_use();
                return uncomp;
            });
        } else {
            throw exceptions::protocol_exception(sprint("Unknown compression algorithm"));
        }
    }
    return _buffer_reader.read_exactly(_read_buf, length);
}

unsigned cql_server::connection::pick_request_cpu()
{
    if (_server._lb == cql_load_balance::round_robin) {
        return _request_cpu++ % smp::count;
    }
    return engine().cpu_id();
}

future<response_type> cql_server::connection::process_startup(uint16_t stream, request_reader in, service::client_state client_state)
{
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
             throw exceptions::protocol_exception(sprint("Unknown compression algorithm: %s", compression));
         }
    }
    auto& a = client_state.get_auth_service()->underlying_authenticator();
    if (a.require_authentication()) {
        return make_ready_future<response_type>(std::make_pair(make_autheticate(stream, a.qualified_java_name(), client_state.get_trace_state()), client_state));
    }
    return make_ready_future<response_type>(std::make_pair(make_ready(stream, client_state.get_trace_state()), client_state));
}

future<response_type> cql_server::connection::process_auth_response(uint16_t stream, request_reader in, service::client_state client_state)
{
    auto sasl_challenge = client_state.get_auth_service()->underlying_authenticator().new_sasl_challenge();
    auto buf = in.read_raw_bytes_view(in.bytes_left());
    auto challenge = sasl_challenge->evaluate_response(buf);
    if (sasl_challenge->is_complete()) {
        return sasl_challenge->get_authenticated_user().then([this, sasl_challenge, stream, client_state = std::move(client_state), challenge = std::move(challenge)](auth::authenticated_user user) mutable {
            client_state.set_login(::make_shared<auth::authenticated_user>(std::move(user)));
            auto f = client_state.check_user_exists();
            return f.then([this, stream, client_state = std::move(client_state), challenge = std::move(challenge)]() mutable {
                auto tr_state = client_state.get_trace_state();
                return make_ready_future<response_type>(std::make_pair(make_auth_success(stream, std::move(challenge), tr_state), std::move(client_state)));
            });
        });
    }
    auto tr_state = client_state.get_trace_state();
    return make_ready_future<response_type>(std::make_pair(make_auth_challenge(stream, std::move(challenge), tr_state), std::move(client_state)));
}

future<response_type> cql_server::connection::process_options(uint16_t stream, request_reader in, service::client_state client_state)
{
    return make_ready_future<response_type>(std::make_pair(make_supported(stream, client_state.get_trace_state()), client_state));
}

void
cql_server::connection::init_cql_serialization_format() {
    _cql_serialization_format = cql_serialization_format(_version);
}

future<response_type> cql_server::connection::process_query(uint16_t stream, request_reader in, service::client_state client_state)
{
    auto query = in.read_long_string_view();
    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    q_state->options = in.read_options(_version, _cql_serialization_format, this->timeout_config());
    auto& options = *q_state->options;
    auto skip_metadata = options.skip_metadata();

    tracing::set_page_size(query_state.get_trace_state(), options.get_page_size());
    tracing::set_consistency_level(query_state.get_trace_state(), options.get_consistency());
    tracing::set_optional_serial_consistency_level(query_state.get_trace_state(), options.get_serial_consistency());
    tracing::add_query(query_state.get_trace_state(), query);
    tracing::set_user_timestamp(query_state.get_trace_state(), options.get_specific_options().timestamp);

    tracing::begin(query_state.get_trace_state(), "Execute CQL3 query", query_state.get_client_state().get_client_address());

    return _server._query_processor.local().process(query, query_state, options).then([this, stream, &query_state, skip_metadata] (auto msg) {
         tracing::trace(query_state.get_trace_state(), "Done processing - preparing a result");
         return this->make_result(stream, msg, query_state.get_trace_state(), skip_metadata);
    }).then([&query_state, q_state = std::move(q_state), this] (std::unique_ptr<cql_server::response> response) {
        /* Keep q_state alive. */
        return make_ready_future<response_type>(std::make_pair(std::move(response), query_state.get_client_state()));
    });
}

future<response_type> cql_server::connection::process_prepare(uint16_t stream, request_reader in, service::client_state client_state_)
{
    auto query = in.read_long_string_view().to_string();

    tracing::add_query(client_state_.get_trace_state(), query);
    tracing::begin(client_state_.get_trace_state(), "Preparing CQL3 query", client_state_.get_client_address());

    auto cpu_id = engine().cpu_id();
    auto cpus = boost::irange(0u, smp::count);
    auto client_state = std::make_unique<service::client_state>(client_state_);
    const auto& cs = *client_state;
    return parallel_for_each(cpus.begin(), cpus.end(), [this, query, cpu_id, &cs] (unsigned int c) mutable {
        if (c != cpu_id) {
            return smp::submit_to(c, [this, query, &cs] () mutable {
                return _server._query_processor.local().prepare(std::move(query), cs, false).discard_result();
            });
        } else {
            return make_ready_future<>();
        }
    }).then([this, query, stream, &cs] () mutable {
        tracing::trace(cs.get_trace_state(), "Done preparing on remote shards");
        return _server._query_processor.local().prepare(std::move(query), cs, false).then([this, stream, &cs] (auto msg) {
            tracing::trace(cs.get_trace_state(), "Done preparing on a local shard - preparing a result. ID is [{}]", seastar::value_of([&msg] {
                return messages::result_message::prepared::cql::get_id(msg);
            }));
            return this->make_result(stream, msg, cs.get_trace_state());
        });
    }).then([client_state = std::move(client_state)] (std::unique_ptr<cql_server::response> response) {
        /* keep client_state alive */
        return make_ready_future<response_type>(std::make_pair(std::move(response), *client_state));
    });
}

future<response_type> cql_server::connection::process_execute(uint16_t stream, request_reader in, service::client_state client_state)
{
    cql3::prepared_cache_key_type cache_key(in.read_short_bytes());
    auto& id = cql3::prepared_cache_key_type::cql_id(cache_key);
    bool needs_authorization = false;

    // First, try to lookup in the cache of already authorized statements. If the corresponding entry is not found there
    // look for the prepared statement and then authorize it.
    auto prepared = _server._query_processor.local().get_prepared(client_state.user().get(), cache_key);
    if (!prepared) {
        needs_authorization = true;
        prepared = _server._query_processor.local().get_prepared(cache_key);
    }

    if (!prepared) {
        throw exceptions::prepared_query_not_found_exception(id);
    }

    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    if (_version == 1) {
        std::vector<cql3::raw_value_view> values;
        in.read_value_view_list(_version, values);
        auto consistency = in.read_consistency();
        q_state->options = std::make_unique<cql3::query_options>(consistency, timeout_config(), std::experimental::nullopt, values, false,
                                                                 cql3::query_options::specific_options::DEFAULT, _cql_serialization_format);
    } else {
        q_state->options = in.read_options(_version, _cql_serialization_format, this->timeout_config());
    }
    auto& options = *q_state->options;
    auto skip_metadata = options.skip_metadata();

    tracing::set_page_size(client_state.get_trace_state(), options.get_page_size());
    tracing::set_consistency_level(client_state.get_trace_state(), options.get_consistency());
    tracing::set_optional_serial_consistency_level(client_state.get_trace_state(), options.get_serial_consistency());
    tracing::add_query(client_state.get_trace_state(), prepared->raw_cql_statement);
    tracing::add_prepared_statement(client_state.get_trace_state(), prepared);

    tracing::begin(client_state.get_trace_state(), seastar::value_of([&id] { return seastar::format("Execute CQL3 prepared query [{}]", id); }),
                   client_state.get_client_address());

    auto stmt = prepared->statement;
    tracing::trace(query_state.get_trace_state(), "Checking bounds");
    if (stmt->get_bound_terms() != options.get_values_count()) {
        tracing::trace(query_state.get_trace_state(), "Invalid amount of bind variables: expected {:d} received {:d}", stmt->get_bound_terms(), options.get_values_count());
        throw exceptions::invalid_request_exception("Invalid amount of bind variables");
    }

    options.prepare(prepared->bound_names);

    tracing::trace(query_state.get_trace_state(), "Processing a statement");
    return _server._query_processor.local().process_statement_prepared(std::move(prepared), std::move(cache_key), query_state, options, needs_authorization).then([this, stream, &query_state, skip_metadata] (auto msg) {
        tracing::trace(query_state.get_trace_state(), "Done processing - preparing a result");
        return this->make_result(stream, msg, query_state.get_trace_state(), skip_metadata);
    }).then([&query_state, q_state = std::move(q_state), this] (std::unique_ptr<cql_server::response> response) {
        /* Keep q_state alive. */
        tracing::stop_foreground_prepared(query_state.get_trace_state(), q_state->options.get());
        return make_ready_future<response_type>(std::make_pair(std::move(response), query_state.get_client_state()));
    });
}

future<response_type>
cql_server::connection::process_batch(uint16_t stream, request_reader in, service::client_state client_state)
{
    if (_version == 1) {
        throw exceptions::protocol_exception("BATCH messages are not support in version 1 of the protocol");
    }

    const auto type = in.read_byte();
    const unsigned n = in.read_short();

    std::vector<cql3::statements::batch_statement::single_statement> modifications;
    std::vector<std::vector<cql3::raw_value_view>> values;
    std::unordered_map<cql3::prepared_cache_key_type, cql3::authorized_prepared_statements_cache::value_type> pending_authorization_entries;

    modifications.reserve(n);
    values.reserve(n);

    tracing::begin(client_state.get_trace_state(), "Execute batch of CQL3 queries", client_state.get_client_address());

    for ([[gnu::unused]] auto i : boost::irange(0u, n)) {
        const auto kind = in.read_byte();

        std::unique_ptr<cql3::statements::prepared_statement> stmt_ptr;
        cql3::statements::prepared_statement::checked_weak_ptr ps;
        bool needs_authorization(kind == 0);

        switch (kind) {
        case 0: {
            auto query = in.read_long_string_view();
            stmt_ptr = _server._query_processor.local().get_statement(query, client_state);
            ps = stmt_ptr->checked_weak_from_this();
            tracing::add_query(client_state.get_trace_state(), query);
            break;
        }
        case 1: {
            cql3::prepared_cache_key_type cache_key(in.read_short_bytes());
            auto& id = cql3::prepared_cache_key_type::cql_id(cache_key);

            // First, try to lookup in the cache of already authorized statements. If the corresponding entry is not found there
            // look for the prepared statement and then authorize it.
            ps = _server._query_processor.local().get_prepared(client_state.user().get(), cache_key);
            if (!ps) {
                ps = _server._query_processor.local().get_prepared(cache_key);
                if (!ps) {
                    throw exceptions::prepared_query_not_found_exception(id);
                }
                // authorize a particular prepared statement only once
                needs_authorization = pending_authorization_entries.emplace(std::move(cache_key), ps->checked_weak_from_this()).second;
            }

            tracing::add_query(client_state.get_trace_state(), ps->raw_cql_statement);
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
        tracing::add_table_name(client_state.get_trace_state(), modif_statement_ptr->keyspace(), modif_statement_ptr->column_family());
        tracing::add_prepared_statement(client_state.get_trace_state(), ps);

        modifications.emplace_back(std::move(modif_statement_ptr), needs_authorization);

        std::vector<cql3::raw_value_view> tmp;
        in.read_value_view_list(_version, tmp);

        auto stmt = ps->statement;
        if (stmt->get_bound_terms() != tmp.size()) {
            throw exceptions::invalid_request_exception(sprint("There were %d markers(?) in CQL but %d bound variables",
                            stmt->get_bound_terms(), tmp.size()));
        }
        values.emplace_back(std::move(tmp));
    }

    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    // #563. CQL v2 encodes query_options in v1 format for batch requests.
    q_state->options = std::make_unique<cql3::query_options>(cql3::query_options::make_batch_options(std::move(*in.read_options(_version < 3 ? 1 : _version, _cql_serialization_format, this->timeout_config())), std::move(values)));
    auto& options = *q_state->options;

    tracing::set_consistency_level(client_state.get_trace_state(), options.get_consistency());
    tracing::set_optional_serial_consistency_level(client_state.get_trace_state(), options.get_serial_consistency());
    tracing::trace(client_state.get_trace_state(), "Creating a batch statement");

    auto batch = ::make_shared<cql3::statements::batch_statement>(cql3::statements::batch_statement::type(type), std::move(modifications), cql3::attributes::none(), _server._query_processor.local().get_cql_stats());
    return _server._query_processor.local().process_batch(batch, query_state, options, std::move(pending_authorization_entries)).then([this, stream, batch, &query_state] (auto msg) {
        return this->make_result(stream, msg, query_state.get_trace_state());
    }).then([&query_state, q_state = std::move(q_state), this] (std::unique_ptr<cql_server::response> response) {
        /* Keep q_state alive. */
        tracing::stop_foreground_prepared(query_state.get_trace_state(), q_state->options.get());
        return make_ready_future<response_type>(std::make_pair(std::move(response), query_state.get_client_state()));
    });
}

future<response_type>
cql_server::connection::process_register(uint16_t stream, request_reader in, service::client_state client_state)
{
    std::vector<sstring> event_types;
    in.read_string_list(event_types);
    for (auto&& event_type : event_types) {
        auto et = parse_event_type(event_type);
        _server._notifier->register_event(et, this);
    }
    return make_ready_future<response_type>(std::make_pair(make_ready(stream, client_state.get_trace_state()), client_state));
}

std::unique_ptr<cql_server::response> cql_server::connection::make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(required);
    response->write_int(alive);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state)
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

std::unique_ptr<cql_server::response> cql_server::connection::make_read_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, bool data_present, const tracing::trace_state_ptr& tr_state)
{
    if (_version < 4) {
        return make_read_timeout_error(stream, err, std::move(msg), cl, received, blockfor, data_present, tr_state);
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

std::unique_ptr<cql_server::response> cql_server::connection::make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_string(sprint("%s", type));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_mutation_write_failure_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t numfailures, int32_t blockfor, db::write_type type, const tracing::trace_state_ptr& tr_state)
{
    if (_version < 4) {
        return make_mutation_write_timeout_error(stream, err, std::move(msg), cl, received, blockfor, type, tr_state);
    }
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_int(numfailures);
    response->write_string(sprint("%s", type));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_string(ks_name);
    response->write_string(cf_name);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_short_bytes(id);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_error(int16_t stream, exceptions::exception_code err, sstring msg, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::ERROR, tr_state);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_ready(int16_t stream, const tracing::trace_state_ptr& tr_state)
{
    return std::make_unique<cql_server::response>(stream, cql_binary_opcode::READY, tr_state);
}

std::unique_ptr<cql_server::response> cql_server::connection::make_autheticate(int16_t stream, const sstring& clz, const tracing::trace_state_ptr& tr_state)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTHENTICATE, tr_state);
    response->write_string(clz);
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_auth_success(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) {
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTH_SUCCESS, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_auth_challenge(int16_t stream, bytes b, const tracing::trace_state_ptr& tr_state) {
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::AUTH_CHALLENGE, tr_state);
    response->write_bytes(std::move(b));
    return response;
}

std::unique_ptr<cql_server::response> cql_server::connection::make_supported(int16_t stream, const tracing::trace_state_ptr& tr_state)
{
    std::multimap<sstring, sstring> opts;
    opts.insert({"CQL_VERSION", cql3::query_processor::CQL_VERSION});
    opts.insert({"COMPRESSION", "lz4"});
    opts.insert({"COMPRESSION", "snappy"});
    auto& part = dht::global_partitioner();
    opts.insert({"SCYLLA_SHARD", sprint("%d", engine().cpu_id())});
    opts.insert({"SCYLLA_NR_SHARDS", sprint("%d", smp::count)});
    opts.insert({"SCYLLA_SHARDING_ALGORITHM", part.cpu_sharding_algorithm_name()});
    opts.insert({"SCYLLA_SHARDING_IGNORE_MSB", sprint("%d", part.sharding_ignore_msb())});
    opts.insert({"SCYLLA_PARTITIONER", part.name()});
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::SUPPORTED, tr_state);
    response->write_string_multimap(opts);
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
        _response.write(*m.metadata(), _version);
        if (_version > 1) {
            _response.write(*m.result_metadata());
        }
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
            assert(0);
        }
    }

    virtual void visit(const messages::result_message::rows& m) override {
        _response.write_int(0x0002);
        auto& rs = m.rs();
        _response.write(rs.get_metadata(), _skip_metadata);
        auto row_count_plhldr = _response.write_int_placeholder();

        class visitor {
            cql_server::response& _response;
            int32_t _row_count = 0;
        public:
            visitor(cql_server::response& r) : _response(r) { }

            void start_row() {
                _row_count++;
            }
            void accept_value(std::optional<query::result_bytes_view> cell) {
                _response.write_value(cell);
            }
            void end_row() { }

            int32_t row_count() const { return _row_count; }
        };

        auto v = visitor(_response);
        rs.visit(v);
        row_count_plhldr.write(v.row_count());
    }
};

std::unique_ptr<cql_server::response>
cql_server::connection::make_result(int16_t stream, shared_ptr<messages::result_message> msg, const tracing::trace_state_ptr& tr_state, bool skip_metadata)
{
    auto response = std::make_unique<cql_server::response>(stream, cql_binary_opcode::RESULT, tr_state);
    fmt_visitor fmt{_version, *response, skip_metadata};
    msg->accept(fmt);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_topology_change_event(const event::topology_change& event)
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("TOPOLOGY_CHANGE");
    response->write_string(to_string(event.change));
    response->write_inet(event.node);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_status_change_event(const event::status_change& event)
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("STATUS_CHANGE");
    response->write_string(to_string(event.status));
    response->write_inet(event.node);
    return response;
}

std::unique_ptr<cql_server::response>
cql_server::connection::make_schema_change_event(const event::schema_change& event)
{
    auto response = std::make_unique<cql_server::response>(-1, cql_binary_opcode::EVENT, tracing::trace_state_ptr());
    response->write_string("SCHEMA_CHANGE");
    response->serialize(event, _version);
    return response;
}

void cql_server::connection::write_response(foreign_ptr<std::unique_ptr<cql_server::response>>&& response, cql_compression compression)
{
    _ready_to_respond = _ready_to_respond.then([this, compression, response = std::move(response)] () mutable {
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
    using namespace compression_buffers;
    auto view = input_buffer.get_linearized_view(_body);
    const char* input = reinterpret_cast<const char*>(view.data());
    size_t input_len = view.size();

    size_t output_len = LZ4_COMPRESSBOUND(input_len) + 4;
  _body = output_buffer.make_buffer(output_len, [&] (bytes_mutable_view output_view) {
    char* output = reinterpret_cast<char*>(output_view.data());
    output[0] = (input_len >> 24) & 0xFF;
    output[1] = (input_len >> 16) & 0xFF;
    output[2] = (input_len >> 8) & 0xFF;
    output[3] = input_len & 0xFF;
#ifdef SEASTAR_HAVE_LZ4_COMPRESS_DEFAULT
    auto ret = LZ4_compress_default(input, output + 4, input_len, LZ4_compressBound(input_len));
#else
    auto ret = LZ4_compress(input, output + 4, input_len);
#endif
    if (ret == 0) {
        throw std::runtime_error("CQL frame LZ4 compression failure");
    }
    return ret + 4;
  });
    on_compression_buffer_use();
}

void cql_server::response::compress_snappy()
{
    using namespace compression_buffers;
    auto view = input_buffer.get_linearized_view(_body);
    const char* input = reinterpret_cast<const char*>(view.data());
    size_t input_len = view.size();

    size_t output_len = snappy_max_compressed_length(input_len);
  _body = output_buffer.make_buffer(output_len, [&] (bytes_mutable_view output_view) {
    char* output = reinterpret_cast<char*>(output_view.data());
    if (snappy_compress(input, input_len, output, &output_len) != SNAPPY_OK) {
        throw std::runtime_error("CQL frame Snappy compression failure");
    }
    return output_len;
  });
    on_compression_buffer_use();
}

void cql_server::response::serialize(const event::schema_change& event, uint8_t version)
{
    if (version >= 3) {
        write_string(to_string(event.change));
        write_string(to_string(event.target));
        write_string(event.keyspace);
        if (event.target != event::schema_change::target_type::KEYSPACE) {
            write_string(*(event.table_or_type_or_function));
        }
    } else {
        if (event.target == event::schema_change::target_type::TYPE) {
	    // The v1/v2 protocol is unable to represent type changes. Tell the
	    // client that the keyspace was updated instead.
            write_string(to_string(event::schema_change::change_type::UPDATED));
            write_string(event.keyspace);
            write_string("");
        } else {
            write_string(to_string(event.change));
            write_string(event.keyspace);
            if (event.target != event::schema_change::target_type::KEYSPACE) {
                write_string(*(event.table_or_type_or_function));
            } else {
                write_string("");
            }
        }
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
        throw std::runtime_error(sprint("Value too large, %d > %d", v, max));
    }
    return static_cast<T>(v);
}

void cql_server::response::write_string(const sstring& s)
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

void cql_server::response::write_inet(ipv4_addr inet)
{
    write_byte(4);
    write_byte(((inet.ip & 0xff000000) >> 24));
    write_byte(((inet.ip & 0x00ff0000) >> 16));
    write_byte(((inet.ip & 0x0000ff00) >> 8 ));
    write_byte(((inet.ip & 0x000000ff)      ));
    write_int(inet.port);
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

void cql_server::response::write_value(bytes_opt value)
{
    if (!value) {
        write_int(-1);
        return;
    }

    write_int(value->size());
    _body.write(*value);
}

void cql_server::response::write_value(std::optional<query::result_bytes_view> value)
{
    if (!value) {
        write_int(-1);
        return;
    }

    write_int(value->size_bytes());
    using boost::range::for_each;
    for_each(*value, [&] (bytes_view fragment) {
        _body.write(fragment);
    });
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

    using type_id_to_type_type = boost::bimap<
        boost::bimaps::unordered_set_of<type_id>,
        boost::bimaps::unordered_set_of<data_type>>;

    static thread_local const type_id_to_type_type type_id_to_type;
public:
    static void encode(cql_server::response& r, data_type type) {
        type = type->underlying_type();

        // For compatibility sake, we still return DateType as the timestamp type in resultSet metadata (#5723)
        if (type == date_type) {
            type = timestamp_type;
        }

        auto i = type_id_to_type.right.find(type);
        if (i != type_id_to_type.right.end()) {
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
            if (&ctype->_kind == &collection_type_impl::kind::map) {
                r.write_short(uint16_t(type_id::MAP));
                auto&& mtype = static_cast<const map_type_impl*>(ctype);
                encode(r, mtype->get_keys_type());
                encode(r, mtype->get_values_type());
            } else if (&ctype->_kind == &collection_type_impl::kind::set) {
                r.write_short(uint16_t(type_id::SET));
                auto&& stype = static_cast<const set_type_impl*>(ctype);
                encode(r, stype->get_elements_type());
            } else if (&ctype->_kind == &collection_type_impl::kind::list) {
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

thread_local const type_codec::type_id_to_type_type type_codec::type_id_to_type = boost::assign::list_of<type_id_to_type_type::relation>
    (type_id::ASCII     , ascii_type)
    (type_id::BIGINT    , long_type)
    (type_id::BLOB      , bytes_type)
    (type_id::BOOLEAN   , boolean_type)
    (type_id::COUNTER   , counter_type)
    (type_id::DECIMAL   , decimal_type)
    (type_id::DOUBLE    , double_type)
    (type_id::FLOAT     , float_type)
    (type_id::INT       , int32_type)
    (type_id::TINYINT   , byte_type)
    (type_id::DURATION  , duration_type)
    (type_id::SMALLINT  , short_type)
    (type_id::TIMESTAMP , timestamp_type)
    (type_id::UUID      , uuid_type)
    (type_id::VARCHAR   , utf8_type)
    (type_id::VARINT    , varint_type)
    (type_id::TIMEUUID  , timeuuid_type)
    (type_id::DATE      , simple_date_type)
    (type_id::TIME      , time_type)
    (type_id::INET      , inet_addr_type);

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
        ::shared_ptr<cql3::column_specification> name = *names_i;
        if (!global_tables_spec) {
            write_string(name->ks_name);
            write_string(name->cf_name);
        }
        write_string(name->name->text());
        type_codec::encode(*this, name->type);
    };
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

}
