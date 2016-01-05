/*
 * Copyright 2015 Cloudius Systems
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
#include <boost/locale/encoding_utf.hpp>
#include <boost/range/adaptor/sliced.hpp>

#include "cql3/statements/batch_statement.hh"
#include "service/migration_manager.hh"
#include "service/storage_service.hh"
#include "db/consistency_level.hh"
#include "db/write_type.hh"
#include "core/future-util.hh"
#include "core/reactor.hh"
#include "utils/UUID.hh"
#include "database.hh"
#include "net/byteorder.hh"
#include <seastar/core/scollectd.hh>

#include "enum_set.hh"
#include "service/query_state.hh"
#include "service/client_state.hh"
#include "exceptions/exceptions.hh"

#include "auth/authenticator.hh"

#include <cassert>
#include <string>

namespace transport {

static logging::logger logger("cql_server");

struct cql_frame_error : std::exception {
    const char* what() const throw () override {
        return "bad cql binary frame";
    }
};

enum class cql_binary_opcode : uint8_t {
    ERROR          = 0,
    STARTUP        = 1,
    READY          = 2,
    AUTHENTICATE   = 3,
    CREDENTIALS    = 4,
    OPTIONS        = 5,
    SUPPORTED      = 6,
    QUERY          = 7,
    RESULT         = 8,
    PREPARE        = 9,
    EXECUTE        = 10,
    REGISTER       = 11,
    EVENT          = 12,
    BATCH          = 13,
    AUTH_CHALLENGE = 14,
    AUTH_RESPONSE  = 15,
    AUTH_SUCCESS   = 16,
};

inline db::consistency_level wire_to_consistency(int16_t v)
{
     switch (v) {
     case 0x0000: return db::consistency_level::ANY;
     case 0x0001: return db::consistency_level::ONE;
     case 0x0002: return db::consistency_level::TWO;
     case 0x0003: return db::consistency_level::THREE;
     case 0x0004: return db::consistency_level::QUORUM;
     case 0x0005: return db::consistency_level::ALL;
     case 0x0006: return db::consistency_level::LOCAL_QUORUM;
     case 0x0007: return db::consistency_level::EACH_QUORUM;
     case 0x0008: return db::consistency_level::SERIAL;
     case 0x0009: return db::consistency_level::LOCAL_SERIAL;
     case 0x000A: return db::consistency_level::LOCAL_ONE;
     default:     throw exceptions::protocol_exception(sprint("Unknown code %d for a consistency level", v));
     }
}

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

class cql_server::response {
    int16_t           _stream;
    cql_binary_opcode _opcode;
    std::vector<char> _body;
public:
    response(int16_t stream, cql_binary_opcode opcode)
        : _stream{stream}
        , _opcode{opcode}
    { }
    scattered_message<char> make_message(uint8_t version);
    void serialize(const event::schema_change& event, uint8_t version);
    void write_byte(uint8_t b);
    void write_int(int32_t n);
    void write_long(int64_t n);
    void write_short(int16_t n);
    void write_string(const sstring& s);
    void write_long_string(const sstring& s);
    void write_uuid(utils::UUID uuid);
    void write_string_list(std::vector<sstring> string_list);
    void write_bytes(bytes b);
    void write_short_bytes(bytes b);
    void write_option(std::pair<int16_t, data_value> opt);
    void write_option_list(std::vector<std::pair<int16_t, data_value>> opt_list);
    void write_inet(ipv4_addr inet);
    void write_consistency(db::consistency_level c);
    void write_string_map(std::map<sstring, sstring> string_map);
    void write_string_multimap(std::multimap<sstring, sstring> string_map);
    void write_value(bytes_opt value);
    void write(const cql3::metadata& m);
    future<> output(output_stream<char>& out, uint8_t version);

    cql_binary_opcode opcode() const {
        return _opcode;
    }
private:
    sstring make_frame(uint8_t version, size_t length);
};

cql_server::cql_server(distributed<service::storage_proxy>& proxy, distributed<cql3::query_processor>& qp, cql_load_balance lb)
    : _proxy(proxy)
    , _query_processor(qp)
    , _max_request_size(memory::stats().total_memory() / 10)
    , _memory_available(_max_request_size)
    , _collectd_registrations(std::make_unique<scollectd::registrations>(setup_collectd()))
    , _lb(lb)
{
}

scollectd::registrations
cql_server::setup_collectd() {
    return {
        scollectd::add_polled_metric(
            scollectd::type_instance_id("transport", scollectd::per_cpu_plugin_instance,
                    "connections", "cql-connections"),
            scollectd::make_typed(scollectd::data_type::DERIVE, _connects)),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("transport", scollectd::per_cpu_plugin_instance,
                    "current_connections", "current"),
            scollectd::make_typed(scollectd::data_type::GAUGE, _connections)),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("transport", scollectd::per_cpu_plugin_instance,
                    "total_requests", "requests_served"),
            scollectd::make_typed(scollectd::data_type::DERIVE, _requests_served)),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("transport", scollectd::per_cpu_plugin_instance,
                    "queue_length", "requests_serving"),
            scollectd::make_typed(scollectd::data_type::GAUGE, _requests_serving)),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("transport", scollectd::per_cpu_plugin_instance,
                    "queue_length", "requests_blocked_memory"),
            scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return _memory_available.waiters(); })),
    };
}

future<> cql_server::stop() {
    _stopping = true;
    size_t nr = 0;
    size_t nr_total = _listeners.size();
    logger.debug("cql_server: abort accept nr_total={}", nr_total);
    for (auto&& l : _listeners) {
        l.abort_accept();
        logger.debug("cql_server: abort accept {} out of {} done", ++nr, nr_total);
    }
    auto nr_conn = make_lw_shared<size_t>(0);
    auto nr_conn_total = _connections_list.size();
    logger.debug("cql_server: shutdown connection nr_total={}", nr_conn_total);
    return parallel_for_each(_connections_list.begin(), _connections_list.end(), [nr_conn, nr_conn_total] (auto&& c) {
        return c.shutdown().then([nr_conn, nr_conn_total] {
            logger.debug("cql_server: shutdown connection {} out of {} done", ++(*nr_conn), nr_conn_total);
        });
    }).then([this] {
        service::get_local_storage_service().unregister_subscriber(_notifier.get());
        service::get_local_migration_manager().unregister_listener(_notifier.get());
        return std::move(_stopped);
    });
}

future<>
cql_server::listen(ipv4_addr addr, ::shared_ptr<seastar::tls::server_credentials> creds) {
    _notifier = std::make_unique<event_notifier>(addr.port);
    service::get_local_migration_manager().register_listener(_notifier.get());
    service::get_local_storage_service().register_subscriber(_notifier.get());

    listen_options lo;
    lo.reuse_address = true;
    server_socket ss = creds
                    ? seastar::tls::listen(creds, make_ipv4_address(addr), lo)
                    : engine().listen(make_ipv4_address(addr), lo);
    _listeners.emplace_back(std::move(ss));
    _stopped = when_all(std::move(_stopped), do_accepts(_listeners.size() - 1)).discard_result();
    return make_ready_future<>();
}

future<>
cql_server::do_accepts(int which) {
    ++_connections_being_accepted;
    return _listeners[which].accept().then_wrapped([this, which] (future<connected_socket, socket_address> f_cs_sa) mutable {
        --_connections_being_accepted;
        if (_stopping) {
            f_cs_sa.ignore_ready_future();
            maybe_idle();
            return;
        }
        auto cs_sa = f_cs_sa.get();
        auto fd = std::get<0>(std::move(cs_sa));
        auto addr = std::get<1>(std::move(cs_sa));
        fd.set_nodelay(true);
        auto conn = make_shared<connection>(*this, std::move(fd), std::move(addr));
        ++_connects;
        ++_connections;
        conn->process().then_wrapped([this, conn] (future<> f) {
            --_connections;
            try {
                f.get();
            } catch (...) {
                logger.debug("connection error: {}", std::current_exception());
            }
        });
        do_accepts(which);
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (...) {
            logger.debug("accept failed: {}", std::current_exception());
        }
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
            init_serialization_format();
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

future<response_type>
cql_server::connection::process_request_one(bytes_view buf, uint8_t op, uint16_t stream, service::client_state client_state) {
    auto cqlop = static_cast<cql_binary_opcode>(op);
    return make_ready_future<>().then([this, cqlop, stream, buf = std::move(buf), client_state] () mutable {
        // When using authentication, we need to ensure we are doing proper state transitions,
        // i.e. we cannot simply accept any query/exec ops unless auth is complete
        switch (_state) {
            case state::UNINITIALIZED:
                if (cqlop != cql_binary_opcode::STARTUP && cqlop != cql_binary_opcode::OPTIONS) {
                    throw exceptions::protocol_exception(sprint("Unexpected message %d, expecting STARTUP or OPTIONS", int(cqlop)));
                }
                break;
            case state::AUTHENTICATION:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                if (cqlop != cql_binary_opcode::AUTH_RESPONSE && cqlop != cql_binary_opcode::CREDENTIALS) {
                    throw exceptions::protocol_exception(sprint("Unexpected message %d, expecting %s", int(cqlop), _version == 1 ? "CREDENTIALS" : "SASL_RESPONSE"));
                }
                break;
            case state::READY: default:
                if (cqlop == cql_binary_opcode::STARTUP) {
                    throw exceptions::protocol_exception("Unexpected message STARTUP, the connection is already initialized");
                }
                break;
        }
        switch (cqlop) {
        case cql_binary_opcode::STARTUP:       return process_startup(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::AUTH_RESPONSE: return process_auth_response(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::OPTIONS:       return process_options(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::QUERY:         return process_query(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::PREPARE:       return process_prepare(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::EXECUTE:       return process_execute(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::BATCH:         return process_batch(stream, std::move(buf), std::move(client_state));
        case cql_binary_opcode::REGISTER:      return process_register(stream, std::move(buf), std::move(client_state));
        default:                               throw exceptions::protocol_exception(sprint("Unknown opcode %d", int(cqlop)));
        }
    }).then_wrapped([this, cqlop, stream, client_state] (future<response_type> f) {
        --_server._requests_serving;
        try {
            response_type response = f.get0();
            auto res_op = response.first->opcode();
            // and modify state now that we've generated a response.
            switch (_state) {
                case state::UNINITIALIZED:
                    if (cqlop == cql_binary_opcode::STARTUP) {
                        if (res_op == cql_binary_opcode::AUTHENTICATE) {
                            _state = state::AUTHENTICATION;
                        } else if (res_op == cql_binary_opcode::READY) {
                            _state = state::READY;
                        }
                    }
                    break;
                case state::AUTHENTICATION:
                    // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                    assert(cqlop == cql_binary_opcode::AUTH_RESPONSE || cqlop == cql_binary_opcode::CREDENTIALS);
                    if (res_op == cql_binary_opcode::READY || res_op == cql_binary_opcode::AUTH_SUCCESS) {
                        _state = state::READY;
                        // we won't use the authenticator again, null it
                        _sasl_challenge = nullptr;
                    }
                    break;
                default:
                case state::READY:
                    break;
            }
            return make_ready_future<response_type>(response);
        } catch (const exceptions::unavailable_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_unavailable_error(stream, ex.code(), ex.what(), ex.consistency, ex.required, ex.alive), client_state));
        } catch (const exceptions::read_timeout_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_read_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.data_present), client_state));
        } catch (const exceptions::mutation_write_timeout_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_mutation_write_timeout_error(stream, ex.code(), ex.what(), ex.consistency, ex.received, ex.block_for, ex.type), client_state));
        } catch (const exceptions::already_exists_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_already_exists_error(stream, ex.code(), ex.what(), ex.ks_name, ex.cf_name), client_state));
        } catch (const exceptions::prepared_query_not_found_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_unprepared_error(stream, ex.code(), ex.what(), ex.id), client_state));
        } catch (const exceptions::cassandra_exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_error(stream, ex.code(), ex.what()), client_state));
        } catch (std::exception& ex) {
            return make_ready_future<response_type>(std::make_pair(make_error(stream, exceptions::exception_code::SERVER_ERROR, ex.what()), client_state));
        } catch (...) {
            return make_ready_future<response_type>(std::make_pair(make_error(stream, exceptions::exception_code::SERVER_ERROR, "unknown error"), client_state));
        }
    });
}

cql_server::connection::connection(cql_server& server, connected_socket&& fd, socket_address addr)
    : _server(server)
    , _fd(std::move(fd))
    , _read_buf(_fd.input())
    , _write_buf(_fd.output())
    , _client_state(service::client_state::for_external_calls()) {
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
            return make_ready_future<>();
        } catch (const exceptions::cassandra_exception& ex) {
            return write_response(make_error(0, ex.code(), ex.what()));
        } catch (std::exception& ex) {
            return write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, ex.what()));
        } catch (...) {
            return write_response(make_error(0, exceptions::exception_code::SERVER_ERROR, "unknown error"));
        }
    }).finally([this] {
        _server._notifier->unregister_connection(this);
        return _pending_requests_gate.close().then([this] {
            return _ready_to_respond.finally([this] {
                return _write_buf.close();
            });
        });
    });
}

future<> cql_server::connection::shutdown()
{
    _fd.shutdown_input();
    return _pending_requests_gate.close().then([this] {
        _fd.shutdown_output();
    });
}

future<> cql_server::connection::process_request() {
    return read_frame().then_wrapped([this] (future<std::experimental::optional<cql_binary_frame_v3>>&& v) {
        auto maybe_frame = std::get<0>(v.get());
        if (!maybe_frame) {
            // eof
            return make_ready_future<>();
        }

        auto& f = *maybe_frame;

        // FIXME: compression
        if (f.flags & 0x01) {
            throw std::runtime_error("CQL frame compression is not supported");
        }

        auto op = f.opcode;
        auto stream = f.stream;
        auto mem_estimate = f.length * 2 + 8000; // Allow for extra copies and bookkeeping

        if (mem_estimate > _server._max_request_size) {
            throw exceptions::invalid_request_exception(sprint(
                    "request size too large (frame size %d; estimate %d; allowed %d",
                    f.length, mem_estimate, _server._max_request_size));
        }

        return with_semaphore(_server._memory_available, mem_estimate, [this, length = f.length, op, stream] {
          return _read_buf.read_exactly(length).then([this, op, stream] (temporary_buffer<char> buf) {

            ++_server._requests_served;
            ++_server._requests_serving;

            with_gate(_pending_requests_gate, [this, op, stream, buf = std::move(buf)] () mutable {
                auto bv = bytes_view{reinterpret_cast<const int8_t*>(buf.begin()), buf.size()};
                auto cpu = pick_request_cpu();
                return smp::submit_to(cpu, [this, bv = std::move(bv), op, stream, client_state = _client_state] () mutable {
                    return this->process_request_one(bv, op, stream, std::move(client_state)).then([] (auto&& response) {
                        return std::make_pair(make_foreign(response.first), response.second);
                    });
                }).then([this] (auto&& response) {
                    _client_state.merge(response.second);
                    return this->write_response(std::move(response.first));
                }).then([buf = std::move(buf)] {
                    // Keep buf alive.
                });
            }).handle_exception([] (std::exception_ptr ex) {
                logger.error("request processing failed: {}", ex);
            });

            return make_ready_future<>();
          });
        });
    });
}

unsigned cql_server::connection::pick_request_cpu()
{
    if (_server._lb == cql_load_balance::round_robin) {
        return _request_cpu++ % smp::count;
    }
    return engine().cpu_id();
}

future<response_type> cql_server::connection::process_startup(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    /*auto string_map =*/ read_string_map(buf);
    auto& a = auth::authenticator::get();
    if (a.require_authentication()) {
        return make_ready_future<response_type>(std::make_pair(make_autheticate(stream, a.class_name()), client_state));
    }
    return make_ready_future<response_type>(std::make_pair(make_ready(stream), client_state));
}

future<response_type> cql_server::connection::process_auth_response(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    if (_sasl_challenge == nullptr) {
        _sasl_challenge = auth::authenticator::get().new_sasl_challenge();
    }

    auto challenge = _sasl_challenge->evaluate_response(buf);
    if (_sasl_challenge->is_complete()) {
        return _sasl_challenge->get_authenticated_user().then([this, stream, client_state = std::move(client_state), challenge = std::move(challenge)](::shared_ptr<auth::authenticated_user> user) mutable {
            return client_state.login(std::move(user)).then([this, stream, client_state = std::move(client_state), challenge = std::move(challenge)]() mutable {
                return make_ready_future<response_type>(std::make_pair(make_auth_success(stream, std::move(challenge)), std::move(client_state)));
            });
        });
    }
    return make_ready_future<response_type>(std::make_pair(make_auth_challenge(stream, std::move(challenge)), std::move(client_state)));
}

future<response_type> cql_server::connection::process_options(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    return make_ready_future<response_type>(std::make_pair(make_supported(stream), client_state));
}

void
cql_server::connection::init_serialization_format() {
    if (_version < 3) {
        _serialization_format = serialization_format::use_16_bit();
    } else {
        _serialization_format = serialization_format::use_32_bit();
    }
}

future<response_type> cql_server::connection::process_query(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    auto query = read_long_string_view(buf);
    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    q_state->options = read_options(buf);
    auto& options = *q_state->options;
    return _server._query_processor.local().process(query, query_state, options).then([this, stream, buf = std::move(buf), &query_state] (auto msg) {
         return this->make_result(stream, msg);
    }).then([&query_state, q_state = std::move(q_state), this] (auto&& response) {
        /* Keep q_state alive. */
        return make_ready_future<response_type>(std::make_pair(response, query_state.get_client_state()));
    });
}

future<response_type> cql_server::connection::process_prepare(uint16_t stream, bytes_view buf, service::client_state client_state_)
{
    auto query = read_long_string_view(buf).to_string();
    auto cpu_id = engine().cpu_id();
    auto cpus = boost::irange(0u, smp::count);
    auto client_state = std::make_unique<service::client_state>(client_state_);
    const auto& cs = *client_state;
    return parallel_for_each(cpus.begin(), cpus.end(), [this, query, cpu_id, &cs] (unsigned int c) mutable {
        if (c != cpu_id) {
            return smp::submit_to(c, [this, query, &cs] () mutable {
                _server._query_processor.local().prepare(query, cs, false);
                // FIXME: error handling
            });
        } else {
            return make_ready_future<>();
        }
    }).then([this, query, stream, &cs] {
        return _server._query_processor.local().prepare(query, cs, false).then([this, stream] (auto msg) {
            return this->make_result(stream, msg);
        });
    }).then([client_state = std::move(client_state)] (auto&& response) {
        /* keep client_state alive */
        return make_ready_future<response_type>(std::make_pair(response, *client_state));
    });
}

future<response_type> cql_server::connection::process_execute(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    auto id = read_short_bytes(buf);
    auto prepared = _server._query_processor.local().get_prepared(id);
    if (!prepared) {
        throw exceptions::prepared_query_not_found_exception(id);
    }
    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    q_state->options = read_options(buf);
    auto& options = *q_state->options;
    options.prepare(prepared->bound_names);
    auto stmt = prepared->statement;
    if (stmt->get_bound_terms() != options.get_values_count()) {
        throw exceptions::invalid_request_exception("Invalid amount of bind variables");
    }
    return _server._query_processor.local().process_statement(stmt, query_state, options).then([this, stream, buf = std::move(buf)] (auto msg) {
        return this->make_result(stream, msg);
    }).then([&query_state, q_state = std::move(q_state), this] (auto&& response) {
        /* Keep q_state alive. */
        return make_ready_future<response_type>(std::make_pair(response, query_state.get_client_state()));
    });
}

future<response_type>
cql_server::connection::process_batch(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    if (_version == 1) {
        throw exceptions::protocol_exception("BATCH messages are not support in version 1 of the protocol");
    }

    const auto type = read_byte(buf);
    const unsigned n = read_unsigned_short(buf);

    std::vector<shared_ptr<cql3::statements::modification_statement>> modifications;
    std::vector<std::vector<bytes_view_opt>> values;

    modifications.reserve(n);
    values.reserve(n);

    for ([[gnu::unused]] auto i : boost::irange(0u, n)) {
        const auto kind = read_byte(buf);

        ::shared_ptr<cql3::statements::parsed_statement::prepared> ps;

        switch (kind) {
        case 0: {
            auto query = read_long_string_view(buf).to_string();
            ps = _server._query_processor.local().get_statement(query, client_state);
            break;
        }
        case 1: {
            auto id = read_short_bytes(buf);
            ps = _server._query_processor.local().get_prepared(id);
            if (!ps) {
                throw exceptions::prepared_query_not_found_exception(id);
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

        modifications.emplace_back(static_pointer_cast<cql3::statements::modification_statement>(ps->statement));

        std::vector<bytes_view_opt> tmp;
        read_value_view_list(buf, tmp);

        auto stmt = ps->statement;
        if (stmt->get_bound_terms() != tmp.size()) {
            throw exceptions::invalid_request_exception(sprint("There were %d markers(?) in CQL but %d bound variables",
                            stmt->get_bound_terms(), tmp.size()));
        }
        values.emplace_back(std::move(tmp));
    }

    auto q_state = std::make_unique<cql_query_state>(client_state);
    auto& query_state = q_state->query_state;
    q_state->options = std::make_unique<cql3::query_options>(std::move(*read_options(buf)), std::move(values));
    auto& options = *q_state->options;

    auto batch = ::make_shared<cql3::statements::batch_statement>(-1, cql3::statements::batch_statement::type(type), std::move(modifications), cql3::attributes::none());
    return _server._query_processor.local().process_batch(batch, query_state, options).then([this, stream, batch] (auto msg) {
        return this->make_result(stream, msg);
    }).then([&query_state, q_state = std::move(q_state), this] (auto&& response) {
        /* Keep q_state alive. */
        return make_ready_future<response_type>(std::make_pair(response, query_state.get_client_state()));
    });
}

future<response_type>
cql_server::connection::process_register(uint16_t stream, bytes_view buf, service::client_state client_state)
{
    std::vector<sstring> event_types;
    read_string_list(buf, event_types);
    for (auto&& event_type : event_types) {
        auto et = parse_event_type(event_type);
        _server._notifier->register_event(et, this);
    }
    return make_ready_future<response_type>(std::make_pair(make_ready(stream), client_state));
}

shared_ptr<cql_server::response> cql_server::connection::make_unavailable_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t required, int32_t alive)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(required);
    response->write_int(alive);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_read_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, bool data_present)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_byte(data_present);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_mutation_write_timeout_error(int16_t stream, exceptions::exception_code err, sstring msg, db::consistency_level cl, int32_t received, int32_t blockfor, db::write_type type)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_consistency(cl);
    response->write_int(received);
    response->write_int(blockfor);
    response->write_string(sprint("%s", type));
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_already_exists_error(int16_t stream, exceptions::exception_code err, sstring msg, sstring ks_name, sstring cf_name)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_string(ks_name);
    response->write_string(cf_name);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_unprepared_error(int16_t stream, exceptions::exception_code err, sstring msg, bytes id)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    response->write_short_bytes(id);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_error(int16_t stream, exceptions::exception_code err, sstring msg)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::ERROR);
    response->write_int(static_cast<int32_t>(err));
    response->write_string(msg);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_ready(int16_t stream)
{
    return make_shared<cql_server::response>(stream, cql_binary_opcode::READY);
}

shared_ptr<cql_server::response> cql_server::connection::make_autheticate(int16_t stream, const sstring& clz)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::AUTHENTICATE);
    response->write_string(clz);
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_auth_success(int16_t stream, bytes b) {
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::AUTH_SUCCESS);
    response->write_bytes(std::move(b));
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_auth_challenge(int16_t stream, bytes b) {
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::AUTH_CHALLENGE);
    response->write_bytes(std::move(b));
    return response;
}

shared_ptr<cql_server::response> cql_server::connection::make_supported(int16_t stream)
{
    std::multimap<sstring, sstring> opts;
    opts.insert({"CQL_VERSION", cql3::query_processor::CQL_VERSION});
    opts.insert({"COMPRESSION", "snappy"});
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::SUPPORTED);
    response->write_string_multimap(opts);
    return response;
}

class cql_server::fmt_visitor : public messages::result_message::visitor {
private:
    uint8_t _version;
    shared_ptr<cql_server::response> _response;
public:
    fmt_visitor(uint8_t version, shared_ptr<cql_server::response> response)
        : _version{version}
        , _response{response}
    { }

    virtual void visit(const messages::result_message::void_message&) override {
        _response->write_int(0x0001);
    }

    virtual void visit(const messages::result_message::set_keyspace& m) override {
        _response->write_int(0x0003);
        _response->write_string(m.get_keyspace());
    }

    virtual void visit(const messages::result_message::prepared& m) override {
        auto prepared = m.get_prepared();
        _response->write_int(0x0004);
        _response->write_short_bytes(m.get_id());
        // FIXME: not compatible with v4
        assert(_version < 4);
        cql3::metadata metadata{prepared->bound_names};
        _response->write(metadata);
        if (_version > 1) {
            cql3::metadata result_metadata{std::vector<::shared_ptr<cql3::column_specification>>{}};
            result_metadata.set_skip_metadata();
            _response->write(result_metadata);
        }
    }

    virtual void visit(const messages::result_message::schema_change& m) override {
        auto change = m.get_change();
        switch (change->type) {
        case event::event_type::SCHEMA_CHANGE: {
            auto sc = static_pointer_cast<event::schema_change>(change);
            _response->write_int(0x0005);
            _response->serialize(*sc, _version);
            break;
        }
        default:
            assert(0);
        }
    }

    virtual void visit(const messages::result_message::rows& m) override {
        _response->write_int(0x0002);
        auto& rs = m.rs();
        _response->write(rs.get_metadata());
        _response->write_int(rs.size());
        for (auto&& row : rs.rows()) {
            for (auto&& cell : row | boost::adaptors::sliced(0, rs.get_metadata().column_count())) {
                _response->write_value(cell);
            }
        }
    }
};

shared_ptr<cql_server::response>
cql_server::connection::make_result(int16_t stream, shared_ptr<messages::result_message> msg)
{
    auto response = make_shared<cql_server::response>(stream, cql_binary_opcode::RESULT);
    fmt_visitor fmt{_version, response};
    msg->accept(fmt);
    return response;
}

shared_ptr<cql_server::response>
cql_server::connection::make_topology_change_event(const event::topology_change& event)
{
    auto response = make_shared<cql_server::response>(-1, cql_binary_opcode::EVENT);
    response->write_string("TOPOLOGY_CHANGE");
    response->write_string(to_string(event.change));
    response->write_inet(event.node);
    return response;
}

shared_ptr<cql_server::response>
cql_server::connection::make_status_change_event(const event::status_change& event)
{
    auto response = make_shared<cql_server::response>(-1, cql_binary_opcode::EVENT);
    response->write_string("STATUS_CHANGE");
    response->write_string(to_string(event.status));
    response->write_inet(event.node);
    return response;
}

shared_ptr<cql_server::response>
cql_server::connection::make_schema_change_event(const event::schema_change& event)
{
    auto response = make_shared<cql_server::response>(-1, cql_binary_opcode::EVENT);
    response->write_string("SCHEMA_CHANGE");
    response->serialize(event, _version);
    return response;
}

future<> cql_server::connection::write_response(foreign_ptr<shared_ptr<cql_server::response>>&& response)
{
    _ready_to_respond = _ready_to_respond.then([this, response = std::move(response)] () mutable {
        return do_with(std::move(response), [this] (auto& response) {
            return response->output(_write_buf, _version).then([this] {
                return _write_buf.flush();
            });
        });
    });
    return make_ready_future<>();
}

void cql_server::connection::check_room(bytes_view& buf, size_t n)
{
    if (buf.size() < n) {
        throw exceptions::protocol_exception(sprint("truncated frame: expected %lu bytes, length is %lu", n, buf.size()));
    }
}

void cql_server::connection::validate_utf8(sstring_view s)
{
    try {
        boost::locale::conv::utf_to_utf<char>(s.begin(), s.end(), boost::locale::conv::stop);
    } catch (const boost::locale::conv::conversion_error& ex) {
        throw exceptions::protocol_exception("Cannot decode string as UTF8");
    }
}

int8_t cql_server::connection::read_byte(bytes_view& buf)
{
    check_room(buf, 1);
    int8_t n = buf[0];
    buf.remove_prefix(1);
    return n;
}

int32_t cql_server::connection::read_int(bytes_view& buf)
{
    check_room(buf, sizeof(int32_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint32_t n = (static_cast<uint32_t>(p[0]) << 24)
               | (static_cast<uint32_t>(p[1]) << 16)
               | (static_cast<uint32_t>(p[2]) << 8)
               | (static_cast<uint32_t>(p[3]));
    buf.remove_prefix(4);
    return n;
}

int64_t cql_server::connection::read_long(bytes_view& buf)
{
    check_room(buf, sizeof(int64_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint64_t n = (static_cast<uint64_t>(p[0]) << 56)
               | (static_cast<uint64_t>(p[1]) << 48)
               | (static_cast<uint64_t>(p[2]) << 40)
               | (static_cast<uint64_t>(p[3]) << 32)
               | (static_cast<uint64_t>(p[4]) << 24)
               | (static_cast<uint64_t>(p[5]) << 16)
               | (static_cast<uint64_t>(p[6]) << 8)
               | (static_cast<uint64_t>(p[7]));
    buf.remove_prefix(8);
    return n;
}

int16_t cql_server::connection::read_short(bytes_view& buf)
{
    return static_cast<int16_t>(read_unsigned_short(buf));
}

uint16_t cql_server::connection::read_unsigned_short(bytes_view& buf)
{
    check_room(buf, sizeof(uint16_t));
    auto p = reinterpret_cast<const uint8_t*>(buf.begin());
    uint16_t n = (static_cast<uint16_t>(p[0]) << 8)
               | (static_cast<uint16_t>(p[1]));
    buf.remove_prefix(2);
    return n;
}

bytes cql_server::connection::read_short_bytes(bytes_view& buf)
{
    auto n = read_short(buf);
    check_room(buf, n);
    bytes s{reinterpret_cast<const int8_t*>(buf.begin()), static_cast<size_t>(n)};
    assert(n >= 0);
    buf.remove_prefix(n);
    return s;
}

sstring cql_server::connection::read_string(bytes_view& buf)
{
    auto n = read_short(buf);
    check_room(buf, n);
    sstring s{reinterpret_cast<const char*>(buf.begin()), static_cast<size_t>(n)};
    assert(n >= 0);
    buf.remove_prefix(n);
    validate_utf8(s);
    return s;
}

sstring_view cql_server::connection::read_string_view(bytes_view& buf)
{
    auto n = read_short(buf);
    check_room(buf, n);
    sstring_view s{reinterpret_cast<const char*>(buf.begin()), static_cast<size_t>(n)};
    assert(n >= 0);
    buf.remove_prefix(n);
    validate_utf8(s);
    return s;
}

sstring_view cql_server::connection::read_long_string_view(bytes_view& buf)
{
    auto n = read_int(buf);
    check_room(buf, n);
    sstring_view s{reinterpret_cast<const char*>(buf.begin()), static_cast<size_t>(n)};
    buf.remove_prefix(n);
    validate_utf8(s);
    return s;
}

db::consistency_level cql_server::connection::read_consistency(bytes_view& buf)
{
    return wire_to_consistency(read_short(buf));
}

std::unordered_map<sstring, sstring> cql_server::connection::read_string_map(bytes_view& buf)
{
    std::unordered_map<sstring, sstring> string_map;
    auto n = read_short(buf);
    for (auto i = 0; i < n; i++) {
        auto key = read_string(buf);
        auto val = read_string(buf);
        string_map.emplace(std::piecewise_construct,
            std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(std::move(val)));
    }
    return string_map;
}

enum class options_flag {
    VALUES,
    SKIP_METADATA,
    PAGE_SIZE,
    PAGING_STATE,
    SERIAL_CONSISTENCY,
    TIMESTAMP,
    NAMES_FOR_VALUES
};

using options_flag_enum = super_enum<options_flag,
    options_flag::VALUES,
    options_flag::SKIP_METADATA,
    options_flag::PAGE_SIZE,
    options_flag::PAGING_STATE,
    options_flag::SERIAL_CONSISTENCY,
    options_flag::TIMESTAMP,
    options_flag::NAMES_FOR_VALUES
>;

std::unique_ptr<cql3::query_options> cql_server::connection::read_options(bytes_view& buf)
{
    auto consistency = read_consistency(buf);
    if (_version == 1) {
        return std::make_unique<cql3::query_options>(consistency, std::experimental::nullopt, std::vector<bytes_view_opt>{},
            false, cql3::query_options::specific_options::DEFAULT, 1, _serialization_format);
    }

    assert(_version >= 2);

    auto flags = enum_set<options_flag_enum>::from_mask(read_byte(buf));
    std::vector<bytes_view_opt> values;
    std::vector<sstring_view> names;

    if (flags.contains<options_flag::VALUES>()) {
        if (flags.contains<options_flag::NAMES_FOR_VALUES>()) {
            read_name_and_value_list(buf, names, values);
        } else {
            read_value_view_list(buf, values);
        }
    }

    bool skip_metadata = flags.contains<options_flag::SKIP_METADATA>();
    flags.remove<options_flag::VALUES>();
    flags.remove<options_flag::SKIP_METADATA>();

    std::unique_ptr<cql3::query_options> options;
    if (flags) {
        ::shared_ptr<service::pager::paging_state> paging_state;
        int32_t page_size = flags.contains<options_flag::PAGE_SIZE>() ? read_int(buf) : -1;
        if (flags.contains<options_flag::PAGING_STATE>()) {
            paging_state = service::pager::paging_state::deserialize(read_value(buf));
        }

        db::consistency_level serial_consistency = db::consistency_level::SERIAL;
        if (flags.contains<options_flag::SERIAL_CONSISTENCY>()) {
            serial_consistency = read_consistency(buf);
        }

        api::timestamp_type ts = api::missing_timestamp;
        if (flags.contains<options_flag::TIMESTAMP>()) {
            ts = read_long(buf);
            if (ts < api::min_timestamp || ts > api::max_timestamp) {
                throw exceptions::protocol_exception(sprint("Out of bound timestamp, must be in [%d, %d] (got %d)",
                    api::min_timestamp, api::max_timestamp, ts));
            }
        }

        std::experimental::optional<std::vector<sstring_view>> onames;
        if (!names.empty()) {
            onames = std::move(names);
        }
        options = std::make_unique<cql3::query_options>(consistency, std::move(onames), std::move(values), skip_metadata,
            cql3::query_options::specific_options{page_size, std::move(paging_state), serial_consistency, ts}, _version,
            _serialization_format);
    } else {
        options = std::make_unique<cql3::query_options>(consistency, std::experimental::nullopt, std::move(values), skip_metadata,
            cql3::query_options::specific_options::DEFAULT, _version, _serialization_format);
    }

    return std::move(options);
}

void cql_server::connection::read_name_and_value_list(bytes_view& buf, std::vector<sstring_view>& names, std::vector<bytes_view_opt>& values) {
    uint16_t size = read_unsigned_short(buf);
    names.reserve(size);
    values.reserve(size);
    for (uint16_t i = 0; i < size; i++) {
        names.emplace_back(read_string(buf));
        values.emplace_back(read_value_view(buf));
    }
}

void cql_server::connection::read_string_list(bytes_view& buf, std::vector<sstring>& strings) {
    uint16_t size = read_unsigned_short(buf);
    strings.reserve(size);
    for (uint16_t i = 0; i < size; i++) {
        strings.emplace_back(read_string(buf));
    }
}

void cql_server::connection::read_value_view_list(bytes_view& buf, std::vector<bytes_view_opt>& values) {
    uint16_t size = read_unsigned_short(buf);
    values.reserve(size);
    for (uint16_t i = 0; i < size; i++) {
        values.emplace_back(read_value_view(buf));
    }
}

bytes_opt cql_server::connection::read_value(bytes_view& buf) {
    auto len = read_int(buf);
    if (len < 0) {
        return {};
    }
    check_room(buf, len);
    bytes b(reinterpret_cast<const int8_t*>(buf.begin()), len);
    buf.remove_prefix(len);
    return {std::move(b)};
}

bytes_view_opt cql_server::connection::read_value_view(bytes_view& buf) {
    auto len = read_int(buf);
    if (len < 0) {
        return {};
    }
    check_room(buf, len);
    bytes_view bv(reinterpret_cast<const int8_t*>(buf.begin()), len);
    buf.remove_prefix(len);
    return {std::move(bv)};
}

scattered_message<char> cql_server::response::make_message(uint8_t version) {
    scattered_message<char> msg;
    sstring body{_body.data(), _body.size()};
    sstring frame = make_frame(version, body.size());
    msg.append(std::move(frame));
    msg.append(std::move(body));
    return msg;
}

future<>
cql_server::response::output(output_stream<char>& out, uint8_t version) {
    auto frame = make_frame(version, _body.size());
    auto tmp = temporary_buffer<char>(frame.size());
    std::copy_n(frame.begin(), frame.size(), tmp.get_write());
    auto f = out.write(tmp.get(), tmp.size());
    return f.then([this, &out, tmp = std::move(tmp)] {
        return out.write(_body.data(), _body.size());
    });
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

sstring cql_server::response::make_frame(uint8_t version, size_t length)
{
    switch (version) {
    case 0x01:
    case 0x02: {
        sstring frame_buf(sstring::initialized_later(), sizeof(cql_binary_frame_v1));
        auto* frame = reinterpret_cast<cql_binary_frame_v1*>(frame_buf.begin());
        frame->version = version | 0x80;
        frame->flags   = 0x00;
        frame->stream  = _stream;
        frame->opcode  = static_cast<uint8_t>(_opcode);
        frame->length  = htonl(length);
        return frame_buf;
    }
    case 0x03:
    case 0x04: {
        sstring frame_buf(sstring::initialized_later(), sizeof(cql_binary_frame_v3));
        auto* frame = reinterpret_cast<cql_binary_frame_v3*>(frame_buf.begin());
        frame->version = version | 0x80;
        frame->flags   = 0x00;
        frame->stream  = htons(_stream);
        frame->opcode  = static_cast<uint8_t>(_opcode);
        frame->length  = htonl(length);
        return frame_buf;
    }
    default:
        throw exceptions::protocol_exception(sprint("Invalid or unsupported protocol version: %d", version));
    }
}

void cql_server::response::write_byte(uint8_t b)
{
    _body.insert(_body.end(), b);
}

void cql_server::response::write_int(int32_t n)
{
    auto u = htonl(n);
    auto *s = reinterpret_cast<const char*>(&u);
    _body.insert(_body.end(), s, s+sizeof(u));
}

void cql_server::response::write_long(int64_t n)
{
    auto u = htonq(n);
    auto *s = reinterpret_cast<const char*>(&u);
    _body.insert(_body.end(), s, s+sizeof(u));
}

void cql_server::response::write_short(int16_t n)
{
    auto u = htons(n);
    auto *s = reinterpret_cast<const char*>(&u);
    _body.insert(_body.end(), s, s+sizeof(u));
}

void cql_server::response::write_string(const sstring& s)
{
    assert(s.size() < std::numeric_limits<int16_t>::max());
    write_short(s.size());
    _body.insert(_body.end(), s.begin(), s.end());
}

void cql_server::response::write_long_string(const sstring& s)
{
    assert(s.size() < std::numeric_limits<int32_t>::max());
    write_int(s.size());
    _body.insert(_body.end(), s.begin(), s.end());
}

void cql_server::response::write_uuid(utils::UUID uuid)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_string_list(std::vector<sstring> string_list)
{
    assert(string_list.size() < std::numeric_limits<int16_t>::max());
    write_short(string_list.size());
    for (auto&& s : string_list) {
        write_string(s);
    }
}

void cql_server::response::write_bytes(bytes b)
{
    assert(b.size() < std::numeric_limits<int32_t>::max());
    write_int(b.size());
    _body.insert(_body.end(), b.begin(), b.end());
}

void cql_server::response::write_short_bytes(bytes b)
{
    assert(b.size() < std::numeric_limits<int16_t>::max());
    write_short(b.size());
    _body.insert(_body.end(), b.begin(), b.end());
}

void cql_server::response::write_option(std::pair<int16_t, data_value> opt)
{
    // FIXME
    assert(0);
}

void cql_server::response::write_option_list(std::vector<std::pair<int16_t, data_value>> opt_list)
{
    // FIXME
    assert(0);
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
    assert(string_map.size() < std::numeric_limits<int16_t>::max());
    write_short(string_map.size());
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
    assert(keys.size() < std::numeric_limits<int16_t>::max());
    write_short(keys.size());
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
    _body.insert(_body.end(), value->begin(), value->end());
}

class type_codec {
private:
    enum class type_id : int16_t {
        CUSTOM = 0,
        ASCII,
        BIGINT,
        BLOB,
        BOOLEAN,
        COUNTER,
        DECIMAL,
        DOUBLE,
        FLOAT,
        INT,
        TIMESTAMP = 11,
        UUID,
        VARCHAR,
        VARINT,
        TIMEUUID,
        INET,
        LIST = 32,
        MAP,
        SET,
        UDT = 48,
        TUPLE
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
    //(type_id::COUNTER   , CounterColumn_type)
    (type_id::DECIMAL   , decimal_type)
    (type_id::DOUBLE    , double_type)
    (type_id::FLOAT     , float_type)
    (type_id::INT       , int32_type)
    (type_id::TIMESTAMP , timestamp_type)
    (type_id::UUID      , uuid_type)
    (type_id::VARCHAR   , utf8_type)
    (type_id::VARINT    , varint_type)
    (type_id::TIMEUUID  , timeuuid_type)
    (type_id::INET      , inet_addr_type);

void cql_server::response::write(const cql3::metadata& m) {
    bool no_metadata = m.flags().contains<cql3::metadata::flag::NO_METADATA>();
    bool global_tables_spec = m.flags().contains<cql3::metadata::flag::GLOBAL_TABLES_SPEC>();
    bool has_more_pages = m.flags().contains<cql3::metadata::flag::HAS_MORE_PAGES>();

    write_int(m.flags().mask());
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

}
