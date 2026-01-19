/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>
#include <limits>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/lowres_clock.hh>
#include <fmt/format.h>
#include <seastar/util/log.hh>
#include <signal.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include "db/config.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"
#include "transport/server.hh"
#include "transport/response.hh"
#include <cstring>
#include <unordered_map>

namespace perf {
using namespace seastar;
namespace bpo = boost::program_options;
using namespace cql_transport;

// Small hand and AI crafted CQL client that  builds raw
// frames directly and sends over a tcp connection to exercise the full
// CQL binary protocol parsing path without any external driver layers.

struct raw_cql_test_config {
    std::string workload; // read | write | connect
    unsigned partitions;  // number of partitions existing / to write
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    unsigned concurrency_per_connection; // requests per connection
    unsigned connections_per_shard; // connections per shard
    bool continue_after_error;
    uint16_t port = 9042; // native transport port
    std::string username = ""; // optional auth username
    std::string password = ""; // optional auth password
    std::string remote_host = ""; // target host for CQL + REST (empty => in-process server mode)
    bool connection_per_request = false; // create and tear down a connection for every request
    bool use_prepared = true;
    bool create_non_superuser = false;

    sharded<abort_source>* as = nullptr;
};

} // namespace perf

template <>
struct fmt::formatter<perf::raw_cql_test_config> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const perf::raw_cql_test_config& c, format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{workload={}, partitions={}, concurrency={}, connections={}, duration={}, ops_per_shard={}{}{}{}{}}}",
            c.workload, c.partitions, c.concurrency_per_connection, c.connections_per_shard, c.duration_in_seconds, c.operations_per_shard,
            (c.username.empty() ? "" : ", auth"),
            (c.connection_per_request ? ", connection_per_request" : ""),
            (c.use_prepared ? ", use_prepared" : ""),
            (c.create_non_superuser ? ", create_non_superuser" : ""));
    }
};

namespace perf {

// Basic frame building helpers (CQL v4)
// Binary protocol v4 header is 9 bytes:
//  0: version (request direction bit clear, thus 0x04)
//  1: flags
//  2: stream id (msb)
//  3: stream id (lsb)
//  4: opcode
//  5..8: body length (big endian)
struct frame_builder {
    static constexpr size_t header_size = 9;
    static constexpr size_t initial_capacity = 256;

    int16_t stream_id;
    temporary_buffer<char> body;
    size_t pos = header_size;

    frame_builder(int16_t stream) : stream_id(stream), body(initial_capacity) {}

    void write_int(int32_t v) {
        write_be<int32_t>(body.get_write() + pos, v);
        pos += 4;
    }
    void write_short(uint16_t v) {
        write_be<uint16_t>(body.get_write() + pos, v);
        pos += 2;
    }
    void write_byte(char c) {
        body.get_write()[pos++] = c;
    }
    void write_raw(const char* data, size_t len) {
        std::memcpy(body.get_write() + pos, data, len);
        pos += len;
    }
    void write_string(std::string_view s) {
        write_short(s.size());
        write_raw(s.data(), s.size());
    }
    void write_long_string(std::string_view s) {
        write_int(s.size());
        write_raw(s.data(), s.size());
    }
    void write_bytes(std::string_view s) {
        write_int(s.size());
        write_raw(s.data(), s.size());
    }
    temporary_buffer<char> finish(cql_binary_opcode op) {
        size_t body_len = pos - header_size;
        auto* p = body.get_write();
        p[0] = 0x04;
        p[1] = 0;
        write_be<int16_t>(p + 2, stream_id);
        p[4] = static_cast<uint8_t>(op);
        write_be<int32_t>(p + 5, body_len);
        body.trim(pos);
        return std::move(body);
    }
};

static sstring make_key(uint64_t seq) {
    sstring b(sstring::initialized_later(), sizeof(seq));
    write_be<uint64_t>(b.begin(), seq);
    return b;
}

static sstring to_hex(std::string_view b) {
    static const char* digits = "0123456789abcdef";
    sstring r;
    r.resize(b.size() * 2);
    for (size_t i = 0; i < b.size(); ++i) {
        uint8_t v = b[i];
        r[2 * i] = digits[(v >> 4) & 0xF];
        r[2 * i + 1] = digits[v & 0xF];
    }
    return r;
}

class raw_cql_connection {
    connected_socket _cs;
    input_stream<char> _in;
    output_stream<char> _out;
    semaphore _connection_sem{1};
    sstring _username;
    sstring _password;
    bool _use_prepared = false;
    sstring _read_stmt_id;
    sstring _write_stmt_id;

    struct frame {
        cql_binary_opcode opcode;
        int16_t stream;
        temporary_buffer<char> payload;
    };

    std::unordered_map<int16_t, promise<frame>> _requests;
    future<> _reader_done = make_ready_future<>();
    bool _reader_stopped = false;
    int16_t _next_stream_id = -1;

public:
    raw_cql_connection(connected_socket cs, sstring username = {}, sstring password = {}, bool use_prepared = false)
        : _cs(std::move(cs)), _in(_cs.input()), _out(_cs.output()), _username(std::move(username)), _password(std::move(password)), _use_prepared(use_prepared) {
        start_reader();
    }

    future<> stop() {
        _reader_stopped = true;
        try {
            co_await _in.close();
            co_await _out.close();
        } catch (...) {
            // ignore
        }
        try {
            co_await std::move(_reader_done);
        } catch (...) {
            // ignore
        }
    }

    void start_reader() {
        _reader_done = reader_loop();
    }

    future<> reader_loop() {
        while (!_reader_stopped) {
            try {
                auto f = co_await read_one_frame_internal();
                if (auto it = _requests.find(f.stream); it != _requests.end()) {
                    it->second.set_value(std::move(f));
                    _requests.erase(it);
                }
            } catch (...) {
                auto ep = std::current_exception();
                for (auto& [id, pr] : _requests) {
                    pr.set_exception(ep);
                }
                _requests.clear();
                _reader_stopped = true;
            }
        }
    }

    int16_t allocate_stream() {
        _next_stream_id++;
        return _next_stream_id;
    }

    future<> send_frame(temporary_buffer<char> buf) {
        auto units = co_await get_units(_connection_sem, 1);
        co_await _out.write(std::move(buf));
        co_await _out.flush();
    }

    future<frame> read_one_frame_internal() {
        static constexpr size_t header_size = 9;
        auto hdr_buf = co_await _in.read_exactly(header_size);
        if (hdr_buf.empty()) {
            throw std::runtime_error("connection closed");
        }
        if (hdr_buf.size() != header_size) {
            throw std::runtime_error("short frame header");
        }
        const char* h = hdr_buf.get();
        uint8_t version = h[0];
        (void)version; // unused currently
        uint8_t flags = h[1]; (void)flags;
        uint16_t stream = read_be<uint16_t>(h + 2);
        auto opcode = static_cast<cql_binary_opcode>(h[4]);
        uint32_t len = read_be<uint32_t>(h + 5);

        // Basic protocol sanity checks to catch framing issues early.
        if ((version & 0x7F) != 0x04) {
            throw std::runtime_error(fmt::format("unexpected protocol version byte 0x{:02x} (expected 0x84/0x04)", version));
        }
        if (len > (32u << 20)) { // 32MB arbitrary safety limit
            throw std::runtime_error(fmt::format("suspiciously large frame body length {} > 32MB (malformed?)", len));
        }
        auto body = co_await _in.read_exactly(len);
        if (body.size() != len) {
            throw std::runtime_error("short frame body");
        }
        co_return frame{opcode, stream, std::move(body)};
    }

    future<frame> execute_request(int16_t stream, temporary_buffer<char> buf) {
        promise<frame> p;
        auto f = p.get_future();
        _requests.emplace(stream, std::move(p));
        try {
            co_await send_frame(std::move(buf));
        } catch (...) {
            _requests.erase(stream);
            throw;
        }
        co_return co_await std::move(f);
    }

    future<> startup() {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        // STARTUP frame body (v4): <map<string,string>> of options
        // map encodes with a <short n> for number of entries, then n*(<string><string>)
        fb.write_short(1); // one entry
        fb.write_string("CQL_VERSION");
        fb.write_string("3.0.0");

        auto frame = co_await execute_request(stream, fb.finish(cql_binary_opcode::STARTUP));
        auto op = frame.opcode;
        auto payload = std::move(frame.payload);

        // If user supplied credentials we require the server to challenge with AUTHENTICATE.
        if (!_username.empty() && op != cql_binary_opcode::AUTHENTICATE) {
            throw std::runtime_error("--username specified but server did not request authentication (expected AUTHENTICATE frame)");
        }
        if (op == cql_binary_opcode::AUTHENTICATE) {
            // Assume PasswordAuthenticator; send SASL PLAIN (no need to inspect class name).
            frame_builder auth_fb{stream}; // reuse same stream id per protocol spec
            if (_username.empty()) {
                // Send empty bytes (legacy AllowAll / will trigger error if auth required but no creds supplied)
                auth_fb.write_int(0);
            } else {
                // SASL PLAIN: 0x00 username 0x00 password
                std::string plain;
                plain.reserve(2 + _username.size() + _password.size());
                plain.push_back('\0');
                plain.append(_username.c_str(), _username.size());
                plain.push_back('\0');
                plain.append(_password.c_str(), _password.size());
                auth_fb.write_int(plain.size());
                auth_fb.write_raw(plain.data(), plain.size());
            }
            auto res = co_await execute_request(stream, auth_fb.finish(cql_binary_opcode::AUTH_RESPONSE));
            op = res.opcode;
            payload = std::move(res.payload);
        }
        if (op != cql_binary_opcode::READY && op != cql_binary_opcode::AUTH_SUCCESS) {
            // Try to decode ERROR for better diagnostics
            if (op == cql_binary_opcode::ERROR && payload.size() >= 4) {
                int32_t code = read_be<int32_t>(payload.get());
                // message string follows: <string>
                if (payload.size() >= 6) {
                    auto p = payload.get() + 4;
                    uint16_t slen = read_be<uint16_t>(p);
                    p += 2;
                    sstring msg;
                    if (payload.size() >= 6 + slen) {
                        msg = sstring(p, slen);
                    }
                    throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got ERROR code={} msg='{}'", code, msg));
                }
            }
            throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got opcode {}", static_cast<int>(op)));
        }
        if (!_username.empty()) {
            // With credentials expect AUTH_SUCCESS explicitly.
            if (op != cql_binary_opcode::AUTH_SUCCESS) {
                throw std::runtime_error("authentication expected AUTH_SUCCESS but got different opcode");
            }
        }
    }

    future<> query_simple(std::string_view q) {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        // QUERY frame (v4): <long string><short consistency><byte flags>
        fb.write_long_string(q);
        fb.write_short(0x0001); // ONE
        fb.write_byte(0); // flags
        auto f = co_await execute_request(stream, fb.finish(cql_binary_opcode::QUERY));
        if (f.opcode == cql_binary_opcode::ERROR) {
            throw std::runtime_error(format("server returned ERROR to QUERY: {}", std::string_view(f.payload.get(), f.payload.size())));
        }
    }

    future<sstring> prepare_query(std::string_view q) {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        fb.write_long_string(q);
        auto f = co_await execute_request(stream, fb.finish(cql_binary_opcode::PREPARE));
        auto op = f.opcode;
        auto payload = std::move(f.payload);

        if (op != cql_binary_opcode::RESULT) {
            throw std::runtime_error(fmt::format("expected RESULT for PREPARE, got {}", static_cast<int>(op)));
        }
        // RESULT body: [int kind][short id_len][bytes id]...
        if (payload.size() < 4) {
            throw std::runtime_error("short RESULT body");
        }
        int32_t kind = read_be<int32_t>(payload.get());
        if (kind != 0x0004) { // PREPARED
            throw std::runtime_error(fmt::format("expected RESULT kind PREPARED (4), got {}", kind));
        }
        if (payload.size() < 6) {
            throw std::runtime_error("short PREPARED body");
        }
        uint16_t id_len = read_be<uint16_t>(payload.get() + 4);
        if (payload.size() < 6 + id_len) {
            throw std::runtime_error("short PREPARED id");
        }
        sstring id(payload.get() + 6, id_len);
        co_return id;
    }

    future<> execute_prepared(const sstring& id, std::string_view key) {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        fb.write_string(id); // [short bytes]
        fb.write_short(0x0001); // ONE
        // Flags: VALUES (0x01) | SKIP_METADATA (0x02) = 0x03
        fb.write_byte(0x03);
        fb.write_short(1); // 1 value
        // Value is [int len] + bytes.
        // Our key is bytes.
        fb.write_int(key.size());
        fb.write_raw(key.data(), key.size());

        auto f = co_await execute_request(stream, fb.finish(cql_binary_opcode::EXECUTE));
        if (f.opcode == cql_binary_opcode::ERROR) {
            throw std::runtime_error("server returned ERROR to EXECUTE");
        }
    }

    future<> prepare_statements() {
        if (!_use_prepared) {
            co_return;
        }
        _write_stmt_id = co_await prepare_query("INSERT INTO ks.cf(pk,c0,c1,c2,c3,c4) VALUES (?,0x01,0x02,0x03,0x04,0x05)");
        _read_stmt_id = co_await prepare_query("SELECT * FROM ks.cf WHERE pk=?");
    }

    future<> write_one(uint64_t seq) {
        auto key = make_key(seq);
        if (_use_prepared) {
            co_await execute_prepared(_write_stmt_id, key);
        } else {
            auto key_hex = to_hex(key);
            co_await query_simple(fmt::format("INSERT INTO ks.cf(pk,c0,c1,c2,c3,c4) VALUES (0x{},0x01,0x02,0x03,0x04,0x05)", key_hex));
        }
    }

    future<> read_one(uint64_t seq) {
        auto key = make_key(seq);
        if (_use_prepared) {
            co_await execute_prepared(_read_stmt_id, key);
        } else {
            auto key_hex = to_hex(key);
            co_await query_simple(fmt::format("SELECT * FROM ks.cf WHERE pk=0x{}", key_hex));
        }
    }
};

static future<> ensure_schema(raw_cql_connection& conn) {
    co_await conn.query_simple("CREATE KEYSPACE IF NOT EXISTS ks WITH replication={'class': 'NetworkTopologyStrategy'}");
    co_await conn.query_simple("CREATE TABLE IF NOT EXISTS ks.cf (pk blob primary key, c0 blob, c1 blob, c2 blob, c3 blob, c4 blob)");
}

static future<> create_role_with_permissions(raw_cql_connection& conn, std::string_view username, std::string_view password) {
    co_await conn.query_simple(fmt::format("CREATE ROLE IF NOT EXISTS '{}' WITH PASSWORD = '{}' AND LOGIN = true", username, password));
    co_await conn.query_simple(fmt::format("GRANT CREATE ON ALL KEYSPACES TO {}", username));
    co_await conn.query_simple(fmt::format("GRANT SELECT ON ALL KEYSPACES TO {}", username));
    co_await conn.query_simple(fmt::format("GRANT MODIFY ON ALL KEYSPACES TO {}", username));
}

static constexpr std::string_view non_superuser_name = "perf_test_user";
static constexpr std::string_view non_superuser_password = "perf_test_password";

static std::unique_ptr<raw_cql_connection> make_connection(connected_socket cs, const raw_cql_test_config& cfg) {
    sstring username = cfg.create_non_superuser ? sstring(non_superuser_name) : sstring(cfg.username);
    sstring password = cfg.create_non_superuser ? sstring(non_superuser_password) : sstring(cfg.password);
    return std::make_unique<raw_cql_connection>(std::move(cs), username, password, cfg.use_prepared);
}

// Perform one logical operation (write or read) using an existing connection.
static future<> do_request(raw_cql_connection& c, const raw_cql_test_config& cfg) {
    auto seq = tests::random::get_int<uint64_t>(cfg.partitions - 1);
    if (cfg.workload == "write") {
        co_await c.write_one(seq);
    } else {
        co_await c.read_one(seq);
    }
}

// Create a fresh connection, run a single operation, then let it go out of scope.
static future<> run_one_with_new_connection(const raw_cql_test_config& cfg) {
    connected_socket cs;
    try {
        cs = co_await connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port});
    } catch (...) {
        cs = connected_socket();
    }
    if (!cs) {
        throw std::runtime_error("Failed to connect (single attempt)");
    }
    auto c = make_connection(std::move(cs), cfg);
    std::exception_ptr ep;
    try {
        co_await c->startup();
        if (cfg.workload != "connect") {
            co_await c->prepare_statements();
            co_await do_request(*c, cfg);
        }
    } catch (...) {
        ep = std::current_exception();
    }
    co_await c->stop();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

// Poll the REST API /compaction_manager/compactions until it returns an empty JSON array
// indicating there are no ongoing compactions. Throws on timeout.
static void wait_for_compactions(const raw_cql_test_config& cfg) {
    using namespace std::chrono_literals;
    const unsigned max_attempts = 600; // ~60s
    bool announced = false;
    for (unsigned attempt = 0; attempt < max_attempts; ++attempt) {
        try {
            connected_socket http_cs = connect(socket_address{
                    net::inet_address{cfg.remote_host}, 10000}).get();
            input_stream<char> in = http_cs.input();
            output_stream<char> out = http_cs.output();
            sstring req = seastar::format("GET /compaction_manager/compactions HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", cfg.remote_host);
            out.write(req).get();
            out.flush().get();
            sstring resp;
            while (true) {
                auto buf = in.read().get();
                if (!buf) {
                    break;
                }
                resp.append(buf.get(), buf.size());
            }
            auto pos = resp.find("\r\n\r\n");
            if (pos != sstring::npos) {
                auto body = resp.substr(pos + 4);
                boost::algorithm::trim(body);
                if (body == "[]") {
                    if (attempt) {
                        std::cout << "Compactions drained after " << attempt << " polls" << std::endl;
                    }
                    return;
                } else if (!announced) {
                    std::cout << "Waiting for compactions to end..." << std::endl;
                    announced = true;
                }
            }
        } catch (...) {
            // Ignore and retry
        }
        sleep(100ms).get();
    }
    throw std::runtime_error("Timed out waiting for compactions to drain (endpoint did not return empty JSON array)");
}

// Thread-local connection pool state extracted so that initialization can be performed
// outside of the timed body passed to time_parallel (avoids depressing the first TPS sample).
static thread_local std::vector<std::unique_ptr<raw_cql_connection>> tl_conns;

static future<> prepare_thread_connections(const raw_cql_test_config cfg) {
    SCYLLA_ASSERT(tl_conns.empty());
    tl_conns.reserve(cfg.connections_per_shard);
    for (unsigned i = 0; i < cfg.connections_per_shard; ++i) {
        connected_socket cs;
        for (int attempt = 0; attempt < 200; ++attempt) {
            try {
                cs = co_await connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port});
            } catch (...) {
                cs = connected_socket();
            }
            if (cs) {
                break;
            }
            co_await sleep(std::chrono::milliseconds(25));
        }
        if (!cs) {
            throw std::runtime_error("Failed to connect to native transport port");
        }
        auto c = make_connection(std::move(cs), cfg);
        co_await c->startup();
        co_await c->prepare_statements();
        tl_conns.push_back(std::move(c));
    }
}

static void prepopulate(const raw_cql_test_config& cfg) {
    try {
        if (cfg.create_non_superuser) {
            connected_socket superuser_cs;
            for (int attempt = 0; attempt < 200; ++attempt) {
                try {
                    superuser_cs = connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port}).get();
                } catch (...) {
                    superuser_cs = connected_socket();
                }
                if (superuser_cs) {
                    break;
                }
                sleep(std::chrono::milliseconds(25)).get();
            }
            if (!superuser_cs) {
                throw std::runtime_error("populate phase: failed to connect as superuser");
            }
            raw_cql_connection superuser_conn(std::move(superuser_cs), sstring(cfg.username), sstring(cfg.password), false);
            try {
                superuser_conn.startup().get();
                create_role_with_permissions(superuser_conn, non_superuser_name, non_superuser_password).get();
                ensure_schema(superuser_conn).get();
            } catch (...) {
                superuser_conn.stop().get();
                throw;
            }
            superuser_conn.stop().get();
            std::cout << "Created role '" << non_superuser_name << "' with CREATE, SELECT, MODIFY permissions on all keyspaces" << std::endl;
        }

        connected_socket cs;
        for (int attempt = 0; attempt < 200; ++attempt) {
            try {
                cs = connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port}).get();
            } catch (...) {
                cs = connected_socket();
            }
            if (cs) {
                break;
            }
            sleep(std::chrono::milliseconds(25)).get();
        }
        if (!cs) {
            throw std::runtime_error("populate phase: failed to connect");
        }
        auto conn = make_connection(std::move(cs), cfg);
        try {
            conn->startup().get();
            if (!cfg.create_non_superuser) {
                ensure_schema(*conn).get();
            }
            conn->prepare_statements().get();
            for (uint64_t seq = 0; seq < cfg.partitions; ++seq) {
                conn->write_one(seq).get();
            }
        } catch (...) {
            conn->stop().get();
            throw;
        }
        conn->stop().get();
        std::cout << "Pre-populated " << cfg.partitions << " partitions" << std::endl;
    } catch (...) {
        std::cerr << "Population failed: " << std::current_exception() << std::endl;
        throw;
    }
}

static void workload_main(raw_cql_test_config cfg) {
    fmt::print("Running test with config: {}\n", cfg);
    auto cleanup = defer([] {
        // Cleanup thread-local connections to avoid destruction issues at exit
        smp::invoke_on_all([] {
            return parallel_for_each(tl_conns, [](std::unique_ptr<raw_cql_connection>& c) {
                    return c->stop();
            }).then([] {
                tl_conns.clear();
            });
        }).get();
    });
    if (cfg.workload != "connect") {
        prepopulate(cfg);
    }
    try {
        wait_for_compactions(cfg);
    } catch (...) {
        std::cerr << "Compaction wait failed: " << std::current_exception() << std::endl;
        throw;
    }
    if (!cfg.connection_per_request && cfg.workload != "connect") {
        // Warm up: establish all per-thread connections before measurement.
        try {
            smp::invoke_on_all([cfg] {
                return prepare_thread_connections(cfg);
            }).get();
        } catch (...) {
            std::cerr << "Connection preparation failed: " << std::current_exception() << std::endl;
            throw;
        }
    }
    auto results = time_parallel([cfg] () -> future<> {
        cfg.as->local().check();
        if (cfg.connection_per_request || cfg.workload == "connect") {
            co_await run_one_with_new_connection(cfg);
        } else {
            static thread_local size_t idx = 0;
            // Round-robin over thread-local connections
            auto& c = *tl_conns[idx++ % tl_conns.size()];
            co_await do_request(c, cfg);
        }
    }, cfg.concurrency_per_connection * cfg.connections_per_shard, cfg.duration_in_seconds, cfg.operations_per_shard, !cfg.continue_after_error);
    std::cout << aggregated_perf_results(results) << std::endl;
}

static future<> run_standalone(raw_cql_test_config c) {
    auto as = make_shared<sharded<abort_source>>();
    co_await as->start();
    c.as = as.get();

    auto stop_handler = [as] {
        (void)as->invoke_on_all(&abort_source::request_abort);
    };

    seastar::handle_signal(SIGINT, stop_handler);
    seastar::handle_signal(SIGTERM, stop_handler);

    std::exception_ptr ex;
    try {
        co_await seastar::async([c = std::move(c)] {
            workload_main(c);
        });
    } catch (...) {
        ex = std::current_exception();
    }
    co_await as->stop();
    if (ex) {
        std::rethrow_exception(ex);
    }
}

// Returns a function which launches a performance workload that
// talks to the embedded server over the native CQL protocol using
// handcrafted CQL binary frames (no driver). Similar to perf_alternator
// (runs inside the server process) and perf_simple_query (similar workload types), but
// exercises the full networking + protocol parsing path.
std::function<int(int, char**)> perf_cql_raw(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config>)>* after_init_func) {
    return [=](int ac, char** av) -> int {
        raw_cql_test_config c;
        bpo::options_description opts_desc;
        opts_desc.add_options()
            ("workload", bpo::value<std::string>()->default_value("read"), "workload type: read|write|connect")
            ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
            ("duration", bpo::value<unsigned>()->default_value(5), "test duration seconds")
            ("operations-per-shard", bpo::value<unsigned>()->default_value(0), "fixed op count per shard")
            ("concurrency-per-shard", bpo::value<unsigned>()->default_value(10), "concurrent requests per connection")
            ("connections-per-shard", bpo::value<unsigned>()->default_value(100), "connections per shard")
            ("continue-after-error", bpo::value<bool>()->default_value(false), "continue after error")
            ("username", bpo::value<std::string>()->default_value(""), "authentication username (used as superuser when create-non-superuser is set)")
            ("password", bpo::value<std::string>()->default_value(""), "authentication password (used as superuser when create-non-superuser is set)")
            ("create-non-superuser", bpo::value<bool>()->default_value(false), "create a non-superuser role using username/password as superuser credentials")
            ("remote-host", bpo::value<std::string>()->default_value(""), "remote host to connect to, leave empty to run in-process server")
            ("connection-per-request", bpo::value<bool>()->default_value(false), "create a fresh connection for every request")
            ("use-prepared", bpo::value<bool>()->default_value(true), "use prepared statements");
        bpo::variables_map vm;
        bpo::store(bpo::command_line_parser(ac,av).options(opts_desc).allow_unregistered().run(), vm);

        c.workload = vm["workload"].as<std::string>();
        c.partitions = vm["partitions"].as<unsigned>();
        c.duration_in_seconds = vm["duration"].as<unsigned>();
        c.operations_per_shard = vm["operations-per-shard"].as<unsigned>();
        c.concurrency_per_connection = vm["concurrency-per-shard"].as<unsigned>();
        c.connections_per_shard = vm["connections-per-shard"].as<unsigned>();
        c.continue_after_error = vm["continue-after-error"].as<bool>();
        c.username = vm["username"].as<std::string>();
        c.password = vm["password"].as<std::string>();
        c.create_non_superuser = vm["create-non-superuser"].as<bool>();
        c.remote_host = vm["remote-host"].as<std::string>();
        c.connection_per_request = vm["connection-per-request"].as<bool>();
        c.use_prepared = vm["use-prepared"].as<bool>();

        if (!c.username.empty() && c.password.empty()) {
            std::cerr << "--username specified without --password" << std::endl;
            return 1;
        }
        if (c.create_non_superuser && (c.username.empty() || c.password.empty())) {
            std::cerr << "--create-non-superuser requires both --username and --password" << std::endl;
            return 1;
        }
        if (c.workload != "read" && c.workload != "write" && c.workload != "connect") {
            std::cerr << "Unknown workload: " << c.workload << "\n"; return 1;
        }

        // Remove test options to not disturb scylla main app
        for (auto& opt : opts_desc.options()) {
            auto name = opt->canonical_display_name(bpo::command_line_style::allow_long);
            std::tie(ac, av) = cut_arg(ac, av, name);
        }

        if (!c.remote_host.empty()) {
            // if remote-host provided (non-empty) we run standalone
            c.port = 9042; // TODO: make configurable
            app_template app;
            return app.run(ac, av, [c = std::move(c)] () mutable -> future<> {
                return run_standalone(std::move(c));
            });
        } else {
            // in-process mode
            c.remote_host = "127.0.0.1";
        }

        // Unconditionally append --api-address=127.0.0.1 so the main server binds API locally.
        static std::string api_arg = "--api-address=127.0.0.1";
        {
            // Build a new argv with the extra argument (simple leak acceptable for process lifetime)
            char** new_av = new char*[ac + 2];
            for (int i = 0; i < ac; ++i) { new_av[i] = av[i]; }
            new_av[ac] = const_cast<char*>(api_arg.c_str());
            new_av[ac + 1] = nullptr;
            av = new_av;
            ++ac;
        }

        *after_init_func = [c](lw_shared_ptr<db::config> cfg) mutable {
            c.port = cfg->native_transport_port();
            // run workload in background-ish
            (void)seastar::async([c]() {
                try {
                    workload_main(c);
                } catch (...) {
                    std::cerr << "Perf test failed: " << std::current_exception() << std::endl;
                    raise(SIGKILL); // abnormal shutdown to signal test failure
                }
                raise(SIGINT); // normal shutdown request after test completion
            });
        };
        return scylla_main(ac, av);
    };
}

} // namespace perf
