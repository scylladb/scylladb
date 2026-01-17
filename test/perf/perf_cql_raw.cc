/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/lowres_clock.hh>
#include <fmt/format.h>
#include <signal.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include "db/config.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"
#include "transport/server.hh"
#include "transport/response.hh"
#include <cstring>

namespace perf {
using namespace seastar;
namespace bpo = boost::program_options;
using namespace cql_transport;

// Small hand and AI crafted CQL client that  builds raw
// frames directly and sends over a tcp connection to exercise the full
// CQL binary protocol parsing path without any external driver layers.

struct raw_cql_test_config {
    std::string workload; // read | write
    unsigned partitions;  // number of partitions existing / to write
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    unsigned concurrency; // connections per shard
    bool continue_after_error;
    uint16_t port = 9042; // native transport port
    std::string username; // optional auth username
    std::string password; // optional auth password
    std::string remote_host = "127.0.0.1"; // target host for CQL + REST (empty => in-process server mode)
    bool connection_per_request = false; // create and tear down a connection for every request
    bool use_prepared = true;
    bool create_non_superuser = false;
};

} // namespace perf

template <>
struct fmt::formatter<perf::raw_cql_test_config> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const perf::raw_cql_test_config& c, format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{workload={}, partitions={}, concurrency={}, duration={}, ops_per_shard={}{}{}{}{}}}",
            c.workload, c.partitions, c.concurrency, c.duration_in_seconds, c.operations_per_shard,
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
    int16_t stream_id;
    bytes_ostream body;
    // CQL protocol uses big-endian (network order) for all multi-byte numeric primitives.
    // The generic write<T>() helper used elsewhere in the codebase emits host-endian order
    // (little-endian on our platforms), so we serialize bytes manually here to avoid double
    // or incorrect swapping.
    void write_int(int32_t v) {
        auto p = body.write_place_holder(4);
        p[0] = (uint8_t)((uint32_t)v >> 24);
        p[1] = (uint8_t)((uint32_t)v >> 16);
        p[2] = (uint8_t)((uint32_t)v >> 8);
        p[3] = (uint8_t)((uint32_t)v);
    }
    void write_short(uint16_t v) {
        auto p = body.write_place_holder(2);
        p[0] = (uint8_t)(v >> 8);
        p[1] = (uint8_t)(v & 0xFF);
    }
    void write_byte(uint8_t v) {
        auto p = body.write_place_holder(1);
        p[0] = v;
    }
    void write_string(std::string_view s) {
        write_short(s.size());
        body.write(s.data(), s.size());
    }
    void write_long_string(std::string_view s) {
        write_int(s.size());
        body.write(s.data(), s.size());
    }
    void write_bytes(std::string_view s) {
        write_int(s.size());
        body.write(s.data(), s.size());
    }
    temporary_buffer<char> finish(cql_binary_opcode op) {
        size_t len = body.size();
        static constexpr size_t header_size = 9;
        temporary_buffer<char> buf(len + header_size);
        auto* p = buf.get_write();
        p[0] = 0x04; // version 4 request
        p[1] = 0;    // flags
        p[2] = (stream_id >> 8) & 0xFF;
        p[3] = stream_id & 0xFF;
        p[4] = static_cast<uint8_t>(op);
        uint32_t be_len = htonl(len);
        std::memcpy(p + 5, &be_len, 4);
        // Copy accumulated body bytes into the outgoing buffer.
        // bytes_ostream doesn't provide a direct contiguous view unless linearized;
        // iterate fragments to avoid an extra allocation.
        size_t off = 0;
        for (auto frag : body.fragments()) {
            std::memcpy(p + header_size + off, frag.begin(), frag.size());
            off += frag.size();
        }
        SCYLLA_ASSERT(off == len);
        return buf;
    }
};

static bytes make_key(uint64_t seq) {
    bytes b(bytes::initialized_later(), sizeof(seq));
    auto i = b.begin();
    write<uint64_t>(i, seq);
    return b;
}

static sstring to_hex(bytes_view b) {
    static const char* digits = "0123456789abcdef";
    sstring r;
    r.resize(b.size() * 2);
    for (size_t i = 0; i < b.size(); ++i) {
        r[2 * i] = digits[(b[i] >> 4) & 0xF];
        r[2 * i + 1] = digits[b[i] & 0xF];
    }
    return r;
}

class raw_cql_connection {
    connected_socket _cs;
    input_stream<char> _in;
    output_stream<char> _out;
    // Ensure only one in-flight request per connection. Without this, two
    // workload fibers may interleave writes and especially reads on the same
    // input/output streams leading to undefined behavior and crashes
    // (double-setting futures inside seastar's pollable fd state).
    // Note: currently it should not be needed as we do only one
    // concurrent request per connection.
    semaphore _use_sem{1};
    sstring _username;
    sstring _password;
    bool _use_prepared = false;
    sstring _read_stmt_id;
    sstring _write_stmt_id;
public:
    raw_cql_connection(connected_socket cs, sstring username = {}, sstring password = {}, bool use_prepared = false)
        : _cs(std::move(cs)), _in(_cs.input()), _out(_cs.output()), _username(std::move(username)), _password(std::move(password)), _use_prepared(use_prepared) {}

    int16_t allocate_stream() {
        // We send one request at a time per connection, so we can reuse stream id 0.
        return 0;
    }

    future<> send_frame(temporary_buffer<char> buf) {
        co_await _out.write(std::move(buf));
        co_await _out.flush();
    }

    future<cql_binary_opcode> read_one_frame(bytes& payload) {
        static constexpr size_t header_size = 9;
        auto hdr_buf = co_await _in.read_exactly(header_size);
        if (hdr_buf.empty()) {
            throw std::runtime_error("connection closed");
        }
        if (hdr_buf.size() != header_size) {
            throw std::runtime_error("short frame header");
        }
        const unsigned char* h = reinterpret_cast<const unsigned char*>(hdr_buf.get());
        uint8_t version = h[0];
        (void)version; // unused currently
        uint8_t flags = h[1]; (void)flags;
        uint16_t stream = (h[2] << 8) | h[3];
        (void)stream; // unused currently
        auto opcode = static_cast<cql_binary_opcode>(h[4]);
        uint32_t len; std::memcpy(&len, h + 5, 4); len = ntohl(len);
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
        payload = bytes(bytes::initialized_later(), len);
        std::memcpy(payload.begin(), body.get(), len);
        co_return opcode;
    }

    future<> startup() {
        auto startup_stream = allocate_stream();
        frame_builder fb{startup_stream};
        // STARTUP frame body (v4): <map<string,string>> of options
        // map encodes with a <short n> for number of entries, then n*(<string><string>)
        fb.write_short(1); // one entry
        fb.write_string("CQL_VERSION");
        fb.write_string("3.0.0");
        co_await send_frame(fb.finish(cql_binary_opcode::STARTUP));
        bytes payload; auto op = co_await read_one_frame(payload);
        // If user supplied credentials we require the server to challenge with AUTHENTICATE.
        if (!_username.empty() && op != cql_binary_opcode::AUTHENTICATE) {
            throw std::runtime_error("--username specified but server did not request authentication (expected AUTHENTICATE frame)");
        }
        if (op == cql_binary_opcode::AUTHENTICATE) {
            // Assume PasswordAuthenticator; send SASL PLAIN (no need to inspect class name).
            frame_builder auth_fb{startup_stream}; // reuse same stream id per protocol spec
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
                auth_fb.body.write(plain.data(), plain.size());
            }
            co_await send_frame(auth_fb.finish(cql_binary_opcode::AUTH_RESPONSE));
            payload = bytes();
            op = co_await read_one_frame(payload);
        }
        if (op != cql_binary_opcode::READY && op != cql_binary_opcode::AUTH_SUCCESS) {
            // Try to decode ERROR for better diagnostics
            if (op == cql_binary_opcode::ERROR && payload.size() >= 4) {
                int32_t code = ntohl(*reinterpret_cast<const int32_t*>(payload.begin()));
                // message string follows: <string>
                if (payload.size() >= 6) {
                    auto p = payload.begin() + 4;
                    uint16_t slen = ntohs(*reinterpret_cast<const uint16_t*>(p));
                    p += 2;
                    sstring msg;
                    if (payload.size() >= 6 + slen) {
                        msg = sstring(reinterpret_cast<const char*>(p), slen);
                    }
                    throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got ERROR code={} msg='{}'", code, msg));
                }
            }
            throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got opcode {}", (int)op));
        }
        if (!_username.empty()) {
            // With credentials expect AUTH_SUCCESS explicitly.
            if (op != cql_binary_opcode::AUTH_SUCCESS) {
                throw std::runtime_error("authentication expected AUTH_SUCCESS but got different opcode");
            }
        }
    }

    future<> query_simple(std::string_view q) {
        // Serialize use of the underlying socket to avoid concurrent reads
        // on the same input stream which are not supported.
        co_await _use_sem.wait();
        auto releaser = seastar::defer([this] { _use_sem.signal(); });
        auto stream = allocate_stream();
        frame_builder fb{stream};
        // QUERY frame (v4): <long string><short consistency><byte flags>
        fb.write_long_string(q);
        fb.write_short(0x0001); // ONE
        fb.write_byte(0); // flags
        co_await send_frame(fb.finish(cql_binary_opcode::QUERY));
        bytes payload; auto op = co_await read_one_frame(payload);
        if (op == cql_binary_opcode::ERROR) {
            throw std::runtime_error(format("server returned ERROR to QUERY: {}", payload));
        }
    }

    future<sstring> prepare_query(std::string_view q) {
        co_await _use_sem.wait();
        auto releaser = seastar::defer([this] { _use_sem.signal(); });
        auto stream = allocate_stream();
        frame_builder fb{stream};
        fb.write_long_string(q);
        co_await send_frame(fb.finish(cql_binary_opcode::PREPARE));
        bytes payload;
        auto op = co_await read_one_frame(payload);
        if (op != cql_binary_opcode::RESULT) {
            throw std::runtime_error(fmt::format("expected RESULT for PREPARE, got {}", (int)op));
        }
        // RESULT body: [int kind][short id_len][bytes id]...
        if (payload.size() < 4) throw std::runtime_error("short RESULT body");
        int32_t kind = ntohl(*reinterpret_cast<const int32_t*>(payload.begin()));
        if (kind != 0x0004) { // PREPARED
            throw std::runtime_error(fmt::format("expected RESULT kind PREPARED (4), got {}", kind));
        }
        if (payload.size() < 6) throw std::runtime_error("short PREPARED body");
        uint16_t id_len = ntohs(*reinterpret_cast<const uint16_t*>(payload.begin() + 4));
        if (payload.size() < 6 + id_len) throw std::runtime_error("short PREPARED id");
        sstring id(reinterpret_cast<const char*>(payload.begin() + 6), id_len);
        co_return id;
    }

    future<> execute_prepared(const sstring& id, bytes_view key) {
        co_await _use_sem.wait();
        auto releaser = seastar::defer([this] { _use_sem.signal(); });
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
        fb.body.write(reinterpret_cast<const char*>(key.begin()), key.size());

        co_await send_frame(fb.finish(cql_binary_opcode::EXECUTE));
        bytes payload;
        auto op = co_await read_one_frame(payload);
        if (op == cql_binary_opcode::ERROR) {
            throw std::runtime_error("server returned ERROR to EXECUTE");
        }
    }

    future<> prepare_statements() {
        if (!_use_prepared) co_return;
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
    co_await c->startup();
    if (cfg.workload == "connect") {
        co_return;
    }
    co_await c->prepare_statements();
    co_await do_request(*c, cfg);
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
                    net::inet_address{cfg.remote_host}, (uint16_t)10000}).get();
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
static thread_local bool tl_initialized = false;
static thread_local semaphore tl_init_sem(1);

static future<> prepare_thread_connections(const raw_cql_test_config cfg) {
    if (tl_initialized) {
        co_return;
    }
    co_await tl_init_sem.wait();
    if (!tl_initialized) {
        try {
            tl_conns.reserve(cfg.concurrency);
            for (unsigned i = 0; i < cfg.concurrency; ++i) {
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
            tl_initialized = true;
        } catch (...) {
            tl_init_sem.signal();
            throw;
        }
    }
    tl_init_sem.signal();
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
            superuser_conn.startup().get();
            create_role_with_permissions(superuser_conn, non_superuser_name, non_superuser_password).get();
            ensure_schema(superuser_conn).get();
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
        conn->startup().get();
        if (!cfg.create_non_superuser) {
            ensure_schema(*conn).get();
        }
        conn->prepare_statements().get();
        for (uint64_t seq = 0; seq < cfg.partitions; ++seq) {
            conn->write_one(seq).get();
        }
        std::cout << "Pre-populated " << cfg.partitions << " partitions" << std::endl;
    } catch (...) {
        std::cerr << "Population failed: " << std::current_exception() << std::endl;
        throw;
    }
}

static void workload_main(raw_cql_test_config cfg) {
    fmt::print("Running test with config: {}\n", cfg);
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
        if (cfg.connection_per_request || cfg.workload == "connect") {
            co_await run_one_with_new_connection(cfg);
        } else {
            static thread_local size_t idx = 0;
            auto& c = *tl_conns[idx++ % tl_conns.size()];
            co_await do_request(c, cfg);
        }
    }, cfg.concurrency, cfg.duration_in_seconds, cfg.operations_per_shard, !cfg.continue_after_error);
    std::cout << aggregated_perf_results(results) << std::endl;
}

// Returns a function which launches a performance workload that
// talks to the embedded server over the native CQL protocol using
// handcrafted CQL binary frames (no driver). Similar to perf_alternator
// (runs inside the server process) and perf_simple_query (similar workload types), but
// exercises the full networking + protocol parsing path.
std::function<int(int, char**)> cql_raw(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config>)>* after_init_func) {
    return [=](int ac, char** av) -> int {
        raw_cql_test_config c;
        bpo::options_description opts_desc;
        opts_desc.add_options()
            ("workload", bpo::value<std::string>()->default_value("read"), "workload type: read|write|connect")
            ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
            ("duration", bpo::value<unsigned>()->default_value(5), "test duration seconds")
            ("operations-per-shard", bpo::value<unsigned>()->default_value(0), "fixed op count per shard")
            ("concurrency", bpo::value<unsigned>()->default_value(100), "connections per shard")
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
        c.concurrency = vm["concurrency"].as<unsigned>();
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
            return app.run(ac, av, [c = std::move(c)] () -> future<> {
                return seastar::async([c = std::move(c)] () {
                    workload_main(c);
                    exit(0);
                });
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
        return scylla_main(ac,av);
    };
}

} // namespace perf
