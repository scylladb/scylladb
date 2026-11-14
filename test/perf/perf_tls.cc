/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Measures TLS record encryption and decryption throughput through
// seastar::tls, with credentials built by utils::configure_tls_creds_builder,
// the way Scylla sets up client and internode encryption. Both ends run on
// one shard and talk over an in-memory pipe, so the numbers cover TLS and
// GnuTLS only, without the kernel network stack.
//
// GnuTLS picks its crypto implementations once, at library load time (see
// GNUTLS_CPUID_OVERRIDE), so comparing two settings takes two runs of this
// tool. dist/common/scripts/scylla_gnutls_setup does that.

#include <chrono>
#include <deque>
#include <fstream>

#include <boost/lexical_cast.hpp>
#include <fmt/ranges.h>
#include <gnutls/gnutls.h>
#include <gnutls/x509.h>
#include <json/json.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/net/stack.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include "db/config.hh"
#include "test/lib/tmpdir.hh"
#include "test/perf/perf.hh"

using namespace seastar;

static logging::logger plog("perf_tls");

namespace {

using clk = std::chrono::steady_clock;

// One direction of an in-memory connection. An empty buffer from pop() means EOF.
class mem_pipe {
    std::deque<temporary_buffer<char>> _q;
    std::optional<promise<>> _waiter;
    shared_promise<> _closed;
    bool _is_closed = false;
    bool _discard = false;
public:
    // Drops pushed data instead of queueing it.
    void set_discard(bool discard) { _discard = discard; }

    void push(temporary_buffer<char> b) {
        if (_discard || _is_closed || b.empty()) {
            return;
        }
        _q.push_back(std::move(b));
        wake();
    }

    future<temporary_buffer<char>> pop() {
        while (_q.empty() && !_is_closed) {
            _waiter.emplace();
            co_await _waiter->get_future();
        }
        if (_q.empty()) {
            co_return temporary_buffer<char>();
        }
        auto b = std::move(_q.front());
        _q.pop_front();
        co_return b;
    }

    void close() {
        if (!_is_closed) {
            _is_closed = true;
            _closed.set_value();
            wake();
        }
    }

    future<> wait_closed() { return _closed.get_shared_future(); }
private:
    void wake() {
        if (_waiter) {
            std::exchange(_waiter, std::nullopt)->set_value();
        }
    }
};

class mem_sink : public data_sink_impl {
    lw_shared_ptr<mem_pipe> _p;
public:
    explicit mem_sink(lw_shared_ptr<mem_pipe> p) : _p(std::move(p)) {}
    future<> put(std::span<temporary_buffer<char>> bufs) override {
        for (auto& b : bufs) {
            _p->push(std::move(b));
        }
        return make_ready_future<>();
    }
    future<> close() override {
        _p->close();
        return make_ready_future<>();
    }
};

class mem_source : public data_source_impl {
    lw_shared_ptr<mem_pipe> _p;
public:
    explicit mem_source(lw_shared_ptr<mem_pipe> p) : _p(std::move(p)) {}
    future<temporary_buffer<char>> get() override { return _p->pop(); }
    future<> close() override {
        _p->close();
        return make_ready_future<>();
    }
};

class mem_socket : public net::connected_socket_impl {
    lw_shared_ptr<mem_pipe> _tx;
    lw_shared_ptr<mem_pipe> _rx;
public:
    mem_socket(lw_shared_ptr<mem_pipe> tx, lw_shared_ptr<mem_pipe> rx) : _tx(std::move(tx)), _rx(std::move(rx)) {}
    data_source source() override { return data_source(std::make_unique<mem_source>(_rx)); }
    data_sink sink() override { return data_sink(std::make_unique<mem_sink>(_tx)); }
    void shutdown_input() override { _rx->close(); }
    void shutdown_output() override { _tx->close(); }
    void set_nodelay(bool) override {}
    bool get_nodelay() const override { return true; }
    void set_keepalive(bool) override {}
    bool get_keepalive() const override { return false; }
    void set_keepalive_parameters(const net::keepalive_params&) override {}
    net::keepalive_params get_keepalive_parameters() const override { return net::tcp_keepalive_params{}; }
    void set_sockopt(int, int, const void*, size_t) override {
        throw std::runtime_error("mem_socket does not support socket options");
    }
    int get_sockopt(int, int, void*, size_t) const override {
        throw std::runtime_error("mem_socket does not support socket options");
    }
    socket_address local_address() const noexcept override { return {}; }
    socket_address remote_address() const noexcept override { return {}; }
    future<> wait_input_shutdown() override { return _rx->wait_closed(); }
};

void gnutls_check(int r, const char* what) {
    if (r < 0) {
        throw std::runtime_error(fmt::format("{}: {}", what, gnutls_strerror(r)));
    }
}

sstring export_datum(int (*fn)(void*, gnutls_x509_crt_fmt_t, gnutls_datum_t*), void* obj, const char* what) {
    gnutls_datum_t d;
    gnutls_check(fn(obj, GNUTLS_X509_FMT_PEM, &d), what);
    auto free_d = defer([&] () noexcept { gnutls_free(d.data); });
    return sstring(reinterpret_cast<const char*>(d.data), d.size);
}

const char* const server_name = "perf-tls";

// Writes a fresh self-signed certificate and its key as PEM files.
void generate_certificate(const fs::path& cert_file, const fs::path& key_file, gnutls_pk_algorithm_t pk, unsigned bits) {
    gnutls_x509_privkey_t key;
    gnutls_check(gnutls_x509_privkey_init(&key), "gnutls_x509_privkey_init");
    auto free_key = defer([&] () noexcept { gnutls_x509_privkey_deinit(key); });
    gnutls_check(gnutls_x509_privkey_generate(key, pk, bits, 0), "gnutls_x509_privkey_generate");

    gnutls_x509_crt_t crt;
    gnutls_check(gnutls_x509_crt_init(&crt), "gnutls_x509_crt_init");
    auto free_crt = defer([&] () noexcept { gnutls_x509_crt_deinit(crt); });
    auto now = ::time(nullptr);
    const unsigned char serial = 1;
    gnutls_check(gnutls_x509_crt_set_version(crt, 3), "set_version");
    gnutls_check(gnutls_x509_crt_set_serial(crt, &serial, 1), "set_serial");
    gnutls_check(gnutls_x509_crt_set_activation_time(crt, now - 3600), "set_activation_time");
    gnutls_check(gnutls_x509_crt_set_expiration_time(crt, now + 86400), "set_expiration_time");
    gnutls_check(gnutls_x509_crt_set_dn_by_oid(crt, GNUTLS_OID_X520_COMMON_NAME, 0, server_name, strlen(server_name)), "set_dn");
    gnutls_check(gnutls_x509_crt_set_subject_alt_name(crt, GNUTLS_SAN_DNSNAME, server_name, strlen(server_name), GNUTLS_FSAN_SET), "set_san");
    gnutls_check(gnutls_x509_crt_set_key_usage(crt, GNUTLS_KEY_DIGITAL_SIGNATURE | GNUTLS_KEY_KEY_ENCIPHERMENT), "set_key_usage");
    gnutls_check(gnutls_x509_crt_set_key(crt, key), "set_key");
    gnutls_check(gnutls_x509_crt_sign2(crt, crt, key, GNUTLS_DIG_SHA256, 0), "sign");

    std::ofstream(cert_file) << export_datum([] (void* o, gnutls_x509_crt_fmt_t f, gnutls_datum_t* d) {
        return gnutls_x509_crt_export2(static_cast<gnutls_x509_crt_t>(o), f, d);
    }, crt, "gnutls_x509_crt_export2");
    std::ofstream(key_file) << export_datum([] (void* o, gnutls_x509_crt_fmt_t f, gnutls_datum_t* d) {
        return gnutls_x509_privkey_export2(static_cast<gnutls_x509_privkey_t>(o), f, d);
    }, key, "gnutls_x509_privkey_export2");
}

struct credentials {
    shared_ptr<tls::server_credentials> server;
    shared_ptr<tls::certificate_credentials> client;
};

future<credentials> make_credentials(db::config::string_map options) {
    tls::credentials_builder b;
    co_await utils::configure_tls_creds_builder(b, std::move(options));
    co_return credentials{b.build_server_credentials(), b.build_certificate_credentials()};
}

// A TLS connection whose two ends live on this shard.
struct connection {
    lw_shared_ptr<mem_pipe> c2s = make_lw_shared<mem_pipe>();
    lw_shared_ptr<mem_pipe> s2c = make_lw_shared<mem_pipe>();
    connected_socket client;
    connected_socket server;
    data_sink client_out;
    std::optional<input_stream<char>> client_in;
    data_sink server_out;
    std::optional<input_stream<char>> server_in;
    sstring cipher_suite;
    sstring protocol;

    future<> handshake(const credentials& creds) {
        tls::tls_options opts;
        opts.server_name = server_name;
        // Close the client without a TLS bye, so close() can't wait on the peer.
        opts.bye_timeout = std::chrono::seconds(0);
        client = co_await tls::wrap_client(creds.client, connected_socket(std::make_unique<mem_socket>(c2s, s2c)), opts);
        server = co_await tls::wrap_server(creds.server, connected_socket(std::make_unique<mem_socket>(s2c, c2s)));
        // Each call forces the handshake on its end; both must run at once.
        auto [cs, ss] = co_await when_all_succeed(tls::get_cipher_suite(client), tls::get_cipher_suite(server));
        if (cs != ss) {
            throw std::runtime_error(fmt::format("client and server disagree on the cipher suite: {} vs {}", cs, ss));
        }
        cipher_suite = cs;
        protocol = co_await tls::get_protocol_version(server);
        client_out = client.output().detach();
        client_in.emplace(client.input());
        server_out = server.output().detach();
        server_in.emplace(server.input());
    }

    future<> close() noexcept {
        auto ignore = [] (future<> f) { f.ignore_ready_future(); };
        if (client_in) {
            co_await client_out.close().then_wrapped(ignore);
            co_await client_in->close().then_wrapped(ignore);
        }
        if (server_in) {
            co_await server_out.close().then_wrapped(ignore);
            co_await server_in->close().then_wrapped(ignore);
        }
    }
};

// Reads exactly n bytes, appending them to `out` if given.
future<> read_exactly(input_stream<char>& in, size_t n, std::vector<char>* out = nullptr) {
    while (n) {
        auto b = co_await in.read_up_to(n);
        if (b.empty()) {
            throw std::runtime_error("unexpected EOF");
        }
        if (out) {
            out->insert(out->end(), b.begin(), b.end());
        }
        n -= b.size();
    }
}

temporary_buffer<char> make_payload(size_t size, unsigned seed) {
    temporary_buffer<char> b(size);
    for (size_t i = 0; i < size; ++i) {
        b.get_write()[i] = char((i * 131 + seed * 7 + (i >> 8)) & 0xff);
    }
    return b;
}

// Sends payloads of assorted sizes, including ones that span several records,
// both ways, and checks that they arrive intact.
future<> verify_round_trip(connection& c) {
    unsigned seed = 0;
    for (size_t size : {1, 13, 1000, 8192, 16383, 16384, 16385, 2 * 16384 + 7, 100000}) {
        for (auto [out, in] : {std::pair{&c.client_out, &*c.server_in}, std::pair{&c.server_out, &*c.client_in}}) {
            auto payload = make_payload(size, ++seed);
            co_await out->put(payload.share());
            std::vector<char> got;
            co_await read_exactly(*in, size, &got);
            if (!std::equal(got.begin(), got.end(), payload.begin(), payload.end())) {
                throw std::runtime_error(fmt::format("data corrupted in a round trip of {} bytes", size));
            }
        }
    }
}

struct run_config {
    double duration;
    unsigned repeat;
};

// Calls batch() until the time it reports adds up to cfg.duration, cfg.repeat
// times, and returns the throughput of each round.
template <typename Batch>
future<std::vector<double>> measure(const run_config& cfg, Batch batch) {
    co_await batch(); // warm-up
    std::vector<double> runs;
    for (unsigned r = 0; r < cfg.repeat; ++r) {
        clk::duration busy{};
        size_t bytes = 0;
        while (busy < std::chrono::duration<double>(cfg.duration)) {
            auto [t, b] = co_await batch();
            busy += t;
            bytes += b;
        }
        runs.push_back(bytes / std::chrono::duration<double>(busy).count() / 1e6);
    }
    co_return runs;
}

double median(std::vector<double> v) {
    std::ranges::sort(v);
    return v[v.size() / 2];
}

constexpr size_t batch_bytes = 4 << 20;

// Encryption: the client puts one record per call, the server never reads.
// The dropped records leave the server behind in the record sequence, so this
// runs on a connection of its own.
future<std::vector<double>> bench_encrypt(const credentials& creds, size_t size, const run_config& cfg) {
    auto payload = make_payload(size, 0);
    size_t n = std::max<size_t>(1, batch_bytes / size);
    connection c;
    std::exception_ptr ex;
    std::vector<double> runs;
    try {
        co_await c.handshake(creds);
        c.c2s->set_discard(true);
        runs = co_await measure(cfg, [&] () -> future<std::pair<clk::duration, size_t>> {
            auto start = clk::now();
            for (size_t i = 0; i < n; ++i) {
                co_await c.client_out.put(payload.share());
                co_await coroutine::maybe_yield();
            }
            co_return std::pair(clk::now() - start, n * size);
        });
    } catch (...) {
        ex = std::current_exception();
    }
    co_await c.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return runs;
}

// Decryption: the client queues a batch of records untimed, the server reads them.
future<std::vector<double>> bench_decrypt(connection& c, size_t size, const run_config& cfg) {
    auto payload = make_payload(size, 0);
    size_t n = std::max<size_t>(1, batch_bytes / size);
    co_return co_await measure(cfg, [&] () -> future<std::pair<clk::duration, size_t>> {
        for (size_t i = 0; i < n; ++i) {
            co_await c.client_out.put(payload.share());
            co_await coroutine::maybe_yield();
        }
        auto start = clk::now();
        size_t left = n * size;
        while (left) {
            auto b = co_await c.server_in->read_up_to(left);
            if (b.empty()) {
                throw std::runtime_error("unexpected EOF");
            }
            left -= b.size();
            co_await coroutine::maybe_yield();
        }
        co_return std::pair(clk::now() - start, n * size);
    });
}

// Full handshakes per second (no session resumption).
future<double> bench_handshakes(const credentials& creds, double duration) {
    clk::duration busy{};
    size_t n = 0;
    while (busy < std::chrono::duration<double>(duration)) {
        connection c;
        auto start = clk::now();
        co_await c.handshake(creds);
        busy += clk::now() - start;
        ++n;
        co_await c.close();
    }
    co_return n / std::chrono::duration<double>(busy).count();
}

std::vector<sstring> split(const sstring& s) {
    std::vector<sstring> r;
    for (auto&& part : std::views::split(std::string_view(s), ',')) {
        if (!part.empty()) {
            r.emplace_back(part.begin(), part.end());
        }
    }
    return r;
}

// "AES-256-GCM" -> "AES_256_GCM", the spelling of cipher suite names.
sstring suite_spelling(sstring cipher) {
    std::ranges::replace(cipher, '-', '_');
    return cipher;
}

// Benchmarks one cipher on a fresh connection, appending to `results`.
future<> bench_cipher(connection& c, const credentials& creds, const sstring& cipher, const std::vector<size_t>& sizes,
        const run_config& rc, Json::Value& results) {
    co_await c.handshake(creds);
    if (cipher != "default" && c.cipher_suite.find(suite_spelling(cipher)) == sstring::npos) {
        throw std::runtime_error(fmt::format("asked for {}, negotiated {}", cipher, c.cipher_suite));
    }
    co_await verify_round_trip(c);
    for (auto size : sizes) {
        for (auto op : {"enc", "dec"}) {
            auto runs = op == std::string_view("enc") ? co_await bench_encrypt(creds, size, rc) : co_await bench_decrypt(c, size, rc);
            auto m = median(runs);
            fmt::print("{:<14} {:<24} {:>4} {:>7} {:>10.1f}  {:.1f}\n", cipher, c.cipher_suite, op, size, m, fmt::join(runs, " "));
            Json::Value r;
            r["cipher"] = std::string(cipher);
            r["suite"] = std::string(c.cipher_suite);
            r["protocol"] = std::string(c.protocol);
            r["op"] = op;
            r["size"] = Json::UInt64(size);
            r["mbps"] = m;
            for (auto v : runs) {
                r["runs"].append(v);
            }
            results.append(std::move(r));
        }
    }
    // The round trip must still work after the benchmark pushed a lot of
    // records through the session.
    co_await verify_round_trip(c);
}

future<int> run(const boost::program_options::variables_map& cfg) {
    auto sizes = split(cfg["sizes"].as<sstring>()) | std::views::transform([] (const sstring& s) {
        return boost::lexical_cast<size_t>(s);
    }) | std::ranges::to<std::vector>();
    auto ciphers = split(cfg["ciphers"].as<sstring>());
    run_config rc{cfg["duration"].as<double>(), cfg["repeat"].as<unsigned>()};
    if (rc.repeat == 0) {
        throw std::invalid_argument("--repeat must be positive");
    }

    tmpdir dir;
    db::config::string_map options;
    if (cfg.contains("certificate")) {
        options["certificate"] = cfg["certificate"].as<sstring>();
        options["keyfile"] = cfg["keyfile"].as<sstring>();
        if (cfg.contains("truststore")) {
            options["truststore"] = cfg["truststore"].as<sstring>();
        }
    } else {
        auto key_type = cfg["key-type"].as<sstring>();
        gnutls_pk_algorithm_t pk;
        unsigned bits;
        if (key_type == "rsa") {
            pk = GNUTLS_PK_RSA;
            bits = cfg["key-bits"].as<unsigned>();
        } else if (key_type == "ecdsa") {
            pk = GNUTLS_PK_ECDSA;
            bits = GNUTLS_CURVE_TO_BITS(GNUTLS_ECC_CURVE_SECP256R1);
        } else {
            throw std::invalid_argument(fmt::format("unknown --key-type: {}", key_type));
        }
        auto cert = dir.path() / "perf-tls.crt";
        auto key = dir.path() / "perf-tls.key";
        generate_certificate(cert, key, pk, bits);
        options["certificate"] = cert.native();
        options["keyfile"] = key.native();
        options["truststore"] = cert.native();
    }
    auto priority = cfg["priority-string"].as<sstring>();

    auto override_env = ::getenv("GNUTLS_CPUID_OVERRIDE");
    fmt::print("gnutls {}, GNUTLS_CPUID_OVERRIDE={}\n", gnutls_check_version(nullptr), override_env ? override_env : "(unset)");
    fmt::print("{:<14} {:<24} {:>4} {:>7} {:>10}  {}\n", "cipher", "suite", "op", "size", "MB/s", "runs");

    Json::Value json;
    json["gnutls_version"] = gnutls_check_version(nullptr);
    json["cpuid_override"] = override_env ? Json::Value(override_env) : Json::Value();
    json["priority_string"] = std::string(priority);
    auto& results = json["results"] = Json::Value(Json::arrayValue);

    std::optional<credentials> last_creds;
    for (auto& cipher : ciphers) {
        auto o = options;
        o["priority_string"] = cipher == "default" ? priority : sstring(fmt::format("{}:-CIPHER-ALL:+{}", priority, cipher));
        auto creds = co_await make_credentials(std::move(o));
        connection c;
        std::exception_ptr ex;
        try {
            co_await bench_cipher(c, creds, cipher, sizes, rc, results);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await c.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        last_creds = std::move(creds);
    }

    if (auto d = cfg["handshake-duration"].as<double>(); d > 0 && last_creds) {
        auto hs = co_await bench_handshakes(*last_creds, d);
        fmt::print("handshakes/s: {:.0f}\n", hs);
        json["handshakes_per_sec"] = hs;
    }
    json["verified"] = true;

    if (cfg.contains("json-result")) {
        auto path = cfg["json-result"].as<sstring>();
        std::ofstream f(path);
        std::unique_ptr<Json::StreamWriter>(Json::StreamWriterBuilder().newStreamWriter())->write(json, &f);
        f << "\n";
        if (!f) {
            throw std::runtime_error(fmt::format("failed to write {}", path));
        }
    }
    co_return 0;
}

} // anonymous namespace

namespace perf {

int scylla_tls_main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
            ("sizes", bpo::value<sstring>()->default_value("128,1024,8192,16384"), "Comma-separated record payload sizes in bytes")
            ("ciphers", bpo::value<sstring>()->default_value("AES-256-GCM,AES-128-GCM"),
                    "Comma-separated GnuTLS cipher names to restrict the priority string to, one at a time; "
                    "\"default\" leaves the priority string as is")
            ("priority-string", bpo::value<sstring>()->default_value(db::config::default_tls_priority),
                    "GnuTLS priority string, as in client_encryption_options/server_encryption_options")
            ("duration", bpo::value<double>()->default_value(0.5), "Seconds of measurement per round")
            ("repeat", bpo::value<unsigned>()->default_value(3), "Rounds per case; the median is reported")
            ("handshake-duration", bpo::value<double>()->default_value(0.5), "Seconds of full handshakes to measure; 0 skips it")
            ("key-type", bpo::value<sstring>()->default_value("rsa"), "Type of the generated key: rsa or ecdsa (P-256)")
            ("key-bits", bpo::value<unsigned>()->default_value(2048), "RSA key size of the generated key")
            ("certificate", bpo::value<sstring>(), "PEM certificate to use instead of a generated one; needs --keyfile")
            ("keyfile", bpo::value<sstring>(), "PEM key for --certificate")
            ("truststore", bpo::value<sstring>(), "PEM trust file that verifies --certificate")
            ("json-result", bpo::value<sstring>(), "Write the results as JSON to this file")
            ("verbose", "Enables standard logging")
            ;
    return app.run(argc, argv, [&app] () -> future<int> {
        if (!app.configuration().contains("verbose")) {
            logging::logger_registry().set_all_loggers_level(seastar::log_level::warn);
        }
        if (app.configuration().contains("certificate") != app.configuration().contains("keyfile")) {
            fmt::print(std::cerr, "--certificate and --keyfile go together\n");
            co_return 2;
        }
        try {
            co_return co_await run(app.configuration());
        } catch (...) {
            plog.error("{}", std::current_exception());
            co_return 1;
        }
    });
}

} // namespace perf
