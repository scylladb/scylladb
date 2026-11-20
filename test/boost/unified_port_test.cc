/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/posix-stack.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/net/tls.hh>

#include <cstring>
#include <fcntl.h>

using namespace seastar;

// Unified port shard selection header format:
// Byte 0: Magic (0xAA)
// Byte 1: Flags (bit 0 = TLS)
// Byte 2-3: Shard ID (big-endian uint16_t)

static constexpr uint8_t unified_port_magic = 0xAA;
static constexpr uint8_t shard_select_flag_tls = 0x01;

static std::vector<char> make_shard_selection_header(uint16_t shard_id, bool tls = false) {
    std::vector<char> header(4);
    header[0] = static_cast<char>(unified_port_magic);
    header[1] = tls ? shard_select_flag_tls : 0;
    header[2] = static_cast<char>((shard_id >> 8) & 0xFF);
    header[3] = static_cast<char>(shard_id & 0xFF);
    return header;
}

static std::vector<char> make_pp2_header(uint32_t src_ip = 0x7F000001, uint16_t src_port = 12345,
                                          uint32_t dst_ip = 0x7F000001, uint16_t dst_port = 9042) {
    static const char pp2_sig[12] = {
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };
    std::vector<char> header;
    header.insert(header.end(), pp2_sig, pp2_sig + 12);
    // Version/Command: v2 PROXY
    header.push_back(0x21);
    // Family/Protocol: AF_INET/STREAM
    header.push_back(0x11);
    // Address length: 12 bytes for IPv4
    header.push_back(0x00);
    header.push_back(0x0C);
    // Source IP (big-endian)
    header.push_back(static_cast<char>((src_ip >> 24) & 0xFF));
    header.push_back(static_cast<char>((src_ip >> 16) & 0xFF));
    header.push_back(static_cast<char>((src_ip >> 8) & 0xFF));
    header.push_back(static_cast<char>(src_ip & 0xFF));
    // Dest IP (big-endian)
    header.push_back(static_cast<char>((dst_ip >> 24) & 0xFF));
    header.push_back(static_cast<char>((dst_ip >> 16) & 0xFF));
    header.push_back(static_cast<char>((dst_ip >> 8) & 0xFF));
    header.push_back(static_cast<char>(dst_ip & 0xFF));
    // Source port (big-endian)
    header.push_back(static_cast<char>((src_port >> 8) & 0xFF));
    header.push_back(static_cast<char>(src_port & 0xFF));
    // Dest port (big-endian)
    header.push_back(static_cast<char>((dst_port >> 8) & 0xFF));
    header.push_back(static_cast<char>(dst_port & 0xFF));
    return header;
}

static std::vector<char> make_cql_options_frame() {
    std::vector<char> frame(9);
    frame[0] = 0x04; // version
    frame[1] = 0x00; // flags
    frame[2] = 0x00; frame[3] = 0x01; // stream
    frame[4] = 0x05; // opcode: OPTIONS
    frame[5] = frame[6] = frame[7] = frame[8] = 0; // length = 0
    return frame;
}

// Enable PPv2 acceptance from any physical peer for protocol parser tests.
static unified_port_options proxy_enabled_options() {
    unified_port_options options;
    options.proxy_protocol = unified_port_options::proxy_protocol_mode::optional;
    options.trusted_proxy = [] (const net::inet_address&) { return true; };
    return options;
}

// Listen on port 0 with unified_port enabled, return the server_socket and its actual port.
static std::pair<server_socket, uint16_t> listen_unified_port_on_ipv4(unified_port_options options = proxy_enabled_options()) {
    listen_options lo;
    lo.reuse_address = true;
    lo.unified_port = std::move(options);
    auto ss = seastar::listen(make_ipv4_address({0}), lo);
    auto port = ss.local_address().as_posix_sockaddr_in().sin_port;
    port = ntohs(port);
    return {std::move(ss), port};
}

// Retain a listener when a failed cancellation leaves its accept pending. The
// completion observer owns the same socket, so no callback outlives its owner.
class retained_listener final : public net::server_socket_impl {
    lw_shared_ptr<server_socket> _listener;
public:
    explicit retained_listener(lw_shared_ptr<server_socket> listener) : _listener(std::move(listener)) {}
    future<accept_result> accept() override { return _listener->accept(); }
    void abort_accept() override { _listener->abort_accept(); }
    socket_address local_address() const override { return _listener->local_address(); }
};

// Bound both acceptance and cancellation; keep the listener alive if abort fails.
static accept_result accept_with_deadline(server_socket& ss, std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
    auto listener = make_lw_shared<server_socket>(std::move(ss));
    shared_future<lw_shared_ptr<accept_result>> accepted(listener->accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    try {
        auto result = with_timeout(lowres_clock::now() + timeout, accepted.get_future()).get();
        ss = std::move(*listener);
        return std::move(*result);
    } catch (...) {
        auto error = std::current_exception();
        listener->abort_accept();
        try {
            with_timeout(lowres_clock::now() + timeout, accepted.get_future()).get();
            ss = std::move(*listener);
        } catch (const timed_out_error&) {
            // The callback owns every object it uses, including the listener.
            (void)accepted.get_future().then_wrapped([listener] (auto result) noexcept {
                result.ignore_ready_future();
            });
            ss = server_socket(std::make_unique<retained_listener>(std::move(listener)));
        } catch (...) {
            ss = std::move(*listener);
        }
        std::rethrow_exception(error);
    }
}

// Cleanup never waits for cancellation. If it fails, the completion observer
// keeps the listener alive until its pending accept finishes.
template <typename... Result>
static void observe_accept_completion(server_socket& ss, shared_future<Result...> accepted) noexcept {
    auto pending = accepted.get_future();
    if (pending.available()) {
        pending.ignore_ready_future();
        return;
    }
    if (ss) {
        ss.abort_accept();
    }
    auto listener = make_lw_shared<server_socket>(std::move(ss));
    if (*listener) {
        ss = server_socket(std::make_unique<retained_listener>(listener));
    }
    (void)std::move(pending).then_wrapped([listener] (auto result) noexcept {
        result.ignore_ready_future();
    });
}

// A timed-out read owns its connection and stream until cancellation finishes.
static temporary_buffer<char> read_with_deadline(connected_socket& socket, size_t size,
        std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
    auto connection = make_lw_shared<connected_socket>(std::move(socket));
    auto input = make_lw_shared<input_stream<char>>(connection->input());
    auto reading = input->read_exactly(size).then([connection, input] (auto bytes) { return bytes; });
    try {
        auto bytes = with_timeout(lowres_clock::now() + timeout, std::move(reading)).get();
        socket = std::move(*connection);
        return bytes;
    } catch (...) {
        connection->shutdown_input();
        throw;
    }
}

// Shutdown cancels both connect and established I/O on the owned POSIX socket.
// A late worker completion owns all of its sockets, streams and payload bytes.
class sending_client {
    lw_shared_ptr<seastar::socket> _socket;
    shared_future<> _completed;
public:
    sending_client(seastar::socket socket, socket_address address, std::vector<char> bytes)
        : _socket(make_lw_shared<seastar::socket>(std::move(socket)))
        , _completed(async([socket = _socket, address, bytes = std::move(bytes)] {
            auto connection = socket->connect(address).get();
            auto out = connection.output();
            auto close_output = defer([&out] () noexcept { try { out.close().get(); } catch (...) {} });
            out.write(bytes.data(), bytes.size()).get();
            out.flush().get();
            out.close().get();
        })) {}

    sending_client(socket_address address, std::vector<char> bytes)
        : sending_client(engine().net().socket(), address, std::move(bytes)) {}

    sending_client(const sending_client&) = delete;
    sending_client& operator=(const sending_client&) = delete;

    ~sending_client() {
        try { _socket->shutdown(); } catch (...) {}
        (void)_completed.get_future().then_wrapped([socket = _socket] (auto result) noexcept {
            result.ignore_ready_future();
        });
    }

    void wait(std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
        with_timeout(lowres_clock::now() + timeout, _completed.get_future()).get();
    }
};

SEASTAR_THREAD_TEST_CASE(test_shard_selection_header_format) {
    auto hdr = make_shard_selection_header(0, false);
    BOOST_REQUIRE_EQUAL(hdr.size(), 4u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[0]), unified_port_magic);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[1]), 0x00);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[2]), 0x00);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[3]), 0x00);

    hdr = make_shard_selection_header(3, true);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[1]), shard_select_flag_tls);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[2]), 0x00);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[3]), 0x03);

    hdr = make_shard_selection_header(256, false);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[2]), 0x01);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[3]), 0x00);

    hdr = make_shard_selection_header(0xFFFF, false);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[2]), 0xFF);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[3]), 0xFF);
}

SEASTAR_THREAD_TEST_CASE(test_pp2_header_format) {
    auto hdr = make_pp2_header();
    BOOST_REQUIRE_EQUAL(hdr.size(), 28u);
    static const char pp2_sig[12] = {
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };
    BOOST_REQUIRE(std::memcmp(hdr.data(), pp2_sig, 12) == 0);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[12]), 0x21); // v2 PROXY
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[13]), 0x11); // AF_INET/STREAM
}

SEASTAR_THREAD_TEST_CASE(test_first_byte_detection) {
    // PP v2: first byte is 0x0D
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(make_pp2_header()[0]), 0x0D);

    // Shard selection: first byte is 0xAA
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(make_shard_selection_header(0)[0]), unified_port_magic);

    // CQL frame: first byte is version (0x03 or 0x04)
    auto options = make_cql_options_frame();
    BOOST_REQUIRE(static_cast<uint8_t>(options[0]) >= 0x03 &&
                  static_cast<uint8_t>(options[0]) <= 0x05);

    // TLS ClientHello: first byte is 0x16
    uint8_t tls_first = 0x16;
    BOOST_REQUIRE_EQUAL(tls_first, 0x16);

    // All four first-byte values are distinct
    BOOST_REQUIRE_NE(0x0D, unified_port_magic);
    BOOST_REQUIRE_NE(0x0D, 0x16);
    BOOST_REQUIRE_NE(0x0D, 0x04);
    BOOST_REQUIRE_NE(unified_port_magic, 0x16);
    BOOST_REQUIRE_NE(unified_port_magic, 0x04);
    BOOST_REQUIRE_NE(0x16, 0x04);
}

SEASTAR_THREAD_TEST_CASE(test_listen_options_unified_port) {
    listen_options lo;
    BOOST_REQUIRE(!lo.unified_port);
    lo.unified_port.emplace();
    BOOST_REQUIRE(lo.unified_port.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_listen_options_mutual_exclusion) {
    listen_options lo;
    lo.unified_port.emplace();
    lo.proxy_protocol = true;
    BOOST_REQUIRE_THROW(seastar::listen(make_ipv4_address({0}), lo), std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(test_accept_result_metadata) {
    connection_metadata meta;
    BOOST_REQUIRE_EQUAL(meta.is_tls, false);
    meta.is_tls = true;
    BOOST_REQUIRE_EQUAL(meta.is_tls, true);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_accept_plain_cql) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1u);
    auto [ss, port] = listen_unified_port_on_ipv4();

    sending_client client(make_ipv4_address({"127.0.0.1", port}), make_cql_options_frame());

    auto ar = accept_with_deadline(ss);
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    // The CQL connection creates a stream before TLS wrapping creates another.
    // An unread stream must leave the detected byte available to the next one.
    auto unused = ar.connection.input();
    auto buf = read_with_deadline(ar.connection, 9);
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.wait();
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_accept_shard_selection) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1u);
    auto [ss, port] = listen_unified_port_on_ipv4();

    auto bytes = make_shard_selection_header(0, false);
    auto options = make_cql_options_frame();
    bytes.insert(bytes.end(), options.begin(), options.end());
    sending_client client(make_ipv4_address({"127.0.0.1", port}), std::move(bytes));

    auto ar = accept_with_deadline(ss);
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto buf = read_with_deadline(ar.connection, 9);
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.wait();
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_accept_plain_cql_ipv6) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1u);
    listen_options lo;
    lo.reuse_address = true;
    lo.unified_port.emplace();
    auto loopback6 = net::inet_address("::1");
    auto ss = seastar::listen(socket_address(loopback6, 0), lo);
    auto port = ntohs(ss.local_address().as_posix_sockaddr_in6().sin6_port);

    sending_client client(socket_address(loopback6, port), make_cql_options_frame());

    auto ar = accept_with_deadline(ss);
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto buf = read_with_deadline(ar.connection, 9);
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.wait();
    ss.abort_accept();
}

// --- Negative tests ---

// Observe rejection before a good client exists, then check the accepted bytes.
static void verify_bad_client_dropped(server_socket& ss, uint16_t port,
        noncopyable_function<void(output_stream<char>&)> bad_fn,
        std::vector<char> good_bytes = make_cql_options_frame(), bool tls = false,
        std::vector<char> expected = make_cql_options_frame()) {
    BOOST_REQUIRE_EQUAL(this_smp_shard_count(), 1u);
    auto listener = make_lw_shared<server_socket>(std::move(ss));
    shared_future<lw_shared_ptr<accept_result>> accepted(listener->accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto cleanup = defer([listener, accepted] () mutable noexcept {
        listener->abort_accept();
        (void)accepted.get_future().then_wrapped([listener] (auto result) noexcept {
            result.ignore_ready_future();
        });
    });
    auto bad = make_lw_shared<connected_socket>(seastar::connect(make_ipv4_address({"127.0.0.1", port})).get());
    shared_future<> peer_closed(bad->wait_input_shutdown());
    auto close_peer = defer([bad, peer_closed] () mutable noexcept {
        bad->shutdown_input();
        try {
            with_timeout(lowres_clock::now() + std::chrono::seconds(1), peer_closed.get_future()).get();
        } catch (...) {
            (void)peer_closed.get_future().then_wrapped([bad] (auto result) noexcept {
                result.ignore_ready_future();
            });
        }
    });
    auto out = bad->output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    try {
        bad_fn(out);
        out.flush().get();
        out.close().get();
    } catch (const std::system_error&) {
        // Immediate rejection may reset the peer before its write completes.
    }
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), peer_closed.get_future()).get();
    BOOST_REQUIRE(!accepted.available());

    auto good = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto good_out = good.output();
    good_bytes.push_back('G');
    expected.push_back('G');
    good_out.write(good_bytes.data(), good_bytes.size()).get();
    good_out.close().get();
    auto result = with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get();
    BOOST_REQUIRE_EQUAL(result->metadata.is_tls, tls);
    auto bytes = read_with_deadline(result->connection, expected.size());
    BOOST_REQUIRE_EQUAL(bytes.size(), expected.size());
    BOOST_REQUIRE(std::equal(bytes.begin(), bytes.end(), expected.begin()));
    listener->abort_accept();
    ss = std::move(*listener);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_truncated_shard_header) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // Only magic + 1 byte (incomplete 4-byte header)
        char partial[2] = {static_cast<char>(unified_port_magic), 0x00};
        out.write(partial, 2).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_invalid_pp2_signature) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        auto header = make_pp2_header();
        // Leave command, family and addresses valid; corrupt only the signature.
        header[1] ^= 0x01;
        auto frame = make_cql_options_frame();
        out.write(header.data(), header.size()).get();
        out.write(frame.data(), frame.size()).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_rejection_helper_handles_unexpected_acceptance) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    // A valid CQL connection keeps the peer open. The rejection helper must
    // report its deadline and release the pending close wait safely.
    BOOST_REQUIRE_THROW(verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        auto frame = make_cql_options_frame();
        out.write(frame.data(), frame.size()).get();
    }), timed_out_error);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_truncated_pp2_header) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // PP v2 signature (12 bytes) + 2 more bytes = 14 bytes total (need 16)
        static const char pp2_sig[12] = {
            0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
        };
        out.write(pp2_sig, 12).get();
        char extra[2] = {0x21, 0x11};
        out.write(extra, 2).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_pp2_unsupported_address_family) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        static const char pp2_sig[12] = {
            0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
        };
        out.write(pp2_sig, 12).get();
        // v2 PROXY, AF_UNIX/STREAM (family=3, proto=1 -> 0x31), length=216
        char cmd_fam_len[4] = {0x21, 0x31, 0x00, static_cast<char>(216u & 0xFF)};
        out.write(cmd_fam_len, 4).get();
        // Pad with zeros for the address payload
        char zeros[216] = {};
        out.write(zeros, 216).get();
        // A valid next protocol byte prevents EOF from masking acceptance.
        auto frame = make_cql_options_frame();
        out.write(frame.data(), frame.size()).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_reserved_flags_rejected) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // Magic 0xAA, flags=0x02 (reserved bit 1 set), shard=0
        char hdr[4] = {static_cast<char>(unified_port_magic), 0x02, 0x00, 0x00};
        out.write(hdr, 4).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_pp2_missing_declared_payload) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        static const char pp2_sig[12] = {
            0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
        };
        out.write(pp2_sig, 12).get();
        // Declare a valid 4095-byte payload but send none, exercising EOF in the payload read.
        char cmd_fam_len[4] = {0x21, 0x11, 0x0F, static_cast<char>(0xFF)};
        out.write(cmd_fam_len, 4).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_proxy_metadata_with_balanced_routing) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    auto bytes = make_pp2_header(0xc000020a, 12345);
    auto options = make_cql_options_frame();
    bytes.insert(bytes.end(), options.begin(), options.end());
    sending_client client(make_ipv4_address({"127.0.0.1", port}), std::move(bytes));
    auto accepted = accept_with_deadline(ss);
    BOOST_REQUIRE(accepted.metadata.physical_peer);
    BOOST_REQUIRE(accepted.metadata.physical_peer->addr() == net::inet_address("127.0.0.1"));
    BOOST_REQUIRE(accepted.remote_address.addr() == net::inet_address("192.0.2.10"));
    BOOST_REQUIRE(accepted.connection.remote_address().addr() == net::inet_address("192.0.2.10"));
    auto frame = read_with_deadline(accepted.connection, 9);
    BOOST_REQUIRE_EQUAL(frame.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(frame[0]), 0x04);
    client.wait();
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_rejects_disabled_or_untrusted_proxy) {
    for (bool enabled : {false, true}) {
        unified_port_options options;
        options.proxy_protocol = enabled ? unified_port_options::proxy_protocol_mode::optional
            : unified_port_options::proxy_protocol_mode::disabled;
        options.trusted_proxy = [enabled] (const net::inet_address&) { return !enabled; };
        auto [ss, port] = listen_unified_port_on_ipv4(options);
        verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
            auto proxy = make_pp2_header();
            auto rejected = make_cql_options_frame();
            out.write(proxy.data(), proxy.size()).get();
            out.write(rejected.data(), rejected.size()).get();
        });
    }
}

static void verify_unified_policy(unified_port_options options, std::vector<char> rejected, std::vector<char> accepted, bool tls) {
    auto [ss, port] = listen_unified_port_on_ipv4(std::move(options));
    verify_bad_client_dropped(ss, port, [rejected = std::move(rejected)] (output_stream<char>& out) {
        out.write(rejected.data(), rejected.size()).get();
    }, std::move(accepted), tls, {});
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_requires_proxy_header) {
    auto options = proxy_enabled_options();
    options.proxy_protocol = unified_port_options::proxy_protocol_mode::required;
    auto accepted = make_pp2_header();
    auto shard = make_shard_selection_header(0);
    accepted.insert(accepted.end(), shard.begin(), shard.end());
    verify_unified_policy(options, make_shard_selection_header(0), std::move(accepted), false);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_encryption_policy) {
    unified_port_options options;
    options.encryption = unified_port_options::encryption_mode::tls_only;
    verify_unified_policy(options, make_shard_selection_header(0, false), make_shard_selection_header(0, true), true);
    options.encryption = unified_port_options::encryption_mode::plain_only;
    verify_unified_policy(options, make_shard_selection_header(0, true), make_shard_selection_header(0, false), false);
}

// Regression for scylladb/scylladb#28944: one idle or partial preface must not
// prevent the listener from accepting a later complete connection.
SEASTAR_THREAD_TEST_CASE(test_unified_port_stalled_preface_does_not_block_accept) {
    for (auto partial : {std::vector<char>{}, std::vector<char>{char(0xAA), 0}, std::vector<char>{0x0D}}) {
        auto [ss, port] = listen_unified_port_on_ipv4();
        auto stalled = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto out = stalled.output();
        auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
        if (!partial.empty()) {
            out.write(partial.data(), partial.size()).get();
            out.flush().get();
        }
        shared_future<lw_shared_ptr<accept_result>> accepted(ss.accept().then([] (accept_result result) {
            return make_lw_shared<accept_result>(std::move(result));
        }));
        auto cleanup = defer([&] () noexcept {
            ss.abort_accept();
            stalled.shutdown_output();
            observe_accept_completion(ss, accepted);
        });
        auto good = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto good_out = good.output();
        auto frame = make_cql_options_frame();
        good_out.write(frame.data(), frame.size()).get();
        good_out.flush().get();
        good_out.close().get();
        auto result = with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get();
        auto bytes = read_with_deadline(result->connection, frame.size());
        BOOST_REQUIRE_EQUAL(bytes.size(), frame.size());
        BOOST_REQUIRE(std::equal(bytes.begin(), bytes.end(), frame.begin()));
    }
}

// Regression for scylladb/scylladb#28944: abort must cancel an accepted socket
// blocked inside preface detection, without waiting for the peer to close it.
SEASTAR_THREAD_TEST_CASE(test_unified_port_abort_partial_preface) {
    auto detecting = make_lw_shared<promise<>>();
    auto options = proxy_enabled_options();
    options.trusted_proxy = [detecting] (const net::inet_address&) {
        detecting->set_value();
        return true;
    };
    auto [ss, port] = listen_unified_port_on_ipv4(std::move(options));
    auto client = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = client.output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    char first = 0x0D;
    out.write(&first, 1).get();
    out.flush().get();
    shared_future<> accepted(ss.accept().discard_result());
    auto cleanup = defer([&] () noexcept {
        ss.abort_accept();
        client.shutdown_output();
        observe_accept_completion(ss, accepted);
    });
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), detecting->get_future()).get();
    ss.abort_accept();
    BOOST_REQUIRE_EXCEPTION(
        with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get(),
        std::system_error, [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), client.wait_input_shutdown()).get();
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_preface_limit_and_total_deadline) {
    constexpr size_t limit = 128;
    struct detection_state {
        promise<> saturated;
        promise<> admitted;
        size_t detecting = 0;
    };
    auto state = make_lw_shared<detection_state>();
    auto options = proxy_enabled_options();
    options.trusted_proxy = [state] (const net::inet_address&) {
        if (++state->detecting == limit) {
            state->saturated.set_value();
        } else if (state->detecting == limit + 1) {
            state->admitted.set_value();
        }
        return true;
    };
    auto [ss, port] = listen_unified_port_on_ipv4(std::move(options));
    std::vector<connected_socket> clients;
    std::vector<output_stream<char>> outputs;
    clients.reserve(limit + 1);
    outputs.reserve(limit + 1);
    shared_future<lw_shared_ptr<accept_result>> accepted(ss.accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto cleanup = defer([&] () noexcept {
        ss.abort_accept();
        for (auto& client : clients) {
            client.shutdown_output();
        }
        for (auto& out : outputs) {
            try { out.close().get(); } catch (...) {}
        }
        observe_accept_completion(ss, accepted);
    });
    auto started = lowres_clock::now();
    auto connect = [port] (lowres_clock::time_point deadline) {
        auto connector = make_lw_shared<seastar::socket>(engine().net().socket());
        auto connecting = connector->connect(make_ipv4_address({"127.0.0.1", port})).finally([connector] {});
        try {
            return with_timeout(deadline, std::move(connecting)).get();
        } catch (...) {
            try { connector->shutdown(); } catch (...) {}
            throw;
        }
    };
    for (size_t i = 0; i < limit; ++i) {
        clients.push_back(connect(started + std::chrono::seconds(1)));
        outputs.push_back(clients.back().output());
        char first = 0x0D;
        outputs.back().write(&first, 1).get();
        outputs.back().flush().get();
    }
    with_timeout(started + std::chrono::seconds(1), state->saturated.get_future()).get();
    clients.push_back(connect(lowres_clock::now() + std::chrono::seconds(1)));
    outputs.push_back(clients.back().output());
    auto proxy = make_pp2_header();
    auto frame = make_cql_options_frame();
    outputs.back().write(proxy.data(), proxy.size()).get();
    outputs.back().write(frame.data(), frame.size()).get();
    outputs.back().flush().get();
    shared_future<> admitted_future(state->admitted.get_future());
    BOOST_REQUIRE_THROW(with_timeout(lowres_clock::now() + std::chrono::milliseconds(50), admitted_future.get_future()).get(), timed_out_error);

    // A real-clock delay separates read stages. Completing PPv2 must not restart
    // the five-second deadline when detection proceeds to a partial shard header.
    sleep<lowres_clock>(started + std::chrono::seconds(3) - lowres_clock::now()).get();
    outputs.front().write(proxy.data() + 1, proxy.size() - 1).get();
    char shard_magic = char(0xAA);
    outputs.front().write(&shard_magic, 1).get();
    outputs.front().flush().get();
    auto deadline = started + std::chrono::seconds(6);
    with_timeout(deadline, clients.front().wait_input_shutdown()).get();
    with_timeout(deadline, admitted_future.get_future()).get();
    auto result = with_timeout(deadline, accepted.get_future()).get();
    BOOST_REQUIRE(result->metadata.physical_peer);
    BOOST_REQUIRE(*result->metadata.physical_peer == clients.back().local_address());
    BOOST_REQUIRE_EQUAL(ntohs(result->remote_address.as_posix_sockaddr_in().sin_port), 12345);
    BOOST_REQUIRE_EQUAL(state->detecting, limit + 1);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_completed_queue_limit) {
    constexpr size_t limit = 128;
    auto [ss, port] = listen_unified_port_on_ipv4();
    std::vector<connected_socket> clients;
    clients.reserve(limit + 2);
    shared_future<lw_shared_ptr<accept_result>> first(ss.accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto cleanup = defer([&] () noexcept {
        ss.abort_accept();
        observe_accept_completion(ss, first);
    });
    auto frame = make_cql_options_frame();
    for (size_t i = 0; i < limit + 2; ++i) {
        clients.push_back(seastar::connect(make_ipv4_address({"127.0.0.1", port})).get());
        auto out = clients.back().output();
        out.write(frame.data(), frame.size()).get();
        out.close().get();
    }
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), first.get_future()).get();
    // One connection was delivered to the initial accept, 128 were queued,
    // and the last completion must be closed instead of extending the queue.
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), clients.back().wait_input_shutdown()).get();
    for (size_t i = 0; i < limit; ++i) {
        shared_future<lw_shared_ptr<accept_result>> next(ss.accept().then([] (accept_result result) {
            return make_lw_shared<accept_result>(std::move(result));
        }));
        auto drain = defer([&] () noexcept {
            observe_accept_completion(ss, next);
        });
        lw_shared_ptr<accept_result> result;
        try {
            result = with_timeout(lowres_clock::now() + std::chrono::seconds(1), next.get_future()).get();
        } catch (...) {
            ss.abort_accept();
            throw;
        }
        auto bytes = read_with_deadline(result->connection, frame.size());
        BOOST_REQUIRE_EQUAL(bytes.size(), frame.size());
        BOOST_REQUIRE(std::equal(bytes.begin(), bytes.end(), frame.begin()));
    }
    shared_future<> next(ss.accept().discard_result());
    auto drain = defer([&] () noexcept {
        ss.abort_accept();
        observe_accept_completion(ss, next);
    });
    BOOST_REQUIRE_THROW(with_timeout(lowres_clock::now() + std::chrono::milliseconds(50), next.get_future()).get(), timed_out_error);
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_destroy_partial_preface) {
    auto detecting = make_lw_shared<promise<>>();
    auto options = proxy_enabled_options();
    options.trusted_proxy = [detecting] (const net::inet_address&) {
        detecting->set_value();
        return true;
    };
    auto [ss, port] = listen_unified_port_on_ipv4(std::move(options));
    auto client = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = client.output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    char first = 0x0D;
    out.write(&first, 1).get();
    out.flush().get();
    shared_future<> accepted(ss.accept().discard_result());
    auto cleanup = defer([&] () noexcept {
        if (ss) {
            ss.abort_accept();
        }
        client.shutdown_output();
        observe_accept_completion(ss, accepted);
    });
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), detecting->get_future()).get();
    ss = server_socket{};
    BOOST_REQUIRE_EXCEPTION(
        with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get(),
        std::system_error, [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), client.wait_input_shutdown()).get();
}

// Exercise the receiving end of a shard transfer without starting extra shards.
SEASTAR_THREAD_TEST_CASE(test_unified_port_transferred_queue_lifecycle) {
    constexpr size_t limit = 128;
    auto listen_address = make_ipv4_address({"127.0.0.1", 0});
    auto proxy_remote = make_ipv4_address({"192.0.2.10", 12345});
    auto proxy_local = make_ipv4_address({"192.0.2.20", 9042});
    server_socket ss(std::make_unique<net::posix_ap_server_socket_impl>(IPPROTO_TCP, listen_address, memory::malloc_allocator, true));
    auto cleanup = defer([&] () noexcept { ss = server_socket{}; });
    auto transfer = [&] {
        auto fd = pollable_fd(file_desc::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP));
        auto descriptor = fd.get_file_desc().get();
        connection_metadata metadata;
        metadata.is_tls = true;
        metadata.physical_peer = listen_address;
        net::posix_ap_server_socket_impl::move_connected_socket(
            IPPROTO_TCP, listen_address, std::move(fd), proxy_remote, {},
            net::proxy_data{proxy_remote, proxy_local}, memory::malloc_allocator, metadata, char(0x16));
        return descriptor;
    };
    auto verify = [&] (accept_result& result) {
        BOOST_REQUIRE(result.metadata.is_tls);
        BOOST_REQUIRE(result.metadata.physical_peer);
        BOOST_REQUIRE(*result.metadata.physical_peer == listen_address);
        BOOST_REQUIRE(result.remote_address == proxy_remote);
        BOOST_REQUIRE(result.connection.remote_address() == proxy_remote);
        BOOST_REQUIRE(result.connection.local_address() == proxy_local);
        auto unused = result.connection.input();
        auto in = result.connection.input();
        auto prefix = in.read_exactly(1).get();
        BOOST_REQUIRE_EQUAL(prefix.size(), 1u);
        BOOST_REQUIRE_EQUAL(uint8_t(prefix[0]), 0x16);
    };
    for (size_t i = 0; i < limit + 1; ++i) {
        transfer();
    }
    for (size_t i = 0; i < limit; ++i) {
        auto result = accept_with_deadline(ss);
        verify(result);
    }
    shared_future<lw_shared_ptr<accept_result>> direct(ss.accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto drain = defer([&] () noexcept {
        if (ss) {
            ss.abort_accept();
        }
        observe_accept_completion(ss, direct);
    });
    BOOST_REQUIRE_THROW(with_timeout(lowres_clock::now() + std::chrono::milliseconds(50), direct.get_future()).get(), timed_out_error);
    transfer();
    auto result = with_timeout(lowres_clock::now() + std::chrono::seconds(1), direct.get_future()).get();
    verify(*result);
    shared_future<> pending(ss.accept().discard_result());
    ss = server_socket{};
    observe_accept_completion(ss, pending);
    BOOST_REQUIRE_EXCEPTION(
        with_timeout(lowres_clock::now() + std::chrono::seconds(1), pending.get_future()).get(),
        std::system_error, [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });

    ss = server_socket(std::make_unique<net::posix_ap_server_socket_impl>(IPPROTO_TCP, listen_address, memory::malloc_allocator, true));
    ss.abort_accept();
    auto dropped_descriptor = transfer();
    BOOST_REQUIRE_EQUAL(::fcntl(dropped_descriptor, F_GETFD), -1);
    BOOST_REQUIRE_EQUAL(errno, EBADF);
    BOOST_REQUIRE_EXCEPTION(accept_with_deadline(ss), std::system_error,
        [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });
}

namespace seastar::net {

class posix_server_socket_test_access {
public:
    static size_t proxy_bytes_read(const posix_server_socket_impl& socket) {
        return socket.proxy_protocol_bytes_read();
    }
};

}

struct legacy_proxy_listener {
    server_socket socket;
    uint16_t port;
    // Used only while socket owns the implementation, before destruction.
    net::posix_server_socket_impl* implementation;
};

// Construct the legacy serial acceptor directly so the fixture explicitly
// exercises its PPv2 parser independently of network-stack listener selection.
static legacy_proxy_listener listen_legacy_proxy_on_ipv4() {
    auto address = make_ipv4_address({"127.0.0.1", 0});
    auto fd = file_desc::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    fd.bind(address.as_posix_sockaddr(), address.addr_length);
    fd.listen(128);
    address = fd.get_address();
    auto port = ntohs(address.as_posix_sockaddr_in().sin_port);
    auto impl = std::make_unique<net::posix_server_socket_impl>(IPPROTO_TCP, address, pollable_fd(std::move(fd)),
        server_socket::load_balancing_algorithm::port, 0, true);
    auto* implementation = impl.get();
    return {server_socket(std::move(impl)), port, implementation};
}

static void wait_for_legacy_proxy_bytes(const net::posix_server_socket_impl& socket, size_t expected) {
    auto deadline = lowres_clock::now() + std::chrono::seconds(1);
    while (net::posix_server_socket_test_access::proxy_bytes_read(socket) < expected) {
        if (lowres_clock::now() >= deadline) {
            throw timed_out_error();
        }
        yield().get();
    }
    BOOST_REQUIRE_EQUAL(net::posix_server_socket_test_access::proxy_bytes_read(socket), expected);
}

static void verify_legacy_partial_abort(size_t prefix_size, bool destroy = false) {
    auto [ss, port, implementation] = listen_legacy_proxy_on_ipv4();
    auto client = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = client.output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    auto proxy = make_pp2_header();
    out.write(proxy.data(), prefix_size).get();
    out.flush().get();
    shared_future<> accepted(ss.accept().discard_result());
    auto cleanup = defer([&] () noexcept {
        if (ss) {
            ss.abort_accept();
        }
        client.shutdown_output();
        observe_accept_completion(ss, accepted);
    });
    // Observe consumed bytes before abort. The open peer cannot release the
    // blocked parser through EOF, and a pending listener accept cannot pass.
    wait_for_legacy_proxy_bytes(*implementation, prefix_size);
    BOOST_REQUIRE(!accepted.available());
    if (destroy) {
        ss = server_socket{};
    } else {
        ss.abort_accept();
    }
    BOOST_REQUIRE_EXCEPTION(
        with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get(),
        std::system_error, [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });
}

// Regression for scylladb/scylladb#28944: fragmented fixed-header reads
// must remain cancellable by listener shutdown.
SEASTAR_THREAD_TEST_CASE(test_legacy_proxy_abort_partial_header) {
    verify_legacy_partial_abort(1);
}

SEASTAR_THREAD_TEST_CASE(test_legacy_proxy_abort_partial_payload) {
    verify_legacy_partial_abort(20);
}

SEASTAR_THREAD_TEST_CASE(test_legacy_proxy_destroy_partial_preface) {
    verify_legacy_partial_abort(1, true);
    verify_legacy_partial_abort(20, true);
}

SEASTAR_THREAD_TEST_CASE(test_legacy_proxy_partial_preface_has_total_deadline) {
    auto [ss, port, implementation] = listen_legacy_proxy_on_ipv4();
    auto stalled = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = stalled.output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    auto proxy = make_pp2_header();
    out.write(proxy.data(), 1).get();
    out.flush().get();
    shared_future<lw_shared_ptr<accept_result>> accepted(ss.accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto cleanup = defer([&] () noexcept {
        ss.abort_accept();
        stalled.shutdown_output();
        observe_accept_completion(ss, accepted);
    });
    wait_for_legacy_proxy_bytes(*implementation, 1);
    auto started = lowres_clock::now();
    auto good = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto good_out = good.output();
    auto good_proxy = make_pp2_header(0xc000020a, 23456);
    auto frame = make_cql_options_frame();
    good_out.write(good_proxy.data(), good_proxy.size()).get();
    good_out.write(frame.data(), frame.size()).get();
    good_out.close().get();
    // Progress into the payload after three seconds must not restart the timer.
    sleep<lowres_clock>(std::chrono::seconds(3)).get();
    out.write(proxy.data() + 1, 19).get();
    out.flush().get();
    auto result = with_timeout(started + std::chrono::seconds(6), accepted.get_future()).get();
    BOOST_REQUIRE(result->remote_address.addr() == net::inet_address("192.0.2.10"));
    BOOST_REQUIRE_EQUAL(ntohs(result->remote_address.as_posix_sockaddr_in().sin_port), 23456);
    auto bytes = read_with_deadline(result->connection, frame.size());
    BOOST_REQUIRE_EQUAL(bytes.size(), frame.size());
    BOOST_REQUIRE(std::equal(bytes.begin(), bytes.end(), frame.begin()));
}

SEASTAR_THREAD_TEST_CASE(test_legacy_proxy_accepts_fragmented_header_and_payload) {
    auto [ss, port, implementation] = listen_legacy_proxy_on_ipv4();
    auto client = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = client.output();
    auto close_output = defer([&] () noexcept { try { out.close().get(); } catch (...) {} });
    shared_future<lw_shared_ptr<accept_result>> accepted(ss.accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto cleanup = defer([&] () noexcept {
        ss.abort_accept();
        client.shutdown_output();
        observe_accept_completion(ss, accepted);
    });
    auto proxy = make_pp2_header(0xc000020b, 23457);
    size_t offset = 0;
    for (size_t fragment : {1u, 15u, 5u}) {
        out.write(proxy.data() + offset, fragment).get();
        out.flush().get();
        offset += fragment;
        wait_for_legacy_proxy_bytes(*implementation, offset);
        BOOST_REQUIRE(!accepted.available());
    }
    out.write(proxy.data() + offset, proxy.size() - offset).get();
    auto frame = make_cql_options_frame();
    out.write(frame.data(), frame.size()).get();
    out.close().get();
    auto result = with_timeout(lowres_clock::now() + std::chrono::seconds(1), accepted.get_future()).get();
    BOOST_REQUIRE(result->remote_address.addr() == net::inet_address("192.0.2.11"));
    BOOST_REQUIRE_EQUAL(ntohs(result->remote_address.as_posix_sockaddr_in().sin_port), 23457);
    BOOST_REQUIRE(result->connection.local_address().addr() == net::inet_address("127.0.0.1"));
    auto bytes = read_with_deadline(result->connection, frame.size());
    BOOST_REQUIRE_EQUAL(bytes.size(), frame.size());
    BOOST_REQUIRE(std::equal(bytes.begin(), bytes.end(), frame.begin()));
}

SEASTAR_THREAD_TEST_CASE(test_unified_port_rejected_accept_has_local_deadline) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    auto client = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
    auto out = client.output();
    auto invalid = make_shard_selection_header(0);
    invalid[1] = 0x02;
    out.write(invalid.data(), invalid.size()).get();
    out.close().get();
    BOOST_REQUIRE_THROW(accept_with_deadline(ss), timed_out_error);
    BOOST_REQUIRE_EXCEPTION(accept_with_deadline(ss), std::system_error,
        [] (const std::system_error& e) { return e.code().value() == ECONNABORTED; });
}

SEASTAR_THREAD_TEST_CASE(test_socket_read_deadline_cancels_partial_read) {
    auto [ss, port] = listen_unified_port_on_ipv4();
    auto stop = defer([&ss] () noexcept { ss.abort_accept(); });
    auto connecting = make_lw_shared<seastar::socket>(engine().net().socket());
    auto cancel_connect = defer([connecting] () noexcept { try { connecting->shutdown(); } catch (...) {} });
    auto connection = connecting->connect(make_ipv4_address({"127.0.0.1", port})).finally([connecting] {});
    auto peer = make_lw_shared<connected_socket>(
        with_timeout(lowres_clock::now() + std::chrono::seconds(1), std::move(connection)).get());
    auto out = make_lw_shared<output_stream<char>>(peer->output());
    auto frame = make_lw_shared<std::vector<char>>(make_cql_options_frame());
    shared_future<> writing(out->write(frame->data(), 1).then([out] { return out->flush(); }).finally([out, peer, frame] {}));
    auto close_output = defer([out, peer, writing] () mutable noexcept {
        peer->shutdown_output();
        (void)writing.get_future().then_wrapped([out, peer] (auto result) {
            result.ignore_ready_future();
            return out->close().finally([out, peer] {});
        }).then_wrapped([] (auto result) noexcept { result.ignore_ready_future(); });
    });
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), writing.get_future()).get();
    auto accepted = make_lw_shared<accept_result>(accept_with_deadline(ss));
    shared_future<bool> reading(async([accepted] {
        try {
            read_with_deadline(accepted->connection, 9, std::chrono::milliseconds(10));
            return false;
        } catch (const timed_out_error&) {
            return true;
        }
    }));
    auto cancel = defer([peer, reading] () mutable noexcept {
        peer->shutdown_input();
        peer->shutdown_output();
        (void)reading.get_future().then_wrapped([peer] (auto result) noexcept { result.ignore_ready_future(); });
    });
    BOOST_REQUIRE(with_timeout(lowres_clock::now() + std::chrono::milliseconds(200), reading.get_future()).get());
    BOOST_REQUIRE(!accepted->connection);
}

SEASTAR_THREAD_TEST_CASE(test_client_cleanup_retains_failed_cancellation) {
    struct operation_state {
        promise<connected_socket> connected;
        promise<> destroyed;
        unsigned shutdowns = 0;
    };
    class uncancellable_socket final : public net::socket_impl {
        lw_shared_ptr<operation_state> _state;
    public:
        explicit uncancellable_socket(lw_shared_ptr<operation_state> state) : _state(std::move(state)) {}
        ~uncancellable_socket() { _state->destroyed.set_value(); }
        future<connected_socket> connect(socket_address, socket_address, transport) override {
            return _state->connected.get_future();
        }
        void set_reuseaddr(bool) override {}
        bool get_reuseaddr() const override { return false; }
        void shutdown() override { ++_state->shutdowns; }
    };
    auto state = make_lw_shared<operation_state>();
    auto destroyed = state->destroyed.get_future();
    shared_future<> cleaned(async([state] {
        sending_client client(seastar::socket(std::make_unique<uncancellable_socket>(state)), {}, {});
        BOOST_REQUIRE_THROW(client.wait(std::chrono::milliseconds(10)), timed_out_error);
    }));
    auto release = defer([state, cleaned] () mutable noexcept {
        state->connected.set_exception(std::make_exception_ptr(std::runtime_error("release pending test connect")));
        try { with_timeout(lowres_clock::now() + std::chrono::seconds(1), cleaned.get_future()).get(); } catch (...) {}
    });
    with_timeout(lowres_clock::now() + std::chrono::milliseconds(200), cleaned.get_future()).get();
    BOOST_REQUIRE_GE(state->shutdowns, 1u);
    BOOST_REQUIRE(!destroyed.available());
    release.cancel();
    state->connected.set_exception(std::make_exception_ptr(std::runtime_error("release pending test connect")));
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), std::move(destroyed)).get();
}

// Regression for scylladb/scylladb#28944: failed accept cancellation must not
// turn a local helper deadline into an unbounded cleanup wait.
SEASTAR_THREAD_TEST_CASE(test_accept_deadline_bounds_failed_cancellation) {
    struct operation_state {
        promise<accept_result> accepted;
        promise<> destroyed;
        unsigned aborts = 0;
    };
    class uncancellable_listener final : public net::server_socket_impl {
        lw_shared_ptr<operation_state> _state;
    public:
        explicit uncancellable_listener(lw_shared_ptr<operation_state> state) : _state(std::move(state)) {}
        ~uncancellable_listener() { _state->destroyed.set_value(); }
        future<accept_result> accept() override { return _state->accepted.get_future(); }
        void abort_accept() override { ++_state->aborts; }
        socket_address local_address() const override { return {}; }
    };
    auto state = make_lw_shared<operation_state>();
    auto listener = make_lw_shared<server_socket>(std::make_unique<uncancellable_listener>(state));
    shared_future<> attempted(async([listener] {
        BOOST_REQUIRE_THROW(accept_with_deadline(*listener, std::chrono::milliseconds(10)), timed_out_error);
    }));
    auto cleanup = defer([state, attempted] () mutable noexcept {
        state->accepted.set_exception(std::make_exception_ptr(std::runtime_error("release pending test accept")));
        try { with_timeout(lowres_clock::now() + std::chrono::seconds(1), attempted.get_future()).get(); } catch (...) {}
    });
    with_timeout(lowres_clock::now() + std::chrono::milliseconds(200), attempted.get_future()).get();
    BOOST_REQUIRE_GE(state->aborts, 1u);
    listener = {};
    // A pending continuation must retain its listener until completion.
    auto destroyed = state->destroyed.get_future();
    BOOST_REQUIRE(!destroyed.available());
    cleanup.cancel();
    state->accepted.set_exception(std::make_exception_ptr(std::runtime_error("release pending test accept")));
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), std::move(destroyed)).get();

    // Direct-fixture cleanup must also return without waiting for cancellation.
    state = make_lw_shared<operation_state>();
    listener = make_lw_shared<server_socket>(std::make_unique<uncancellable_listener>(state));
    shared_future<lw_shared_ptr<accept_result>> accepted(listener->accept().then([] (accept_result result) {
        return make_lw_shared<accept_result>(std::move(result));
    }));
    auto release = defer([state] () noexcept {
        state->accepted.set_exception(std::make_exception_ptr(std::runtime_error("release pending test accept")));
    });
    shared_future<> observed(async([listener, accepted] { observe_accept_completion(*listener, accepted); }));
    with_timeout(lowres_clock::now() + std::chrono::milliseconds(200), observed.get_future()).get();
    BOOST_REQUIRE_GE(state->aborts, 1u);
    listener = {};
    auto observer_destroyed = state->destroyed.get_future();
    BOOST_REQUIRE(!observer_destroyed.available());
    release.cancel();
    state->accepted.set_exception(std::make_exception_ptr(std::runtime_error("release pending test accept")));
    with_timeout(lowres_clock::now() + std::chrono::seconds(1), std::move(observer_destroyed)).get();
}
