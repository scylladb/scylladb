/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/tls.hh>

#include <cstring>

using namespace seastar;

// Master port shard selection header format:
// Byte 0: Magic (0xAA)
// Byte 1: Flags (bit 0 = TLS)
// Byte 2-3: Shard ID (big-endian uint16_t)

static constexpr uint8_t MASTER_PORT_MAGIC = 0xAA;
static constexpr uint8_t SHARD_SELECT_FLAG_TLS = 0x01;

static std::vector<char> make_shard_selection_header(uint16_t shard_id, bool tls = false) {
    std::vector<char> header(4);
    header[0] = static_cast<char>(MASTER_PORT_MAGIC);
    header[1] = tls ? SHARD_SELECT_FLAG_TLS : 0;
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

// Listen on port 0 with master_port enabled, return the server_socket and its actual port.
static std::pair<server_socket, uint16_t> listen_master_port_on_ipv4() {
    listen_options lo;
    lo.reuse_address = true;
    lo.master_port = true;
    auto ss = seastar::listen(make_ipv4_address({0}), lo);
    auto port = ss.local_address().as_posix_sockaddr_in().sin_port;
    port = ntohs(port);
    return {std::move(ss), port};
}

SEASTAR_THREAD_TEST_CASE(test_shard_selection_header_format) {
    auto hdr = make_shard_selection_header(0, false);
    BOOST_REQUIRE_EQUAL(hdr.size(), 4u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[0]), MASTER_PORT_MAGIC);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[1]), 0x00);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[2]), 0x00);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[3]), 0x00);

    hdr = make_shard_selection_header(3, true);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(hdr[1]), SHARD_SELECT_FLAG_TLS);
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
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(make_shard_selection_header(0)[0]), MASTER_PORT_MAGIC);

    // CQL frame: first byte is version (0x03 or 0x04)
    auto options = make_cql_options_frame();
    BOOST_REQUIRE(static_cast<uint8_t>(options[0]) >= 0x03 &&
                  static_cast<uint8_t>(options[0]) <= 0x05);

    // TLS ClientHello: first byte is 0x16
    uint8_t tls_first = 0x16;
    BOOST_REQUIRE_EQUAL(tls_first, 0x16);

    // All four first-byte values are distinct
    BOOST_REQUIRE_NE(0x0D, MASTER_PORT_MAGIC);
    BOOST_REQUIRE_NE(0x0D, 0x16);
    BOOST_REQUIRE_NE(0x0D, 0x04);
    BOOST_REQUIRE_NE(MASTER_PORT_MAGIC, 0x16);
    BOOST_REQUIRE_NE(MASTER_PORT_MAGIC, 0x04);
    BOOST_REQUIRE_NE(0x16, 0x04);
}

SEASTAR_THREAD_TEST_CASE(test_listen_options_master_port) {
    listen_options lo;
    BOOST_REQUIRE_EQUAL(lo.master_port, false);
    lo.master_port = true;
    BOOST_REQUIRE_EQUAL(lo.master_port, true);
}

SEASTAR_THREAD_TEST_CASE(test_listen_options_mutual_exclusion) {
    listen_options lo;
    lo.master_port = true;
    lo.proxy_protocol = true;
    BOOST_REQUIRE_THROW(seastar::listen(make_ipv4_address({0}), lo), std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(test_accept_result_metadata) {
    connection_metadata meta;
    BOOST_REQUIRE_EQUAL(meta.is_tls, false);
    meta.is_tls = true;
    BOOST_REQUIRE_EQUAL(meta.is_tls, true);
}

SEASTAR_THREAD_TEST_CASE(test_master_port_accept_plain_cql) {
    BOOST_REQUIRE_EQUAL(smp::count, 1u);
    auto [ss, port] = listen_master_port_on_ipv4();

    auto client = async([port] {
        auto s = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto out = s.output();
        auto options = make_cql_options_frame();
        out.write(options.data(), options.size()).get();
        out.flush().get();
        out.close().get();
    });

    auto ar = ss.accept().get();
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto in = ar.connection.input();
    auto buf = in.read_exactly(9).get();
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.get();
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_accept_shard_selection) {
    BOOST_REQUIRE_EQUAL(smp::count, 1u);
    auto [ss, port] = listen_master_port_on_ipv4();

    uint16_t target_shard = 0;
    auto client = async([port, target_shard] {
        auto s = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto out = s.output();
        auto hdr = make_shard_selection_header(target_shard, false);
        auto options = make_cql_options_frame();
        out.write(hdr.data(), hdr.size()).get();
        out.write(options.data(), options.size()).get();
        out.flush().get();
        out.close().get();
    });

    auto ar = ss.accept().get();
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto in = ar.connection.input();
    auto buf = in.read_exactly(9).get();
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.get();
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_accept_plain_cql_ipv6) {
    BOOST_REQUIRE_EQUAL(smp::count, 1u);
    listen_options lo;
    lo.reuse_address = true;
    lo.master_port = true;
    auto loopback6 = net::inet_address("::1");
    auto ss = seastar::listen(socket_address(loopback6, 0), lo);
    auto port = ntohs(ss.local_address().as_posix_sockaddr_in6().sin6_port);

    auto client = async([port, loopback6] {
        auto s = seastar::connect(socket_address(loopback6, port)).get();
        auto out = s.output();
        auto options = make_cql_options_frame();
        out.write(options.data(), options.size()).get();
        out.flush().get();
        out.close().get();
    });

    auto ar = ss.accept().get();
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto in = ar.connection.input();
    auto buf = in.read_exactly(9).get();
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[4]), 0x05); // OPTIONS opcode

    client.get();
    ss.abort_accept();
}

// --- Negative tests ---

// Helper: sends bad data via bad_fn, then verifies that the server drops the
// bad client and accepts a subsequent good (plain CQL) client successfully.
static void verify_bad_client_dropped(server_socket& ss, uint16_t port,
                                       noncopyable_function<void(output_stream<char>&)> bad_fn) {
    BOOST_REQUIRE_EQUAL(smp::count, 1u);

    auto bad_client = async([port, bad_fn = std::move(bad_fn)] () mutable {
        auto s = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto out = s.output();
        bad_fn(out);
        out.flush().get();
        out.close().get();
    });

    auto good_client = async([port] {
        auto s = seastar::connect(make_ipv4_address({"127.0.0.1", port})).get();
        auto out = s.output();
        auto options = make_cql_options_frame();
        out.write(options.data(), options.size()).get();
        out.flush().get();
        out.close().get();
    });

    auto ar = ss.accept().get();
    BOOST_REQUIRE_EQUAL(ar.metadata.is_tls, false);
    auto in = ar.connection.input();
    auto buf = in.read_exactly(9).get();
    BOOST_REQUIRE_EQUAL(buf.size(), 9u);
    BOOST_REQUIRE_EQUAL(static_cast<uint8_t>(buf[0]), 0x04); // CQL v4

    bad_client.get();
    good_client.get();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_truncated_shard_header) {
    auto [ss, port] = listen_master_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // Only magic + 1 byte (incomplete 4-byte header)
        char partial[2] = {static_cast<char>(MASTER_PORT_MAGIC), 0x00};
        out.write(partial, 2).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_invalid_pp2_signature) {
    auto [ss, port] = listen_master_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // 0x0D followed by 15 zero bytes -- invalid PP v2 signature
        char bad_header[16] = {};
        bad_header[0] = 0x0D;
        out.write(bad_header, 16).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_truncated_pp2_header) {
    auto [ss, port] = listen_master_port_on_ipv4();
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

SEASTAR_THREAD_TEST_CASE(test_master_port_pp2_unsupported_address_family) {
    auto [ss, port] = listen_master_port_on_ipv4();
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
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_reserved_flags_rejected) {
    auto [ss, port] = listen_master_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        // Magic 0xAA, flags=0x02 (reserved bit 1 set), shard=0
        char hdr[4] = {static_cast<char>(MASTER_PORT_MAGIC), 0x02, 0x00, 0x00};
        out.write(hdr, 4).get();
    });
    ss.abort_accept();
}

SEASTAR_THREAD_TEST_CASE(test_master_port_pp2_oversized_addr_data) {
    auto [ss, port] = listen_master_port_on_ipv4();
    verify_bad_client_dropped(ss, port, [] (output_stream<char>& out) {
        static const char pp2_sig[12] = {
            0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
        };
        out.write(pp2_sig, 12).get();
        // v2 PROXY, AF_INET/STREAM, length=0x0FFF (4095 bytes -- way too large)
        char cmd_fam_len[4] = {0x21, 0x11, 0x0F, static_cast<char>(0xFF)};
        out.write(cmd_fam_len, 4).get();
    });
    ss.abort_accept();
}
