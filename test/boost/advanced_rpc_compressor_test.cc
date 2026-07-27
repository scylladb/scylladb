/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/manual_clock.hh>
#include <seastar/net/byteorder.hh>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <seastar/util/tmp_file.hh>
#include <seastar/util/closeable.hh>
#include "test/lib/random_utils.hh"
#include "test/lib/scylla_test_case.hh"
#include "message/advanced_rpc_compressor.hh"
#include "message/advanced_rpc_compressor_protocol.hh"
#include "message/stream_compressor.hh"
#include "utils/crc.hh"
#include "utils/xx_hasher.hh"

using namespace seastar;
using namespace std::chrono_literals;

static netw::dict_ptr make_dict(uint64_t timestamp, std::vector<std::byte> content = {}) {
    return make_lw_shared(make_foreign(make_lw_shared(netw::shared_dict(std::move(content), timestamp, {}))));
}

SEASTAR_THREAD_TEST_CASE(test_control_protocol_sanity) {
    condition_variable cv;
    auto dict_1 = make_dict(1);
    auto dict_2 = make_dict(2);
    netw::control_protocol alice(cv);
    netw::control_protocol bob(cv);
    auto settle = [&] {
        bool run = true;
        while (run) {
            run = false;
            if (auto msg = alice.produce_control_header()) {
                bob.consume_control_header(*msg);
                run = true;
            }
            if (auto msg = bob.produce_control_header()) {
                alice.consume_control_header(*msg);
                run = true;
            }
        }
    };
    alice.announce_dict(dict_1);
    bob.announce_dict(dict_2);
    settle();
    BOOST_REQUIRE(alice.sender_current_dict().id == netw::shared_dict::dict_id());
    BOOST_REQUIRE(bob.sender_current_dict().id == netw::shared_dict::dict_id());
    alice.announce_dict(dict_2);
    settle();
    BOOST_REQUIRE(alice.sender_current_dict().id == (**dict_2).id);
    BOOST_REQUIRE(bob.sender_current_dict().id == (**dict_2).id);
    alice.announce_dict(nullptr);
    settle();
    BOOST_REQUIRE(alice.sender_current_dict().id == (**dict_2).id);
    BOOST_REQUIRE(bob.sender_current_dict().id == (**dict_2).id);
    bob.announce_dict(nullptr);
    settle();
    BOOST_REQUIRE(alice.sender_current_dict().id == netw::shared_dict::dict_id());
    BOOST_REQUIRE(bob.sender_current_dict().id == netw::shared_dict::dict_id());
}

temporary_buffer<char> bytes_view_to_temporary_buffer(bytes_view bv) {
    return temporary_buffer<char>(reinterpret_cast<const char*>(bv.data()), bv.size());
}

template<class T>
concept RpcBuf = std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>;

template <RpcBuf Buf>
bytes rpc_buf_to_bytes(const Buf& data) {
    if (auto src = std::get_if<temporary_buffer<char>>(&data.bufs)) {
        return bytes(reinterpret_cast<const bytes::value_type*>(src->get()), src->size());
    }
    auto src = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    auto out = bytes(bytes::initialized_later{}, data.size);
    size_t i = 0;
    while (i < data.size) {
        std::memcpy(&out[i], src->get(), src->size());
        i += src->size();
        ++src;
    }
    return out;
}

template <RpcBuf Buf, RpcBuf BufFrom>
Buf convert_rpc_buf(BufFrom data) {
    Buf b;
    b.size = data.size;
    b.bufs = std::move(data.bufs);
    return b;
}

class tracker_without_clock final : public netw::advanced_rpc_compressor::tracker {
    virtual uint64_t get_steady_nanos() const override {
        return 0;
    }
public:
    using tracker::tracker;
};

static uint32_t crc32(bytes_view data) {
    utils::crc32 crc;
    crc.process(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    return crc.get();
}

static uint64_t xxh64(bytes_view data) {
    xx_hasher hasher;
    hasher.update(reinterpret_cast<const char*>(data.data()), data.size());
    return hasher.finalize_uint64();
}

static std::filesystem::path fingerprint_match_dump_path() {
    return seastar::default_tmpdir() / fmt::format("scylladb_rpc_fingerprint_match_dump.shard_{}.bin", this_shard_id());
}

static std::vector<std::byte> read_file(std::filesystem::path path) {
    std::ifstream in(path, std::ios::binary);
    std::vector<char> data{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    std::vector<std::byte> out(data.size());
    std::memcpy(out.data(), data.data(), data.size());
    return out;
}

SEASTAR_THREAD_TEST_CASE(test_rpc_compression_fingerprint_serialization) {
    const auto fingerprint = netw::rpc_compression_fingerprint{
        .crc = 0x01020304,
        .compressed_xxh64 = 0x05060708090a0b0c,
    };
    const auto serialized = fingerprint.serialize();
    BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt_hex(serialized)), "0102030405060708090a0b0c");

    auto parsed_from_bytes = netw::rpc_compression_fingerprint::deserialize(serialized);
    BOOST_REQUIRE(parsed_from_bytes);
    BOOST_REQUIRE(*parsed_from_bytes == fingerprint);

    auto parsed_from_string = netw::rpc_compression_fingerprint::deserialize("0102030405060708090a0b0c");
    BOOST_REQUIRE(parsed_from_string);
    BOOST_REQUIRE(*parsed_from_string == fingerprint);

    BOOST_REQUIRE(!netw::rpc_compression_fingerprint::deserialize(""));
    BOOST_REQUIRE(!netw::rpc_compression_fingerprint::deserialize("01020304"));
    BOOST_REQUIRE(!netw::rpc_compression_fingerprint::deserialize("not a fingerprint"));
}

// Test the effect of the dump_message_on_fingerprint option.
static void run_dump_message_on_fingerprint_test(bool live_update) {
    bytes message;
    for (int i = 0; i < 100; ++i) {
        const auto chunk = fmt::format("rpc compression fingerprint dump payload {}\n", i);
        message.append(reinterpret_cast<const int8_t*>(chunk.data()), chunk.size());
    }

    netw::lz4_cstream cstream;
    const auto compressed_message = rpc_buf_to_bytes(
        netw::compress_impl(0, rpc::snd_buf{bytes_view_to_temporary_buffer(message)}, cstream, true, rpc::snd_buf::chunk_size));
    BOOST_REQUIRE_NE(compressed_message, message);

    const auto fingerprint = netw::rpc_compression_fingerprint{
        .crc = crc32(bytes_view(message)),
        .compressed_xxh64 = xxh64(bytes_view(compressed_message)),
    };
    const auto fingerprint_string = sstring(fmt::format("{}", fmt_hex(fingerprint.serialize())));

    const auto path = fingerprint_match_dump_path();
    std::filesystem::remove(path);
    auto cleanup = defer([&path] {
        std::filesystem::remove(path);
    });

    utils::updateable_value_source<sstring> dump_message_on_fingerprint(live_update ? sstring() : fingerprint_string);
    auto cfg = netw::advanced_rpc_compressor::tracker::config{
        .algo_config = utils::updateable_value<netw::algo_config>{
            {netw::compression_algorithm::type::LZ4},
        },
        .checksumming = utils::updateable_value<bool>{true},
        .dump_message_on_fingerprint = utils::updateable_value<sstring>{dump_message_on_fingerprint},
    };
    tracker_without_clock tracker{cfg};
    auto sender = tracker.negotiate(tracker.supported(), true, [] { return make_ready_future<>(); });
    auto close_sender = deferred_close(*sender);
    auto receiver = tracker.negotiate(sender->name(), false, [] { return make_ready_future<>(); });
    auto close_receiver = deferred_close(*receiver);

    // Settle the algorithm negotiation, so that the message below is compressed with LZ4
    // rather than with the initial RAW.
    for (int i = 0; i < 5; ++i) {
        receiver->decompress(convert_rpc_buf<rpc::rcv_buf>(sender->compress(0, rpc::snd_buf{0})));
        sender->decompress(convert_rpc_buf<rpc::rcv_buf>(receiver->compress(0, rpc::snd_buf{0})));
    }

    constexpr int head_space = 4;

    if (live_update) {
        auto compressed = sender->compress(head_space, rpc::snd_buf{bytes_view_to_temporary_buffer(message)});
        dump_message_on_fingerprint.set(fingerprint_string);
    }

    BOOST_REQUIRE(!std::filesystem::exists(path));
    auto compressed = sender->compress(head_space, rpc::snd_buf{bytes_view_to_temporary_buffer(message)});
    BOOST_REQUIRE(std::filesystem::exists(path));

    auto dump = read_file(path);
    size_t pos = 0;
    BOOST_REQUIRE_GT(dump.size(), 1 + sizeof(uint32_t) + sizeof(uint32_t) + 32 + sizeof(uint64_t));
    BOOST_REQUIRE_EQUAL(uint8_t(dump[pos++]), uint8_t(compressed.front().get()[head_space]));
    BOOST_REQUIRE_EQUAL(seastar::read_be<uint32_t>(reinterpret_cast<const char*>(dump.data() + pos)), fingerprint.crc);
    pos += sizeof(uint32_t);
    BOOST_REQUIRE_EQUAL(seastar::read_be<uint32_t>(reinterpret_cast<const char*>(dump.data() + pos)), fingerprint.crc);
    pos += sizeof(uint32_t);
    pos += 32;
    const auto dict_size = seastar::read_be<uint64_t>(reinterpret_cast<const char*>(dump.data() + pos));
    pos += sizeof(uint64_t);
    BOOST_REQUIRE_EQUAL(dict_size, 0);

    bytes dumped_payload;
    while (pos < dump.size()) {
        BOOST_REQUIRE_GE(dump.size() - pos, sizeof(uint64_t));
        const auto fragment_size = seastar::read_be<uint64_t>(reinterpret_cast<const char*>(dump.data() + pos));
        pos += sizeof(uint64_t);
        BOOST_REQUIRE_GE(dump.size() - pos, fragment_size);
        dumped_payload.append(reinterpret_cast<const int8_t*>(dump.data() + pos), fragment_size);
        pos += fragment_size;
    }
    BOOST_REQUIRE_EQUAL(dumped_payload, message);
}

// Note: the dump is rate-limited to one per minute, so at most one dump can be observed
// in a single process. Hence the two variants below have to live in separate test cases
// (the test runner runs each Boost test case in its own process), rather than in a loop.
SEASTAR_THREAD_TEST_CASE(test_dump_message_on_fingerprint_from_initial_config) {
    run_dump_message_on_fingerprint_test(false);
}

SEASTAR_THREAD_TEST_CASE(test_dump_message_on_fingerprint_after_live_update) {
    run_dump_message_on_fingerprint_test(true);
}

SEASTAR_THREAD_TEST_CASE(test_tracker_basic_sanity) {
    for (const bool checksumming : {false, true})
    for (const auto& zstd_cpu_limit : {0.0, 1.0}) {
        auto cfg = netw::advanced_rpc_compressor::tracker::config{
            .zstd_quota_fraction{zstd_cpu_limit},
            .algo_config = utils::updateable_value<netw::algo_config>{
                {netw::compression_algorithm::type::ZSTD, netw::compression_algorithm::type::LZ4},
            },
            .checksumming = utils::updateable_value<bool>{checksumming},
        };
        tracker_without_clock tracker{cfg};
        auto feature_string = tracker.supported();
        auto server_compressor = tracker.negotiate(feature_string, true, [] { return make_ready_future<>(); });
        auto close_server_compressor = deferred_close(*server_compressor);
        auto client_compressor = tracker.negotiate(server_compressor->name(), false, [] { return make_ready_future<>(); });
        auto close_client_compressor = deferred_close(*client_compressor);

        for (const auto [a, b] : {
            std::make_pair(std::ref(server_compressor), std::ref(client_compressor)),
            std::make_pair(std::ref(client_compressor), std::ref(server_compressor)),
        })
        for (int repeat = 0; repeat < 10; ++repeat) {
            auto message = tests::random::get_bytes(100000) + bytes(size_t(100000), bytes::value_type(0));
            constexpr int head_space = 4;
            auto compressed = a->compress(head_space, rpc::snd_buf{bytes_view_to_temporary_buffer(message)});

            compressed.front().trim_front(head_space);
            compressed.size -= head_space;

            // Mess with the header deserializer by prepending an empty fragment to `compressed`.
            if (auto src = std::get_if<temporary_buffer<char>>(&compressed.bufs)) {
                auto vec = std::vector<temporary_buffer<char>>();
                vec.push_back(std::move(*src));
                compressed.bufs = std::move(vec);
            }
            auto &vec = std::get<std::vector<temporary_buffer<char>>>(compressed.bufs);
            vec.insert(vec.begin(), temporary_buffer<char>());

            auto decompressed = b->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(compressed)));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_tracker_dict_sanity) {
    for (const auto& algo : {netw::compression_algorithm::type::ZSTD, netw::compression_algorithm::type::LZ4}) {
        auto cfg = netw::advanced_rpc_compressor::tracker::config{
            .zstd_quota_fraction{1.0},
            .algo_config = utils::updateable_value<netw::algo_config>{
                {netw::compression_algorithm::type::RAW},
            },
        };
        tracker_without_clock tracker{cfg};
        auto feature_string = tracker.supported();
        auto server_compressor = tracker.negotiate(feature_string, true, [] { return make_ready_future<>(); });
        auto close_server_compressor = deferred_close(*server_compressor);
        auto client_compressor = tracker.negotiate(server_compressor->name(), false, [] { return make_ready_future<>(); });
        auto close_client_compressor = deferred_close(*client_compressor);

        auto message = tests::random::get_bytes(8192);
        auto message_view = std::span<const std::byte>(reinterpret_cast<const std::byte*>(message.data()), message.size());
        // We will send messages which perfectly match the dict.
        // If dict negotiation succeeds as expected, this should result in very small messages.
        auto dict = make_dict(1, {message_view.begin(), message_view.end()});
        tracker.announce_dict(dict);
        tracker.set_supported_algos(netw::compression_algorithm_set::singleton(algo));

        // Arbitrary number of repeats.
        for (int repeat = 0; repeat < 10; ++repeat)
        for (const auto [a, b] : {
            std::make_pair(std::ref(server_compressor), std::ref(client_compressor)),
            std::make_pair(std::ref(client_compressor), std::ref(server_compressor)),
        }) {
            constexpr int head_space = 4;
            auto compressed = a->compress(head_space, rpc::snd_buf{bytes_view_to_temporary_buffer(message)});
            // The dict negotiation should have settled after a few repeats.
            if (repeat >= 5) {
                // `100` here is an arbitrary "small size".
                BOOST_REQUIRE_LE(compressed.size, 100);
            }
            compressed.front().trim_front(head_space);
            compressed.size -= head_space;
            auto decompressed = b->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(compressed)));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_tracker_cpu_limit_shortterm) {
    constexpr int quota = 10;
    constexpr int quota_refresh = 128;
    auto cfg = netw::advanced_rpc_compressor::tracker::config{
        .zstd_quota_fraction{float(quota) / quota_refresh},
        .zstd_quota_refresh_ms{quota_refresh},
        .algo_config = utils::updateable_value<netw::algo_config>{
            {netw::compression_algorithm::type::ZSTD, netw::compression_algorithm::type::LZ4},
        },
    };
    
    struct manual_clock_tracker : netw::advanced_rpc_compressor::tracker_with_clock<manual_clock, manual_clock> {
        using tracker_with_clock::tracker_with_clock;
        virtual uint64_t get_steady_nanos() const override {
            manual_clock::advance(std::chrono::milliseconds(1));
            return std::chrono::nanoseconds(manual_clock::now().time_since_epoch()).count();
        }
    };
    manual_clock_tracker tracker{cfg};
    
    auto feature_string = tracker.supported();
    auto server_compressor = tracker.negotiate(feature_string, true, [] { return make_ready_future<>(); });
    auto close_server_compressor = deferred_close(*server_compressor);
    auto client_compressor = tracker.negotiate(server_compressor->name(), false, [] { return make_ready_future<>(); });
    auto close_client_compressor = deferred_close(*client_compressor);

    // Settle negotiations
    for (int i = 0; i < 3; ++i) {
        auto msg = server_compressor->compress(0, rpc::snd_buf{0});
        client_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(msg)));
        msg = client_compressor->compress(0, rpc::snd_buf{0});
        server_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(msg)));
    }
    // Refresh quotas.
    manual_clock::advance(std::chrono::milliseconds(1000));

    for (int repeat = 0; repeat < 5; ++repeat) {
        auto before = tracker.get_stats()[netw::compression_algorithm(netw::compression_algorithm::type::ZSTD).idx()];
        // Do many compressions.
        for (int i = 0; i < 30; ++i) {
            client_compressor->compress(0, rpc::snd_buf{0});
        }
        // Check that the quota is respected.
        auto after = tracker.get_stats()[netw::compression_algorithm(netw::compression_algorithm::type::ZSTD).idx()];
        BOOST_REQUIRE_EQUAL(std::chrono::nanoseconds(after.compression_cpu_nanos - before.compression_cpu_nanos), std::chrono::milliseconds(quota));
        // Refresh quotas.
        manual_clock::advance(std::chrono::milliseconds(1000));
    }
}

SEASTAR_THREAD_TEST_CASE(test_tracker_cpu_limit_longterm) {
    constexpr static auto step = std::chrono::milliseconds(10);
    constexpr static auto limit = 0.1;
    auto cfg = netw::advanced_rpc_compressor::tracker::config{
        .zstd_quota_fraction{1},
        .zstd_quota_refresh_ms{1},
        .zstd_longterm_quota_fraction{limit},
        .zstd_longterm_quota_refresh_ms{1000},
        .algo_config = utils::updateable_value<netw::algo_config>{
            {netw::compression_algorithm::type::ZSTD, netw::compression_algorithm::type::LZ4},
        },
    };
    
    struct manual_clock_tracker : netw::advanced_rpc_compressor::tracker_with_clock<manual_clock, manual_clock> {
        using tracker_with_clock::tracker_with_clock;
        virtual uint64_t get_steady_nanos() const override {
            manual_clock::advance(step);
            return std::chrono::nanoseconds(manual_clock::now().time_since_epoch()).count();
        }
    };
    manual_clock_tracker tracker{cfg};
    
    auto feature_string = tracker.supported();
    auto server_compressor = tracker.negotiate(feature_string, true, [] { return make_ready_future<>(); });
    auto close_server_compressor = deferred_close(*server_compressor);
    auto client_compressor = tracker.negotiate(server_compressor->name(), false, [] { return make_ready_future<>(); });
    auto close_client_compressor = deferred_close(*client_compressor);

    // Settle negotiations
    for (int i = 0; i < 3; ++i) {
        auto msg = server_compressor->compress(0, rpc::snd_buf{0});
        client_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(msg)));
        msg = client_compressor->compress(0, rpc::snd_buf{0});
        server_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(std::move(msg)));
    }

    constexpr int n_compressions = 1000;
    auto used_before = tracker.get_stats()[netw::compression_algorithm(netw::compression_algorithm::type::ZSTD).idx()];
    auto clock_before = manual_clock::now();
    // Do many compressions.
    for (int i = 0; i < n_compressions; ++i) {
        client_compressor->compress(0, rpc::snd_buf{0});
    }
    // Check that the quota is respected.
    auto used_after = tracker.get_stats()[netw::compression_algorithm(netw::compression_algorithm::type::ZSTD).idx()];
    auto clock_after = manual_clock::now();

    auto used = std::chrono::nanoseconds(used_after.compression_cpu_nanos - used_before.compression_cpu_nanos);
    auto elapsed = clock_after - clock_before;

    BOOST_REQUIRE_GE(used, elapsed * limit * 0.9);
    BOOST_REQUIRE_LE(used, elapsed * limit * 1.1);
}
