/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/manual_clock.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/closeable.hh>
#include "test/lib/exception_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/scylla_test_case.hh"
#include "message/advanced_rpc_compressor.hh"
#include "message/advanced_rpc_compressor_protocol.hh"

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

// Splits `b` into fragments of random sizes in [0, max_fragment_size], including empty ones.
// read_rcv_buf() returns a frame in the buffers returned by the socket,
// so the fragment boundaries can fall anywhere, also inside a header field.
static rpc::rcv_buf bytes_to_fragmented_rcv_buf(bytes_view b, size_t max_fragment_size) {
    const size_t size = b.size();
    auto frags = std::vector<temporary_buffer<char>>();
    while (!b.empty()) {
        const auto n = std::min(b.size(), tests::random::get_int<size_t>(0, max_fragment_size));
        frags.push_back(bytes_view_to_temporary_buffer(b.substr(0, n)));
        b.remove_prefix(n);
    }
    return rpc::rcv_buf(std::move(frags), size);
}

// Checks that decompress() parses a frame whose fragment boundaries fall anywhere,
// including inside the header and between the header and the payload, for every algorithm,
// with and without checksums. The other tests pass the frame in one fragment
// (test_tracker_basic_sanity adds just an empty one), so they don't exercise this.
SEASTAR_THREAD_TEST_CASE(test_fragmented_frame) {
    // Larger than rpc::snd_buf::chunk_size after compression, for every algorithm.
    const auto message = tests::random::get_bytes(150000) + bytes(size_t(150000), bytes::value_type(0));
    for (const bool checksumming : {false, true})
    for (const auto algo : {netw::compression_algorithm::type::RAW, netw::compression_algorithm::type::LZ4, netw::compression_algorithm::type::ZSTD}) {
        auto cfg = netw::advanced_rpc_compressor::tracker::config{
            .zstd_quota_fraction{1.0},
            .algo_config = utils::updateable_value<netw::algo_config>{{algo}},
            .checksumming = utils::updateable_value<bool>{checksumming},
        };
        tracker_without_clock tracker{cfg};
        auto server_compressor = tracker.negotiate(tracker.supported(), true, [] { return make_ready_future<>(); });
        auto close_server_compressor = deferred_close(*server_compressor);
        auto client_compressor = tracker.negotiate(server_compressor->name(), false, [] { return make_ready_future<>(); });
        auto close_client_compressor = deferred_close(*client_compressor);

        // Let the algorithm negotiation settle.
        for (int i = 0; i < 5; ++i) {
            client_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(server_compressor->compress(0, rpc::snd_buf{0})));
            server_compressor->decompress(convert_rpc_buf<rpc::rcv_buf>(client_compressor->compress(0, rpc::snd_buf{0})));
        }

        for (const auto [a, b] : {
            std::make_pair(std::ref(server_compressor), std::ref(client_compressor)),
            std::make_pair(std::ref(client_compressor), std::ref(server_compressor)),
        })
        for (const size_t max_fragment_size : {7, 1000, 100000}) {
            constexpr int head_space = 4;
            const auto compressed = rpc_buf_to_bytes(a->compress(head_space, rpc::snd_buf{bytes_view_to_temporary_buffer(message)}));
            const auto messages_received_before = tracker.get_stats()[netw::compression_algorithm(algo).idx()].messages_received;
            auto decompressed = b->decompress(bytes_to_fragmented_rcv_buf(bytes_view(compressed).substr(head_space), max_fragment_size));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
            BOOST_REQUIRE_EQUAL(tracker.get_stats()[netw::compression_algorithm(algo).idx()].messages_received, messages_received_before + 1);
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

static bool preempt_always() {
    return true;
}

static bool preempt_never() {
    return false;
}

static netw::advanced_rpc_compressor::tracker::config segmenting_config(
        netw::compression_algorithm::type algo, bool checksumming, bool (*need_preempt)()) {
    return netw::advanced_rpc_compressor::tracker::config{
        .zstd_quota_fraction{1.0},
        .algo_config = utils::updateable_value<netw::algo_config>{{algo}},
        .checksumming = utils::updateable_value<bool>{checksumming},
        .need_preempt = need_preempt,
    };
}

struct compressor_pair {
    std::unique_ptr<netw::advanced_rpc_compressor> server;
    std::unique_ptr<netw::advanced_rpc_compressor> client;
    compressor_pair(netw::advanced_rpc_compressor::tracker& tracker) {
        server = tracker.negotiate(tracker.supported(), true, [] { return make_ready_future<>(); });
        BOOST_REQUIRE(server);
        client = tracker.negotiate(server->name(), false, [] { return make_ready_future<>(); });
        BOOST_REQUIRE(client);
    }
    future<> close() noexcept {
        co_await server->close();
        co_await client->close();
    }
};

constexpr size_t test_head_space = 4;

// Strips the head space from the output of compress().
static bytes strip_head_space(const rpc::snd_buf& compressed) {
    auto b = rpc_buf_to_bytes(compressed);
    return bytes(b.begin() + test_head_space, b.end());
}

static rpc::rcv_buf bytes_to_rcv_buf(bytes_view b) {
    return rpc::rcv_buf(bytes_view_to_temporary_buffer(b));
}

// compress_async() can end a segment only at a fragment boundary,
// so the messages are split into fragments, as RPC does. (Its fragments are at most 128 kiB.)
constexpr size_t test_fragment_size = 30000;

static rpc::snd_buf bytes_to_snd_buf(bytes_view b, size_t fragment_size = test_fragment_size) {
    const size_t size = b.size();
    auto frags = std::vector<temporary_buffer<char>>();
    while (!b.empty()) {
        const auto n = std::min(b.size(), fragment_size);
        frags.push_back(bytes_view_to_temporary_buffer(b.substr(0, n)));
        b.remove_prefix(n);
    }
    return rpc::snd_buf(std::move(frags), size);
}

static bool has_more_segments(bytes_view compressed) {
    return compressed[0] & 0x20;
}

// Exchanges empty messages until the negotiation settles.
static void settle(compressor_pair& p) {
    for (int i = 0; i < 5; ++i) {
        p.client->decompress(bytes_to_rcv_buf(strip_head_space(p.server->compress(test_head_space, rpc::snd_buf(0)))));
        p.server->decompress(bytes_to_rcv_buf(strip_head_space(p.client->compress(test_head_space, rpc::snd_buf(0)))));
    }
}

// The basic contract of segmented messages, in both directions, for every algorithm, with and without checksums:
// - a message is split at fragment boundaries if and only if need_preempt() is true there,
//   so a message of one fragment is never split;
// - both decompress() and decompress_async() decode a split message;
// - compress() splits like compress_async(), so the tests which use compress() see the same wire format;
// - the stats count a split message once, with all of its bytes.
SEASTAR_THREAD_TEST_CASE(test_segmented_round_trip) {
    const auto message = tests::random::get_bytes(150000) + bytes(size_t(150000), bytes::value_type(0));
    for (const bool checksumming : {false, true})
    for (const auto algo : {netw::compression_algorithm::type::RAW, netw::compression_algorithm::type::LZ4, netw::compression_algorithm::type::ZSTD})
    for (const auto need_preempt : {preempt_always, preempt_never}) {
        tracker_without_clock tracker{segmenting_config(algo, checksumming, need_preempt)};
        compressor_pair p(tracker);
        auto close_p = deferred_close(p);
        settle(p);

        for (const auto [a, b] : {
            std::make_pair(p.server.get(), p.client.get()),
            std::make_pair(p.client.get(), p.server.get()),
        }) {
            const auto stats_before = tracker.get_stats();
            const auto sum = [] (std::span<const netw::per_algorithm_stats, netw::compression_algorithm::count()> stats, auto member) {
                uint64_t s = 0;
                for (const auto& x : stats) {
                    s += x.*member;
                }
                return s;
            };
            const auto messages_sent_before = sum(stats_before, &netw::per_algorithm_stats::messages_sent);
            const auto bytes_sent_before = sum(stats_before, &netw::per_algorithm_stats::bytes_sent);
            const auto messages_received_before = sum(stats_before, &netw::per_algorithm_stats::messages_received);
            const auto bytes_received_before = sum(stats_before, &netw::per_algorithm_stats::bytes_received);

            // Async compression, async and sync decompression.
            // The message is split into segments if and only if need_preempt() is true.
            const auto compressed = strip_head_space(a->compress_async(test_head_space, bytes_to_snd_buf(message)).get());
            BOOST_REQUIRE_EQUAL(has_more_segments(compressed), need_preempt == preempt_always);
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(b->decompress_async(bytes_to_rcv_buf(compressed)).get()));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(b->decompress(bytes_to_rcv_buf(compressed))));

            // Each message is counted once, regardless of the number of segments.
            const auto stats_after = tracker.get_stats();
            BOOST_REQUIRE_EQUAL(sum(stats_after, &netw::per_algorithm_stats::messages_sent) - messages_sent_before, 1);
            BOOST_REQUIRE_EQUAL(sum(stats_after, &netw::per_algorithm_stats::bytes_sent) - bytes_sent_before, message.size());
            BOOST_REQUIRE_EQUAL(sum(stats_after, &netw::per_algorithm_stats::messages_received) - messages_received_before, 2);
            BOOST_REQUIRE_EQUAL(sum(stats_after, &netw::per_algorithm_stats::bytes_received) - bytes_received_before, 2 * message.size());

            // Sync compression splits the same way as async compression.
            BOOST_REQUIRE_EQUAL(compressed, strip_head_space(a->compress(test_head_space, bytes_to_snd_buf(message))));

            // A message of one fragment is never split.
            const auto small_message = bytes(message.begin(), message.begin() + test_fragment_size);
            const auto compressed_small = strip_head_space(a->compress_async(test_head_space, bytes_to_snd_buf(small_message)).get());
            BOOST_REQUIRE(!has_more_segments(compressed_small));
            BOOST_REQUIRE_EQUAL(small_message, rpc_buf_to_bytes(b->decompress_async(bytes_to_rcv_buf(compressed_small)).get()));
        }
    }
}

// Like test_fragmented_frame, but with messages split into segments,
// so that segment headers (including the length field, which only split messages have)
// and segment payloads cross fragment boundaries.
SEASTAR_THREAD_TEST_CASE(test_segmented_fragmented_frame) {
    const auto message = tests::random::get_bytes(150000) + bytes(size_t(150000), bytes::value_type(0));
    for (const bool checksumming : {false, true})
    for (const auto algo : {netw::compression_algorithm::type::RAW, netw::compression_algorithm::type::LZ4, netw::compression_algorithm::type::ZSTD}) {
        tracker_without_clock tracker{segmenting_config(algo, checksumming, preempt_always)};
        compressor_pair p(tracker);
        auto close_p = deferred_close(p);
        settle(p);

        for (const auto [a, b] : {
            std::make_pair(p.server.get(), p.client.get()),
            std::make_pair(p.client.get(), p.server.get()),
        })
        for (const size_t max_fragment_size : {7, 1000, 100000}) {
            const auto compressed = strip_head_space(a->compress_async(test_head_space, bytes_to_snd_buf(message)).get());
            BOOST_REQUIRE(has_more_segments(compressed));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(b->decompress_async(bytes_to_fragmented_rcv_buf(compressed, max_fragment_size)).get()));
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(b->decompress(bytes_to_fragmented_rcv_buf(compressed, max_fragment_size))));
        }
    }
}

// Like test_tracker_dict_sanity, but with messages which are always split into segments.
// Only the first segment of a message carries the control frame, but every segment must be
// compressed and decompressed with the dict which the negotiation selected.
// A later segment compressed without the dict would exceed the size limit below,
// and one decompressed with a different dict than it was compressed with would not round-trip.
SEASTAR_THREAD_TEST_CASE(test_segmented_dict_negotiation) {
    for (const auto algo : {netw::compression_algorithm::type::ZSTD, netw::compression_algorithm::type::LZ4}) {
        tracker_without_clock tracker{segmenting_config(algo, true, preempt_always)};
        compressor_pair p(tracker);
        auto close_p = deferred_close(p);

        for (int dict_version = 1; dict_version <= 3; ++dict_version) {
            // LZ4 only uses the first 64 kiB of the dict, so the message must fit in that.
            const auto message = tests::random::get_bytes(60000);
            const auto message_view = std::as_bytes(std::span(message.data(), message.size()));
            // We will send messages which perfectly match the dict.
            // If dict negotiation succeeds as expected, this should result in very small messages.
            tracker.announce_dict(make_dict(dict_version, {message_view.begin(), message_view.end()}));

            // Arbitrary number of repeats.
            for (int repeat = 0; repeat < 10; ++repeat)
            for (const auto [a, b] : {
                std::make_pair(p.server.get(), p.client.get()),
                std::make_pair(p.client.get(), p.server.get()),
            }) {
                const auto compressed = strip_head_space(a->compress_async(test_head_space, bytes_to_snd_buf(message)).get());
                BOOST_REQUIRE(has_more_segments(compressed));
                // The dict negotiation should have settled after a few repeats.
                if (repeat >= 5) {
                    // `500` here is an arbitrary "small size".
                    BOOST_REQUIRE_LE(compressed.size(), 500);
                }
                BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(b->decompress_async(bytes_to_rcv_buf(compressed)).get()));
            }
        }
    }
}

// SCYLLA_V4 sends split messages, which SCYLLA_V3 can't decode. So a peer which only supports
// SCYLLA_V3 must be rejected, so that the connection falls back to some other compressor.
SEASTAR_THREAD_TEST_CASE(test_negotiation) {
    tracker_without_clock tracker{netw::advanced_rpc_compressor::tracker::config{}};
    BOOST_REQUIRE_EQUAL(tracker.supported(), "SCYLLA_V4");
    for (const auto& feature : {"LZ4", "SCYLLA_V3"}) {
        BOOST_REQUIRE(!tracker.negotiate(feature, true, [] { return make_ready_future<>(); }));
        BOOST_REQUIRE(!tracker.negotiate(feature, false, [] { return make_ready_future<>(); }));
    }
}

// The compression contexts are shared by the connections, and compress_async() and decompress_async()
// yield between segments, so other connections can use the contexts in the meantime.
// This checks that nothing is kept in a context across a yield: neither the stream state,
// nor the dict, which each connection plugs in before use.
//
// It compresses and decompresses a message on two connections at once, so that their segments interleave.
// (With preempt_always, both functions yield after every segment.) The connections use the same algorithm,
// but different dicts: the first one has committed the dict, and the second one hasn't yet.
// The message matches the dict, so a segment compressed with the dict references it and is tiny,
// and one compressed without it is big.
SEASTAR_THREAD_TEST_CASE(test_segmented_interleaving) {
    for (const auto algo : {netw::compression_algorithm::type::LZ4, netw::compression_algorithm::type::ZSTD}) {
        tracker_without_clock tracker{segmenting_config(algo, true, preempt_always)};
        compressor_pair p1(tracker);
        auto close_p1 = deferred_close(p1);
        compressor_pair p2(tracker);
        auto close_p2 = deferred_close(p2);
        // Settles the algorithm on both connections.
        settle(p1);
        settle(p2);
        // One fragment, which fits in the 64 kiB of the dict which LZ4 uses.
        const auto dict_content = tests::random::get_bytes(test_fragment_size);
        const auto dict_view = std::as_bytes(std::span(dict_content.data(), dict_content.size()));
        tracker.announce_dict(make_dict(1, {dict_view.begin(), dict_view.end()}));
        // Settles the dict on the first connection only.
        settle(p1);

        // Every fragment, and so every segment, matches the dict.
        const auto message = dict_content + dict_content + dict_content + dict_content;
        auto [compressed_1, compressed_2] = when_all_succeed(
            p1.client->compress_async(test_head_space, bytes_to_snd_buf(message)),
            p2.client->compress_async(test_head_space, bytes_to_snd_buf(message))
        ).get();
        const auto stripped_1 = strip_head_space(compressed_1);
        const auto stripped_2 = strip_head_space(compressed_2);
        BOOST_REQUIRE(has_more_segments(stripped_1));
        BOOST_REQUIRE(has_more_segments(stripped_2));
        // Every segment of the first connection was compressed with the dict.
        // `2000` here is an arbitrary "small size".
        BOOST_REQUIRE_LE(stripped_1.size(), 2000);
        // The second connection doesn't use the dict.
        BOOST_REQUIRE_GE(stripped_2.size(), message.size() / 2);
        // A segment decompressed with the other connection's dict would not round-trip.
        auto [decompressed_1, decompressed_2] = when_all_succeed(
            p1.server->decompress_async(bytes_to_rcv_buf(stripped_1)),
            p2.server->decompress_async(bytes_to_rcv_buf(stripped_2))
        ).get();
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed_1));
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed_2));
    }
}

// The receiver reads the dict of a message right after the control frame of the first segment,
// and uses it for all segments. A control frame in a later segment would change the dict
// in the middle of the message, so the receiver must reject it. This checks that it does,
// with the dedicated error: a misparsed frame would usually fail anyway, with some other error.
SEASTAR_THREAD_TEST_CASE(test_control_frame_in_later_segment_is_rejected) {
    tracker_without_clock tracker{segmenting_config(netw::compression_algorithm::type::LZ4, false, preempt_always)};
    compressor_pair p(tracker);
    auto close_p = deferred_close(p);
    settle(p);

    // compress_async() can end a segment only at a fragment boundary,
    // so the message must have more than one fragment.
    const auto message = tests::random::get_bytes(100000);
    auto compressed = strip_head_space(p.client->compress_async(test_head_space, bytes_to_snd_buf(message)).get());
    // After settle(), the first segment has no control frame, so its header is
    // the header byte and the length of the segment.
    BOOST_REQUIRE_EQUAL(compressed[0] & 0xe0, 0x20);
    const size_t first_segment_length = seastar::read_le<uint32_t>(reinterpret_cast<const char*>(&compressed[1]));
    const size_t second_segment_offset = 1 + sizeof(uint32_t) + first_segment_length;
    BOOST_REQUIRE_LT(second_segment_offset, compressed.size());
    compressed[second_segment_offset] |= 0x80;
    BOOST_REQUIRE_EXCEPTION(p.server->decompress_async(bytes_to_rcv_buf(compressed)).get(), std::runtime_error,
        exception_predicate::message_contains("Control frame in a non-first segment"));
}

// With RPC's fragments of 128 kiB, each segment decompresses to an exact multiple of 128 kiB,
// which fills its last output chunk exactly. RPC reads the decompressed message
// through an input_stream, which treats an empty fragment as the end of the stream,
// so the decompressed message must not contain any empty fragments.
SEASTAR_THREAD_TEST_CASE(test_segmented_no_empty_fragments) {
    const auto message = tests::random::get_bytes(3 * rpc::snd_buf::chunk_size);
    for (const auto algo : {netw::compression_algorithm::type::RAW, netw::compression_algorithm::type::LZ4, netw::compression_algorithm::type::ZSTD}) {
        tracker_without_clock tracker{segmenting_config(algo, true, preempt_always)};
        compressor_pair p(tracker);
        auto close_p = deferred_close(p);
        settle(p);

        const auto compressed = strip_head_space(p.client->compress_async(test_head_space, bytes_to_snd_buf(message, rpc::snd_buf::chunk_size)).get());
        BOOST_REQUIRE(has_more_segments(compressed));
        const auto check = [&] (const rpc::rcv_buf& decompressed) {
            BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
            if (auto* frags = std::get_if<std::vector<temporary_buffer<char>>>(&decompressed.bufs)) {
                for (const auto& frag : *frags) {
                    BOOST_REQUIRE(!frag.empty());
                }
            }
        };
        check(p.server->decompress_async(bytes_to_rcv_buf(compressed)).get());
        check(p.server->decompress(bytes_to_rcv_buf(compressed)));
    }
}
