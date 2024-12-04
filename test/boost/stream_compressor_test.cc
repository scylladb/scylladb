/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE core
#include <seastar/util/alloc_failure_injector.hh>
#include "utils/stream_compressor.hh"
#include "test/lib/random_utils.hh"
#include <boost/test/unit_test.hpp>
#include "bytes.hh"

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

// Compute some interesting splits of a message into pieces.
// Pieces of size 0, 1, and whole message are particularly interesting -- they should cover most edge cases.
template <RpcBuf Buf>
std::vector<Buf> compute_splits(bytes b) {
    std::vector<Buf> out;
    {
        // Mostly 1-byte pieces, some 0-byte pieces.
        std::vector<temporary_buffer<char>> frags;
        frags.push_back(temporary_buffer<char>());
        for (size_t i = 0; i < b.size();) {
            for (size_t k = std::min(b.size(), i + 2); i < k; ++i) {
                frags.push_back(temporary_buffer<char>(reinterpret_cast<const char*>(&b[i]), 1));
            }
            frags.push_back(temporary_buffer<char>());
        }
        out.emplace_back(std::move(frags), b.size());
    }
    {
        // Mostly 0-byte pieces, some 1-byte pieces.
        std::vector<temporary_buffer<char>> frags;
        frags.push_back(temporary_buffer<char>());
        for (size_t i = 0; i < b.size(); ++i) {
            frags.push_back(temporary_buffer<char>(reinterpret_cast<const char*>(&b[i]), 1));
            for (size_t k = 0; k < 2; ++k) {
                frags.push_back(temporary_buffer<char>());
            }
        }
        out.emplace_back(std::move(frags), b.size());
    }
    {
        // Whole message.
        out.emplace_back(temporary_buffer<char>(reinterpret_cast<const char*>(b.data()), b.size()));
    }
    return out;
}

void test_compressor_pair_basic_correctness(utils::stream_compressor& compressor, utils::stream_decompressor& decompressor) {
    // Generate some messages.
    for (const auto& message : {
        tests::random::get_bytes(0),
        tests::random::get_bytes(1),
        tests::random::get_bytes(2),
        tests::random::get_bytes(2000),
    })
    // Test both with and without streaming.
    for (bool end_of_frame : {false, true})
    // Split input into pieces.
    for (const auto& input : compute_splits<rpc::snd_buf>(message))
    // Test chunk sizes smaller, equal, and larger to the message.
    for (const auto& chunk_size : {1, 5000})
    // Use each compressor multiple times to make sure it is returned to a proper state after each use.
    for (int repeat = 0; repeat < 3; ++repeat)
    {
        auto compressed = utils::compress_impl(0, input, compressor, end_of_frame, chunk_size);
        auto rcv_buf = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
        auto decompressed = utils::decompress_impl(rcv_buf, decompressor, end_of_frame, chunk_size);
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(std::move(decompressed)));
    }
}

BOOST_AUTO_TEST_CASE(test_correctness) {
    {
        utils::raw_stream stream;
        test_compressor_pair_basic_correctness(stream, stream);
    }
    {
        utils::lz4_dstream dstream{};
        utils::lz4_cstream cstream{};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
    {
        utils::lz4_dstream dstream{2};
        utils::lz4_cstream cstream{2};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
    {
        utils::zstd_dstream dstream{};
        utils::zstd_cstream cstream{};
        test_compressor_pair_basic_correctness(cstream, dstream);
    }
}

void test_recovery_after_oom_one(utils::stream_decompressor& dstream, utils::stream_compressor& cstream) {
    // Check that compressors and decompressors handle OOM properly and can be reused afterwards.
    for (int repeat = 0; repeat < 3; ++repeat) {
        auto message = tests::random::get_bytes(256);
        rpc::snd_buf compressed;
        {
            auto message_buf = rpc::snd_buf(temporary_buffer<char>(reinterpret_cast<const char*>(message.data()), message.size()));
            memory::with_allocation_failures([&] {
                try {
                    compressed = utils::compress_impl(0, message_buf, cstream, true, 64);
                } catch (const std::runtime_error&) {
                    throw std::bad_alloc();
                }
            });
        }
        rpc::rcv_buf decompressed;
        {
            auto compressed_2 = convert_rpc_buf<rpc::rcv_buf>(std::move(compressed));
            memory::with_allocation_failures([&] {
                try {
                    decompressed = utils::decompress_impl(compressed_2, dstream, true, 64);
                } catch (const std::runtime_error&) {
                    throw std::bad_alloc();
                }
            });
        }
        BOOST_REQUIRE_EQUAL(message, rpc_buf_to_bytes(decompressed));
    }
}

BOOST_AUTO_TEST_CASE(test_recovery_after_oom) {
    {
        utils::lz4_dstream dstream{};
        utils::lz4_cstream cstream{};
        test_recovery_after_oom_one(dstream, cstream);
    }
    {
        utils::zstd_dstream dstream{};
        utils::zstd_cstream cstream{};
        test_recovery_after_oom_one(dstream, cstream);
    }
}
