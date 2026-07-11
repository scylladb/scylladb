/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/decompressing_cursor.hh"

#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <seastar/core/byteorder.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/memory-data-source.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"
#include "test/lib/reader_concurrency_semaphore.hh"

namespace {

class noop_compressor final : public compressor {
public:
    size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        if (input_len > output_len) {
            throw std::runtime_error("output buffer too small");
        }
        std::memcpy(output, input, input_len);
        return input_len;
    }

    size_t compress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        return uncompress(input, input_len, output, output_len);
    }

    size_t compress_max_size(size_t input_len) const override {
        return input_len;
    }

    algorithm get_algorithm() const override {
        return algorithm::none;
    }
};

std::vector<char> make_chunk(std::string_view payload, uint32_t uncompressed_chunk_length) {
    // On-disk layout: [header][compressed data][checksum][footer], with the
    // header and footer both holding this chunk's compressed length.
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    const auto frame_len = codec.frame_len(payload.size());
    std::vector<char> chunk(codec.extent(payload.size()));
    codec.write_frames(chunk.data(), payload.size());
    std::copy(payload.begin(), payload.end(), chunk.begin() + frame_len);
    // The checksum covers the header and the compressed data, not the footer.
    const auto checksum = crc32_utils::checksum(chunk.data(), frame_len + payload.size());
    write_be<uint32_t>(chunk.data() + frame_len + payload.size(), checksum);
    return chunk;
}

temporary_buffer<char> make_file(std::span<const std::string_view> payloads, uint32_t uncompressed_chunk_length) {
    std::vector<std::vector<char>> chunks;
    size_t size = 0;
    for (auto payload : payloads) {
        chunks.push_back(make_chunk(payload, uncompressed_chunk_length));
        size += chunks.back().size();
    }

    temporary_buffer<char> file(size);
    size_t pos = 0;
    for (const auto& chunk : chunks) {
        std::copy(chunk.begin(), chunk.end(), file.get_write() + pos);
        pos += chunk.size();
    }
    return file;
}

std::string to_string(const temporary_buffer<char>& buf) {
    return std::string(buf.get(), buf.size());
}

// Returns `data` in pieces of `piece_size` bytes. If `async`, each piece
// becomes available only after a task switch. Then a consume() on an
// input_stream with an empty buffer calls its consumer after it returned.
class piecewise_data_source final : public data_source_impl {
    temporary_buffer<char> _data;
    size_t _piece_size;
    bool _async;
public:
    piecewise_data_source(temporary_buffer<char> data, size_t piece_size, bool async)
        : _data(std::move(data))
        , _piece_size(piece_size)
        , _async(async)
    {}

    virtual future<temporary_buffer<char>> get() override {
        auto piece = _data.share(0, std::min(_piece_size, _data.size()));
        _data.trim_front(piece.size());
        if (!_async) {
            return make_ready_future<temporary_buffer<char>>(std::move(piece));
        }
        return yield().then([piece = std::move(piece)] () mutable {
            return std::move(piece);
        });
    }
};

input_stream<char> make_piecewise_stream(temporary_buffer<char> data, size_t piece_size, bool async) {
    return input_stream<char>(data_source(std::make_unique<piecewise_data_source>(std::move(data), piece_size, async)));
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_replaces_hint_with_exact_extent) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto first_exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[0].size());
    const auto second_exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[1].size());
    // An index gives the first chunk a hint that reaches the end of its
    // 512-byte block.
    const auto start = sstables::sstable_position::from_physical(0, 512, 1);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(start,
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(start);

    auto first_result = input->consume_one(std::nullopt, [&] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "bcde");
        auto pos = input->compute_relative_position(-2).as_physical();
        BOOST_REQUIRE_EQUAL(pos.chunk_position, 0);
        BOOST_REQUIRE_EQUAL(pos.extent_hint, first_exact_extent);
        BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 3);
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(first_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->skip(2).get();
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.extent_hint, second_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 2);

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "hijk");
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(second_result.get()));
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, first_exact_extent);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 2);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_caps_hint_at_file_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto first_exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[0].size());
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(sstables::sstable_position::from_physical(first_exact_extent, 512, 0),
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(sstables::sstable_position::from_physical(first_exact_extent, 512, 0));

    auto result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fghijk");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    BOOST_REQUIRE_EQUAL(pos.extent_hint, 0);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_skip_to_preserves_cached_chunk) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcdefgh"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[0].size());
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(sstables::sstable_position::from_physical(0, exact_extent, 0),
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, 0));

    auto first_result = input->consume_one(
            sstables::sstable_position::from_physical(0, exact_extent, 2),
            [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "ab");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(first_result.get()));

    input->skip_to(
            sstables::sstable_position::from_physical(0, exact_extent, 5)).get();

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fgh");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(second_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_reuses_chunk_after_remainder_at_chunk_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcdefgh"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto file_size = file.size();
    const auto exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[0].size());
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(sstables::sstable_position::from_physical(0, exact_extent, 5),
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, 5));

    auto first_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "fgh");
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(first_result.get()));
    auto pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, 0);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 5);

    input->skip_to(
            sstables::sstable_position::from_physical(0, exact_extent, 6)).get();

    auto second_result = input->consume_one(std::nullopt, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "gh");
        return continue_consuming{};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<continue_consuming>(second_result.get()));
    pos = input->stream_position().position.as_physical();
    BOOST_REQUIRE_EQUAL(pos.chunk_position, file_size);
    BOOST_REQUIRE_EQUAL(pos.offset_within_chunk, 0);

    input->close().get();
}

SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_stops_at_range_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk", "lmn"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    const auto first_extent = codec.extent(payloads[0].size());
    const auto second_extent = codec.extent(payloads[1].size());
    const auto file_size = file.size();
    // The range ends at the start of the third chunk. The source ends there
    // too, as a bounded sstable source does.
    const auto range_end = sstables::sstable_position::from_physical(first_extent + second_extent, 0, 0);
    file.trim(first_extent + second_extent);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(sstables::sstable_position::from_physical(0, 0, 0), range_end),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(sstables::sstable_position::from_physical(0, 0, 0));

    // As the row parser does for a skipped row: consume part of a buffer, then
    // skip past it, across a chunk, exactly to the end of the range.
    auto first_result = input->consume_one(range_end, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE_EQUAL(to_string(data), "abcde");
        data.trim_front(2);
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(first_result.get()));
    input->skip(3 + payloads[1].size()).get();
    BOOST_REQUIRE(input->stream_position().position == range_end);

    // The parser reads once more. It must get an empty buffer, which means end
    // of stream, and not an error from reading the third chunk.
    auto second_result = input->consume_one(range_end, [] (temporary_buffer<char> data) {
        BOOST_REQUIRE(data.empty());
        return stop_consuming<char>{std::move(data)};
    }).get();
    BOOST_REQUIRE(std::holds_alternative<stop_consuming<char>>(second_result.get()));
    BOOST_REQUIRE(input->stream_position().position == range_end);

    input->close().get();
}

// In-chunk offsets come from the index, which has no checksum. An offset at or
// past the end of the decompressed chunk must give a malformed sstable error,
// both when the chunk is loaded and when the cursor seeks into a loaded chunk.
SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_rejects_offset_outside_chunk) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde"};
    const auto exact_extent = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length).extent(payloads[0].size());
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto make_input = [&] {
        auto file = make_file(payloads, uncompressed_chunk_length);
        const auto file_size = file.size();
        return sstables::make_decompressing_cursor(
                sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                    return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
                }),
                sstables::disk_read_range(sstables::sstable_position::from_physical(0, exact_extent, 0),
                        sstables::sstable_position::from_physical(file_size, 0, 0)),
                compressor,
                sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
                file_size,
                semaphore.make_permit());
    };
    auto consume_all = [] (temporary_buffer<char>) -> consumption_result<char> {
        return continue_consuming{};
    };

    for (int64_t bad_offset : {int64_t(payloads[0].size()), int64_t(uncompressed_chunk_length - 1)}) {
        // The offset is checked when the chunk is loaded.
        auto input = make_input();
        input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, bad_offset));
        BOOST_REQUIRE_THROW(input->consume_one(std::nullopt, consume_all).get(), sstables::malformed_sstable_exception);
        input->close().get();

        // The offset is checked when the cursor seeks into the loaded chunk.
        input = make_input();
        input->init_stream_position(sstables::sstable_position::from_physical(0, exact_extent, 0));
        input->consume_one(sstables::sstable_position::from_physical(0, exact_extent, 2), consume_all).get();
        BOOST_REQUIRE_THROW(input->skip_to(sstables::sstable_position::from_physical(0, exact_extent, bad_offset)).get(),
                sstables::malformed_sstable_exception);
        input->close().get();
    }
}

// framed_chunk_stream_reader reads every chunk correctly, wherever the buffers
// of the stream end. If the buffer holds at least max_frame_len() bytes at the
// start of a chunk, the reader can leave the header in the stream (the fast
// path). With fewer bytes, it must consume the header (the slow path), and
// reassemble the chunk. With an asynchronous source, consume() calls the
// consumer of a peek at an empty buffer after the fast path returned.
SEASTAR_THREAD_TEST_CASE(test_framed_chunk_stream_reader_buffer_boundaries) {
    constexpr uint32_t uncompressed_chunk_length = 1u << 16;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    BOOST_REQUIRE_EQUAL(codec.max_frame_len(), 3);
    // Chunks with 1-, 2- and 3-byte frames.
    const std::string payloads[] = {"abcde", std::string(200, 'b'), std::string(20000, 'c'), "", std::string(130, 'e')};
    const std::vector<std::string_view> views(std::begin(payloads), std::end(payloads));
    auto file = make_file(views, uncompressed_chunk_length);

    for (bool async : {false, true}) {
        for (size_t piece_size : {size_t(1), size_t(2), size_t(3), size_t(4), size_t(5), size_t(7), size_t(11), size_t(4096), file.size()}) {
            BOOST_TEST_CONTEXT("async=" << async << " piece_size=" << piece_size) {
                sstables::framed_chunk_stream_reader reader(codec);
                auto in = make_piecewise_stream(file.share(), piece_size, async);
                uint64_t pos = 0;
                unsigned fast_path_headers = 0;
                for (const auto& payload : payloads) {
                    BOOST_TEST_INFO("chunk at " << pos);
                    const auto header = reader.read_header(in, pos).get();
                    BOOST_REQUIRE_EQUAL(header.frame.compressed_len, payload.size());
                    BOOST_REQUIRE_EQUAL(header.frame.len, codec.frame_len(payload.size()));
                    // The bytes in the buffer of the stream at the start of the
                    // chunk. An empty buffer is refilled with a whole piece.
                    const uint64_t buffered = std::min<uint64_t>(piece_size - pos % piece_size, file.size() - pos);
                    if (buffered < codec.max_frame_len()) {
                        BOOST_REQUIRE(header.consumed);
                    }
                    fast_path_headers += !header.consumed;
                    const auto chunk = reader.read_chunk(in, header).get();
                    const auto expected = make_chunk(payload, uncompressed_chunk_length);
                    BOOST_REQUIRE(to_string(chunk) == std::string(expected.begin(), expected.end()));
                    pos += chunk.size();
                }
                BOOST_REQUIRE_EQUAL(pos, file.size());
                // The stream ends where the next header would start.
                BOOST_REQUIRE_THROW(reader.read_header(in, pos).get(), sstables::malformed_sstable_exception);
                if (piece_size >= 4096) {
                    BOOST_REQUIRE_GT(fast_path_headers, 0);
                }
                in.close().get();
            }
        }
    }
}

// If the stream ends inside a header, read_header() throws. If it ends inside
// the rest of the chunk, read_chunk() returns a shorter buffer.
SEASTAR_THREAD_TEST_CASE(test_framed_chunk_stream_reader_truncated) {
    constexpr uint32_t uncompressed_chunk_length = 1u << 16;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    const std::string payloads[] = {"abcde", std::string(200, 'b')};
    const std::vector<std::string_view> views(std::begin(payloads), std::end(payloads));
    auto file = make_file(views, uncompressed_chunk_length);
    const uint64_t second_start = codec.extent(payloads[0].size());
    BOOST_REQUIRE_EQUAL(codec.frame_len(payloads[1].size()), 2);

    for (size_t piece_size : {size_t(1), file.size()}) {
        BOOST_TEST_CONTEXT("piece_size=" << piece_size) {
            sstables::framed_chunk_stream_reader reader(codec);

            // The stream ends after the first byte of the second header.
            auto in_header = make_piecewise_stream(file.share(0, second_start + 1), piece_size, false);
            reader.read_chunk(in_header, reader.read_header(in_header, 0).get()).get();
            BOOST_REQUIRE_THROW(reader.read_header(in_header, second_start).get(), sstables::malformed_sstable_exception);
            in_header.close().get();

            // The stream ends one byte before the end of the second chunk.
            auto in_body = make_piecewise_stream(file.share(0, file.size() - 1), piece_size, false);
            reader.read_chunk(in_body, reader.read_header(in_body, 0).get()).get();
            const auto header = reader.read_header(in_body, second_start).get();
            BOOST_REQUIRE_EQUAL(reader.read_chunk(in_body, header).get().size(), file.size() - 1 - second_start);
            in_body.close().get();
        }
    }
}
