/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/decompressing_cursor.hh"

#include <algorithm>
#include <array>
#include <optional>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <boost/test/unit_test.hpp>
#include <seastar/core/byteorder.hh>
#include <seastar/core/file.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>
#include <seastar/util/memory-data-source.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/compress.hh"
#include "sstables/compressor.hh"
#include "sstables/file_cursor.hh"
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

// Writes a footer that holds `len` just before `chunk_end`.
void write_footer(const sstables::framed_chunk_codec& codec, char* chunk_end, uint64_t len) {
    std::array<char, sstables::max_chunk_frame_len> header;
    codec.write_header(header.data(), len);
    const auto frame_len = codec.frame_len(len);
    std::reverse_copy(header.begin(), header.begin() + frame_len, chunk_end - frame_len);
}

// The size of the payload of a chunk with extent `extent`, if there is one.
// The frame size depends on the payload size, so not every extent is possible.
std::optional<size_t> payload_for_extent(const sstables::framed_chunk_codec& codec, uint64_t extent) {
    for (uint32_t frame_len = 1; frame_len <= codec.max_frame_len(); ++frame_len) {
        const uint64_t overhead = 2 * frame_len + sizeof(uint32_t);
        if (extent >= overhead && codec.frame_len(extent - overhead) == frame_len) {
            return extent - overhead;
        }
    }
    return std::nullopt;
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

// A file in memory. It records the interval of the file that each read
// returns.
class recording_file_impl final : public file_impl {
    temporary_buffer<char> _data;
    std::vector<std::pair<uint64_t, uint64_t>>& _reads;

    [[noreturn]] void unsupported() {
        throw std::logic_error("unsupported operation");
    }
public:
    recording_file_impl(temporary_buffer<char> data, std::vector<std::pair<uint64_t, uint64_t>>& reads, unsigned alignment)
        : _data(std::move(data))
        , _reads(reads)
    {
        _memory_dma_alignment = alignment;
        _disk_read_dma_alignment = alignment;
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, io_intent*) override {
        BOOST_REQUIRE_EQUAL(offset % _disk_read_dma_alignment, 0);
        BOOST_REQUIRE_EQUAL(size % _disk_read_dma_alignment, 0);
        const uint64_t begin = std::min<uint64_t>(offset, _data.size());
        const uint64_t end = std::min<uint64_t>(offset + size, _data.size());
        _reads.emplace_back(offset, offset + size);
        temporary_buffer<uint8_t> buf(end - begin);
        std::copy_n(_data.get() + begin, end - begin, buf.get_write());
        return make_ready_future<temporary_buffer<uint8_t>>(std::move(buf));
    }

    virtual future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    virtual future<size_t> read_dma(uint64_t, void*, size_t, io_intent*) override { unsupported(); }
    virtual future<size_t> read_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    virtual future<> flush() override { unsupported(); }
    virtual future<> truncate(uint64_t) override { unsupported(); }
    virtual future<> discard(uint64_t, uint64_t) override { unsupported(); }
    virtual future<> allocate(uint64_t, uint64_t) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(); }
    virtual future<struct stat> stat() override { unsupported(); }
    virtual future<uint64_t> size() override { return make_ready_future<uint64_t>(_data.size()); }
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override { unsupported(); }
    virtual future<> close() override { return make_ready_future<>(); }
};

constexpr unsigned test_dma_alignment = 512;
constexpr uint64_t max_io_size = 128 * 1024;

file make_recording_file(temporary_buffer<char> data, std::vector<std::pair<uint64_t, uint64_t>>& reads) {
    return file(seastar::make_shared<recording_file_impl>(std::move(data), reads, test_dma_alignment));
}

// Checks that no I/O is larger than 128 KiB, and that no byte of the file was
// read twice. `file_size` is the size of the file: reads past its end return
// nothing, so they can overlap.
void check_reads(std::vector<std::pair<uint64_t, uint64_t>> reads, uint64_t file_size) {
    for (auto [begin, end] : reads) {
        BOOST_TEST_INFO("read [" << begin << ", " << end << ")");
        BOOST_REQUIRE_LE(end - begin, max_io_size);
    }
    std::ranges::sort(reads);
    for (size_t i = 1; i < reads.size(); ++i) {
        BOOST_TEST_INFO("reads [" << reads[i - 1].first << ", " << reads[i - 1].second << ") and ["
                << reads[i].first << ", " << reads[i].second << ")");
        BOOST_REQUIRE_LE(std::min(reads[i - 1].second, file_size), reads[i].first);
    }
}

std::unique_ptr<sstables::decompressing_cursor> make_bidirectional_cursor(file f, uint64_t file_size,
        const compressor& compressor, const sstables::framed_chunk_codec& codec, reader_permit permit) {
    return sstables::make_bidirectional_decompressing_cursor(std::move(f), file_size,
            sstables::disk_read_range(sstables::sstable_position::from_physical(0, 0, 0),
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor, codec, std::move(permit));
}

// Reads backwards from the current position of `input` to the start of the
// file. Returns the bytes in file order.
std::string read_backwards_to_start(sstables::decompressing_cursor& input) {
    std::string result;
    while (true) {
        auto data = input.read_backwards_up_to(1000);
        auto buf = data.get();
        if (buf.empty()) {
            return result;
        }
        result.insert(0, to_string(buf));
    }
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

std::string concat(std::span<const std::string> payloads) {
    std::string result;
    for (const auto& payload : payloads) {
        result += payload;
    }
    return result;
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

// A reader whose end is at a chunk boundary, before the end of the range,
// reads until it gets an empty buffer, as
// physical_reversing_cursor::read_forwards() does. The cursor must not load
// the chunk after the end to return that empty buffer. Here that chunk is
// corrupt, so loading it fails the checksum check.
SEASTAR_THREAD_TEST_CASE(test_decompressing_cursor_does_not_load_chunk_at_reader_end) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const std::string_view payloads[] = {"abcde", "fghijk"};
    auto file = make_file(payloads, uncompressed_chunk_length);
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    const auto first_extent = codec.extent(payloads[0].size());
    const auto file_size = file.size();
    file.get_write()[first_extent + codec.frame_len(payloads[1].size())] ^= 1;
    const auto reader_end = sstables::sstable_position::from_physical(first_extent, 0, 0);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto input = sstables::make_decompressing_cursor(
            sstables::decompressing_cursor_source_opener([file = std::move(file)] () mutable {
                return make_ready_future<data_source>(data_source(std::make_unique<seastar::util::temporary_buffer_data_source>(std::move(file))));
            }),
            sstables::disk_read_range(sstables::sstable_position::from_physical(0, 0, 0),
                    sstables::sstable_position::from_physical(file_size, 0, 0)),
            compressor,
            sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length),
            file_size,
            semaphore.make_permit());
    input->init_stream_position(sstables::sstable_position::from_physical(0, 0, 0));

    std::string result;
    bool at_end = false;
    while (!at_end) {
        input->consume_one(reader_end, [&] (temporary_buffer<char> data) -> consumption_result<char> {
            at_end = data.empty();
            result += to_string(data);
            return continue_consuming{};
        }).get();
    }
    BOOST_REQUIRE_EQUAL(result, "abcde");
    BOOST_REQUIRE(input->stream_position().position == reader_end);

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

// A file_cursor never issues an I/O larger than 128 KiB, even for a larger
// read from an unaligned position, and it never reads a byte twice.
SEASTAR_THREAD_TEST_CASE(test_file_cursor_limits_io_size) {
    constexpr uint64_t file_size = 1024 * 1024;
    constexpr uint64_t read_size = 200 * 1024;
    temporary_buffer<char> data(file_size);
    for (uint64_t i = 0; i < file_size; ++i) {
        data.get_write()[i] = char(i * 7 + i / 251);
    }
    const auto expected = std::string(data.get(), data.size());
    tests::reader_concurrency_semaphore_wrapper semaphore;

    for (bool forward : {true, false}) {
        BOOST_TEST_CONTEXT("forward=" << forward) {
            std::vector<std::pair<uint64_t, uint64_t>> reads;
            sstables::file_cursor cursor(make_recording_file(data.share(), reads), file_size, semaphore.make_permit());
            const uint64_t start = forward ? 1001 : file_size - 1001;
            cursor.seek(start);
            auto buf = forward
                    ? cursor.read_forwards_exactly(read_size, file_size).get()
                    : cursor.read_backwards_exactly(read_size).get();
            BOOST_REQUIRE_EQUAL(buf.size(), read_size);
            const uint64_t buf_start = forward ? start : start - read_size;
            BOOST_REQUIRE(to_string(buf) == expected.substr(buf_start, read_size));
            check_reads(reads, file_size);
            cursor.close().get();
        }
    }
}

// The checksum of a chunk doesn't cover its footer, so the cursor checks that
// the footer holds the same length as the header. A read in either direction
// rejects a chunk whose footer doesn't.
SEASTAR_THREAD_TEST_CASE(test_bidirectional_cursor_rejects_mismatched_footer) {
    constexpr uint32_t uncompressed_chunk_length = 8;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    const std::string_view payloads[] = {"abcde", "fghijk"};
    for (bool forward : {true, false}) {
        BOOST_TEST_CONTEXT("forward=" << forward) {
            auto file = make_file(payloads, uncompressed_chunk_length);
            const uint64_t file_size = file.size();
            // The footer of the chunk that the read loads first.
            const uint64_t chunk_end = forward ? codec.extent(payloads[0].size()) : file_size;
            const size_t payload_size = forward ? payloads[0].size() : payloads[1].size();
            write_footer(codec, file.get_write() + chunk_end, payload_size - 1);

            std::vector<std::pair<uint64_t, uint64_t>> reads;
            auto input = make_bidirectional_cursor(make_recording_file(std::move(file), reads), file_size,
                    compressor, codec, semaphore.make_permit());
            if (forward) {
                input->init_stream_position(sstables::sstable_position::from_physical(0, 0, 0));
                BOOST_REQUIRE_THROW(input->consume_one(std::nullopt, [] (temporary_buffer<char>) {
                    return continue_consuming{};
                }).get(), sstables::malformed_sstable_exception);
            } else {
                input->seek(sstables::sstable_position::from_physical(file_size, 0, 0));
                BOOST_REQUIRE_THROW(input->read_backwards_up_to(1).get(), sstables::malformed_sstable_exception);
            }
            input->close().get();
        }
    }
}

// Reads a file backwards, chunk by chunk, from its end. The first backward I/O
// starts `k` bytes before the end of the second to last chunk, so the buffer
// holds only `k` bytes of that chunk. If `k` is less than the largest frame,
// the cursor reads the footer through the slow path. If `k` is also less than
// the footer, the footer starts in one I/O buffer and ends in the next one.
// The footer has 2 or 3 bytes. No byte may be read twice.
SEASTAR_THREAD_TEST_CASE(test_bidirectional_cursor_reads_footers_across_buffers_once) {
    constexpr uint32_t uncompressed_chunk_length = 1u << 16;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    BOOST_REQUIRE_EQUAL(codec.max_frame_len(), 3);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    for (size_t straddling_size : {1000, 20000}) {
        for (uint64_t k = 1; k <= codec.max_frame_len(); ++k) {
            BOOST_TEST_CONTEXT("straddling_size=" << straddling_size << " k=" << k) {
                // The file size is a multiple of 512, so the first backward I/O,
                // of 4096 bytes, starts 4096 bytes before the end of the file. The
                // last chunk has an extent of 4096 - k bytes. A padding chunk at
                // the start of the file sets the file size.
                const std::string last(payload_for_extent(codec, 4096 - k).value(), 'z');
                const std::string straddling(straddling_size, 's');
                const uint64_t tail_extent = codec.extent(straddling.size()) + codec.extent(last.size());
                uint64_t padding_extent = (test_dma_alignment - tail_extent % test_dma_alignment) % test_dma_alignment;
                while (!payload_for_extent(codec, padding_extent)) {
                    padding_extent += test_dma_alignment;
                }
                const std::string padding(*payload_for_extent(codec, padding_extent), 'p');
                const std::string payloads[] = {padding, straddling, last};
                const std::vector<std::string_view> views(std::begin(payloads), std::end(payloads));
                auto file = make_file(views, uncompressed_chunk_length);
                const uint64_t file_size = file.size();
                BOOST_REQUIRE_EQUAL(file_size % test_dma_alignment, 0);
                BOOST_REQUIRE_EQUAL(file_size - codec.extent(last.size()), file_size - 4096 + k);

                std::vector<std::pair<uint64_t, uint64_t>> reads;
                auto input = make_bidirectional_cursor(make_recording_file(std::move(file), reads), file_size,
                        compressor, codec, semaphore.make_permit());
                input->seek(sstables::sstable_position::from_physical(file_size, 0, 0));
                BOOST_REQUIRE(read_backwards_to_start(*input) == concat(payloads));
                check_reads(reads, file_size);
                input->close().get();
            }
        }
    }
}

// Reads a file backwards from the middle of a chunk. The extent hint of the
// position reaches past the end of the chunk, as a hint from the index can.
// No byte may be read twice.
SEASTAR_THREAD_TEST_CASE(test_bidirectional_cursor_reads_backwards_from_chunk_middle_once) {
    constexpr uint32_t uncompressed_chunk_length = 1u << 16;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    const std::string payloads[] = {std::string(3000, 'a'), std::string(100, 'b'), std::string(5000, 'c'), std::string(20000, 'd')};
    const std::vector<std::string_view> views(std::begin(payloads), std::end(payloads));
    auto file = make_file(views, uncompressed_chunk_length);
    const uint64_t file_size = file.size();
    const uint64_t third_start = codec.extent(payloads[0].size()) + codec.extent(payloads[1].size());
    const int32_t hint = codec.extent(payloads[2].size()) + 300;

    std::vector<std::pair<uint64_t, uint64_t>> reads;
    auto input = make_bidirectional_cursor(make_recording_file(std::move(file), reads), file_size,
            compressor, codec, semaphore.make_permit());
    input->seek(sstables::sstable_position::from_physical(third_start, hint, 2000));
    const auto expected = concat(payloads);
    BOOST_REQUIRE(read_backwards_to_start(*input) == expected.substr(0, payloads[0].size() + payloads[1].size() + 2000));
    check_reads(reads, file_size);
    input->close().get();
}

// Reads a file forwards through the bidirectional cursor. The first I/O ends
// `k` bytes after the start of a chunk, so the buffer holds only `k` bytes of
// that chunk. If `k` is less than the largest frame, the cursor reads the
// header through the slow path. If `k` is also less than the header, the
// header starts in one I/O buffer and ends in the next one. The header has 2
// or 3 bytes. No byte may be read twice, and no I/O may be larger than
// 128 KiB.
SEASTAR_THREAD_TEST_CASE(test_bidirectional_cursor_reads_headers_across_buffers_once) {
    constexpr uint32_t uncompressed_chunk_length = 1u << 16;
    const auto codec = sstables::framed_chunk_codec::for_framed_chunks(uncompressed_chunk_length);
    BOOST_REQUIRE_EQUAL(codec.max_frame_len(), 3);
    noop_compressor compressor;
    tests::reader_concurrency_semaphore_wrapper semaphore;

    for (size_t straddling_size : {1000, 20000}) {
        for (uint64_t k = 1; k <= codec.max_frame_len(); ++k) {
            BOOST_TEST_CONTEXT("straddling_size=" << straddling_size << " k=" << k) {
                // The first forward I/O covers [0, 128 KiB). The straddling chunk
                // starts k bytes before its end.
                const std::string big(60000, 'a');
                const uint64_t padding_extent = max_io_size - k - 2 * codec.extent(big.size());
                const std::string padding(payload_for_extent(codec, padding_extent).value(), 'p');
                const std::string payloads[] = {big, big, padding, std::string(straddling_size, 's'),
                        std::string(100, 'b'), big, big};
                const std::vector<std::string_view> views(std::begin(payloads), std::end(payloads));
                auto file = make_file(views, uncompressed_chunk_length);
                const uint64_t file_size = file.size();

                std::vector<std::pair<uint64_t, uint64_t>> reads;
                auto input = make_bidirectional_cursor(make_recording_file(std::move(file), reads), file_size,
                        compressor, codec, semaphore.make_permit());
                input->init_stream_position(sstables::sstable_position::from_physical(0, 0, 0));
                std::string result;
                while (true) {
                    bool eof = false;
                    input->consume_one(std::nullopt, [&] (temporary_buffer<char> data) {
                        eof = data.empty();
                        result += to_string(data);
                        return continue_consuming{};
                    }).get();
                    if (eof) {
                        break;
                    }
                }
                BOOST_REQUIRE(result == concat(payloads));
                check_reads(reads, file_size);
                input->close().get();
            }
        }
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
