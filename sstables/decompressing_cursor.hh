/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/util/noncopyable_function.hh>

#include "reader_permit.hh"
#include "sstables/consumer.hh"
#include "sstables/framed_chunk.hh"
#include "sstables/sstable_position.hh"
#include "sstables/shared_sstable.hh"
#include "tracing/trace_state.hh"

class compressor;

namespace sstables {

// Supplies the raw bytes of the framed chunks of a physically indexed Data.db
// to a decompressing_cursor.
class framed_chunk_source {
public:
    enum class direction { forward, backward };

    // The bytes of one chunk. `header` holds the chunk's first frame_len()
    // bytes, and `rest` holds the bytes after them. `rest` can extend past the
    // end of the chunk.
    struct raw_chunk {
        temporary_buffer<char> header;
        temporary_buffer<char> rest;
    };

    virtual ~framed_chunk_source() = default;

    // Reads the chunk that starts at `chunk_start`. `extent_hint` is 0, or an
    // upper bound of the chunk's extent. `dir` is the direction in which the
    // reader moves; it tells the source where to read ahead. When reading
    // forward, the reader needs no data past the file offset `forward_limit`,
    // so the source should not read ahead past it. A source that is already
    // bounded can ignore it.
    virtual future<raw_chunk> read_chunk(uint64_t chunk_start, uint64_t extent_hint, direction dir, uint64_t forward_limit) = 0;

    // Returns the extent of the chunk that ends at `chunk_end`, from its footer.
    virtual future<uint64_t> read_previous_extent(uint64_t chunk_end) = 0;

    virtual future<> close() = 0;
};

// Reads the uncompressed data of a physically indexed, compressed Data.db
// (`mu`). Its positions are physical: a chunk position and an offset in the
// uncompressed chunk. It doesn't use CompressionInfo.db, because each chunk's
// header and footer give the positions of the next and the previous chunk.
//
// The cursor reads forward through continuous_data_consumer_input_stream. If
// its source supports it, the cursor can also seek backwards and read
// backwards. Then forward and backward reads share one source, and one buffer
// of decompressed data.
//
// The cursor verifies the checksum of each chunk that it loads. If it was
// given a digest, and it loads every chunk of the file in order, it also
// verifies the whole-file digest.
class decompressing_cursor final : public data_consumer::continuous_data_consumer_input_stream {
    struct chunk {
        // The position of the chunk in Data.db.
        uint64_t position;
        // The size of the chunk in Data.db.
        uint64_t extent;
        temporary_buffer<char> data;
    };

    std::unique_ptr<framed_chunk_source> _source;
    framed_chunk_codec _codec;
    const compressor& _compressor;
    reader_permit _permit;
    uint64_t _compressed_file_length;
    reader_position_tracker _stream_position;
    // The end of the range. The source can end with the chunk that contains
    // _end, so the cursor must not load a chunk after it.
    sstable_position _end;
    // The last chunk that the cursor loaded. It is not always the chunk at
    // _stream_position: while a consume_one() consumer runs, the position can
    // be at the end of _chunk, which is the start of the next chunk.
    std::optional<chunk> _chunk;

    // Whole-file digest check, enabled if a digest was passed to the
    // constructor. Each loaded chunk is added to _actual_digest, in file order.
    // _next_digest_chunk_position is the position of the next chunk to add. If
    // the cursor skips a chunk, no later chunk matches this position, and the
    // digest is not checked. Thus, the digest is checked only if the cursor
    // reads all chunks, from the start of the file to its end.
    std::optional<uint32_t> _expected_digest;
    uint32_t _actual_digest;
    uint64_t _next_digest_chunk_position = 0;

    // Whether _chunk is the chunk at _stream_position.
    bool has_current_chunk() const;
    // Loads the chunk at _stream_position. A forward read needs no data past
    // `end`, or past _end if `end` is not given.
    future<> load_chunk(framed_chunk_source::direction dir, std::optional<sstable_position> end);
    void add_to_digest(uint64_t chunk_start, const framed_chunk& chunk, uint32_t checksum);
    future<> ensure_chunk_if_not_eof(std::optional<sstable_position> end);
    // Moves the position by `n` bytes forward within the current chunk. Steps
    // to the next chunk at the end of the current one.
    void advance(uint64_t n);
public:
    decompressing_cursor(std::unique_ptr<framed_chunk_source> source, framed_chunk_codec codec,
            const compressor& compressor, reader_permit permit, uint64_t compressed_file_length,
            disk_read_range range, std::optional<uint32_t> digest);

    // Moves the cursor to `pos`, in either direction. Keeps the loaded chunk if
    // `pos` is in it.
    void seek(sstable_position pos);

    // Reads up to `n` bytes that precede the current position, and moves the
    // position back by their size. Returns an empty buffer at the start of the
    // file. The source must support backward reads.
    future<temporary_buffer<char>> read_backwards_up_to(size_t n);

    // Like read_backwards_up_to(), but reads exactly `n` bytes, or fewer at the
    // start of the file.
    future<temporary_buffer<char>> read_backwards_exactly(size_t n);

    void init_stream_position(sstable_position pos) override;
    future<> skip_to(sstable_position target) override;
    future<> skip(uint64_t n) override;
    // Like skip(n), but the reader needs no data past `end`, so the source
    // doesn't read ahead past the chunk that contains it.
    future<> skip(uint64_t n, std::optional<sstable_position> end);
    future<consumption_result<char>> consume_one(std::optional<sstable_position> end_position, consumer_one_fn consumer) override;
    sstable_position compute_relative_position(int64_t offset) override;
    future<> close() override;
};

using decompressing_cursor_source_opener = noncopyable_function<future<data_source>()>;

// Reads the chunks from range.start to range.end, through a forward-only
// source. The cursor reports the end of the stream after the chunk that
// contains range.end. The source must start at range.start's chunk position,
// aligned down to 512 bytes. It must contain at least every chunk up to
// range.end. The chunks must be framed.
std::unique_ptr<decompressing_cursor>
make_decompressing_cursor(decompressing_cursor_source_opener open_source,
        disk_read_range range, const compressor& compressor,
        const framed_chunk_codec& codec, uint64_t compressed_file_length,
        reader_permit permit, std::optional<uint32_t> digest = std::nullopt);

// Reads `range` of `sst`'s Data.db forward, with the given options.
std::unique_ptr<decompressing_cursor>
make_decompressing_cursor(shared_sstable sst, disk_read_range range,
        reader_permit permit, tracing::trace_state_ptr trace_state,
        file_input_stream_options options, std::optional<uint32_t> digest = std::nullopt);

// Reads `range` of `sst`'s Data.db in both directions, through a file_cursor.
std::unique_ptr<decompressing_cursor>
make_bidirectional_decompressing_cursor(shared_sstable sst, disk_read_range range, reader_permit permit,
        tracing::trace_state_ptr trace_state);

} // namespace sstables
