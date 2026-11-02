/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
#include <iterator>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

#include <seastar/core/fstream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "replica/logstor/ondisk.hh"

namespace replica::logstor {

struct segment_info {
    segment_kind kind;
    segment_sequence segment_seq;

    struct mixed {};
    struct full {
        table_id table;
        dht::token first_token;
        dht::token last_token;
    };

    std::variant<mixed, full> v;
};

// The segment info of the chunk at the start of `in`, or nothing if its headers are not valid.
// The chunk's body is not read, so a torn chunk is told apart only by a segment_scanner.
future<std::optional<segment_info>> read_chunk_info(seastar::input_stream<char>& in);

// Reads a segment through its input stream, and keeps the position in the segment of what it
// reads next.
class segment_reader {
public:
    explicit segment_reader(seastar::input_stream<char>& in) noexcept : _in(in) {}

    size_t position() const noexcept {
        return _position;
    }

    future<temporary_buffer<char>> read_exactly(size_t n) {
        _position += n;
        return _in.read_exactly(n);
    }

    // Reads an on-disk T, or returns nullopt if the stream ends first.
    template <typename T>
    future<std::optional<T>> read() {
        constexpr size_t size = ser::serializer<T>::serialized_size;
        return read_exactly(size).then([] (temporary_buffer<char> buf) -> std::optional<T> {
            if (buf.size() < size) {
                return std::nullopt;
            }
            return ser::deserialize_from_buffer(buf, std::type_identity<T>{});
        });
    }

    // Does nothing if the reader is at or past the position already.
    future<> skip_to(size_t position) {
        if (_position >= position) {
            return make_ready_future<>();
        }
        auto n = position - _position;
        _position = position;
        return _in.skip(n);
    }

private:
    seastar::input_stream<char>& _in;
    size_t _position = 0;
};

// A record of a chunk. The views point into the chunk body.
struct chunk_record : ondisk::parsed_record_frame {
    record_location location;

    log_record to_log_record() const {
        return log_record{header, record_value(bytes.value)};
    }
};

class scanned_chunk;

// The records of a chunk body, in order. The range ends at the first record frame that does not
// parse.
class chunk_records {
public:
    class iterator {
    public:
        using value_type = chunk_record;
        using difference_type = std::ptrdiff_t;

        explicit iterator(const scanned_chunk& chunk);

        const chunk_record& operator*() const noexcept { return *_current; }
        const chunk_record* operator->() const noexcept { return &*_current; }
        iterator& operator++() {
            read_next();
            return *this;
        }
        void operator++(int) { read_next(); }
        bool operator==(std::default_sentinel_t) const noexcept { return !_current; }

    private:
        const scanned_chunk* _chunk;
        // What is left of the record frames after the current one.
        simple_memory_input_stream _records;
        std::optional<chunk_record> _current;

        void read_next();
    };

    explicit chunk_records(const scanned_chunk& chunk) noexcept : _chunk(chunk) {}

    iterator begin() const { return iterator(_chunk); }
    std::default_sentinel_t end() const noexcept { return {}; }

private:
    const scanned_chunk& _chunk;
};

// A chunk with valid headers, of the same use of the segment as the first chunk.
class scanned_chunk {
public:
    const segment_info& info() const noexcept { return _info; }
    // False if the body does not match its body CRC or is cut short. Such a chunk has no records:
    // a torn chunk can hold records of an earlier use of the segment that parse cleanly.
    bool body_valid() const noexcept { return _body_valid; }
    chunk_records records() const noexcept { return chunk_records(*this); }

private:
    log_segment_id _segment_id;
    segment_info _info;
    temporary_buffer<char> _body;
    // Where the body starts in the segment. Record locations are relative to the segment.
    size_t _body_position;
    bool _body_valid;

    friend class segment_scanner;
    friend class chunk_records::iterator;

    // The record frames, or nothing if the body is not valid.
    std::span<const char> record_bytes() const noexcept {
        return _body_valid ? std::span<const char>(_body.get(), _body.size()) : std::span<const char>();
    }

    scanned_chunk(log_segment_id segment_id, segment_info info, temporary_buffer<char> body, size_t body_position, bool body_valid)
        : _segment_id(segment_id)
        , _info(std::move(info))
        , _body(std::move(body))
        , _body_position(body_position)
        , _body_valid(body_valid)
    {}
};

// Scans the chunks of a segment in order. The scanner reads from the stream, and does not close
// it.
//
// The scan stops at the first chunk whose headers are invalid or cut short, or whose segment_seq is
// not the one of the first chunk. That chunk is not returned. A chunk whose body is cut short is
// returned with its info and no records, and the scan stops after it. A chunk whose body fails its
// CRC check is returned with its info and no records, and the scan continues at the next chunk. The
// scan also stops after the only chunk of a full segment.
class segment_scanner {
public:
    segment_scanner(seastar::input_stream<char>& in, log_segment_id segment_id, size_t segment_size);
    segment_scanner(const segment_scanner&) = delete;
    segment_scanner& operator=(const segment_scanner&) = delete;

    // The next chunk, or nullptr at the end of the scan. The chunk, and the views of its records,
    // are valid until the next call.
    future<const scanned_chunk*> next_chunk();

private:
    segment_reader _reader;
    log_segment_id _segment_id;
    size_t _segment_size;
    std::optional<segment_sequence> _segment_seq;
    std::optional<scanned_chunk> _chunk;
    // last: the chunk returned last is the last chunk of the scan. done: next_chunk() returned
    // nullptr.
    enum class scan_state { more, last, done };
    scan_state _state = scan_state::more;
    // The number of chunks whose body does not match its CRC or is cut short by the end of the
    // stream, and whether the last chunk of the scan is one of them.
    size_t _failed_chunks = 0;
    bool _last_chunk_failed = false;

    future<std::optional<scanned_chunk>> read_chunk();
    void report_failed_chunks() const;
};

using streamed_buffer_consumer = std::function<future<>(bytes_view)>;

// Rewrites the initial streamed logstor chunk header to the local segment sequence and
// forwards subsequent bytes unchanged. This preserves the current branch's streaming
// behavior, which only rewrites the first streamed chunk header.
class streamed_segment_rewriter {
    log_segment_id _target_segment;
    segment_sequence _target_seq;
    streamed_buffer_consumer _on_buffer;
    std::vector<char> _pending_data;
    std::optional<size_t> _initial_header_size;
    bool _header_rewritten = false;

    ondisk::chunk_header read_chunk_header() const;
    void maybe_parse_initial_header();
    void rewrite_chunk_header();
    future<> flush_pending_data();

public:
    streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer);

    future<> put(std::span<temporary_buffer<char>> data);
    future<> close();

    size_t buffer_size() const noexcept;
};

}
