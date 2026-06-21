/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/shared_future.hh>

#include "replica/logstor/ondisk.hh"
#include "schema/schema_fwd.hh"
#include "types.hh"
#include "timeout_config.hh"

namespace replica {

namespace logstor {

class segment_manager;
class logstor_group;

struct write_target {
    logstor_group* cg = nullptr;
    seastar::gate::holder cg_holder;
};

// Writer for log records that handles serialization and size computation
class log_record_writer {

    using ostream = seastar::simple_memory_output_stream;

    log_record _record;
    mutable std::optional<size_t> _header_size;
    mutable std::optional<size_t> _data_size;

    void compute_sizes() const;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    // Get serialized sizes (computed lazily)
    size_t header_size() const {
        if (!_header_size) {
            compute_sizes();
        }
        return *_header_size;
    }

    size_t data_size() const {
        if (!_data_size) {
            compute_sizes();
        }
        return *_data_size;
    }

    // Total serialized content size (header + data)
    size_t size() const {
        return header_size() + data_size();
    }

    // Write the record to an output stream
    void write(ostream& out) const;

    const log_record& record() const {
        return _record;
    }
};

using log_location_with_holder = std::tuple<log_location, seastar::gate::holder>;

// Serializes one in-memory logstor buffer.
//
// Callers append log records one by one and then seal the buffer with a target
// segment sequence number before writing the resulting bytes out. The serialized
// layout is:
//   buffer_header
//   (segment_header)?                 // for segment_kind::full only
//   record_header + log_record_header + canonical_mutation
//   ...
//   zero padding to the requested final alignment
//
// Each record payload is aligned to record_alignment. For full buffers,
// the segment header stores the owning table and the min/max token range of the
// appended records. This type is serialization-only and is used directly by tests
// and internally by write_buffer.
class raw_write_buffer {
public:

    using ostream = seastar::simple_memory_output_stream;

    struct append_result {
        size_t record_header_offset;
        size_t total_size;
    };

private:

    using aligned_buffer_type = std::unique_ptr<char[], free_deleter>;

    size_t _buffer_size;
    aligned_buffer_type _buffer;
    segment_kind _segment_kind;
    ostream _stream;
    ondisk::buffer_header _buffer_header;
    ostream _header_stream;
    ostream _segment_header_stream;

    size_t _net_data_size{0};
    size_t _record_count{0};
    std::optional<dht::token> _min_token;
    std::optional<dht::token> _max_token;

    bool _sealed{false};

public:

    raw_write_buffer(size_t buffer_size, segment_kind kind);

    void reset();

    raw_write_buffer(const raw_write_buffer&) = delete;
    raw_write_buffer& operator=(const raw_write_buffer&) = delete;

    raw_write_buffer(raw_write_buffer&&) noexcept = default;
    raw_write_buffer& operator=(raw_write_buffer&&) noexcept = default;

    const char* data() const noexcept { return _buffer.get(); }
    size_t serialized_size() const noexcept { return offset_in_buffer(); }

    size_t get_buffer_size() const noexcept { return _buffer_size; }
    size_t offset_in_buffer() const noexcept { return _buffer_size - _stream.size(); }

    bool can_fit(size_t data_size) const noexcept;

    bool can_fit(const log_record_writer& writer) const noexcept {
        return can_fit(writer.size());
    }

    bool has_data() const noexcept;

    size_t max_record_size() const noexcept;

    size_t net_data_size() const noexcept { return _net_data_size; }
    size_t record_count() const noexcept { return _record_count; }
    segment_kind kind() const noexcept { return _segment_kind; }

    append_result append(const log_record_writer& writer);
    size_t sealed_size(size_t alignment) const noexcept;

    static size_t estimate_required_segments(size_t net_data_size, size_t record_count, size_t segment_size);

    bool with_segment_header() const noexcept {
        return _segment_kind == segment_kind::full;
    }

    size_t header_size() const noexcept {
        size_t s = ondisk::buffer_header_size;
        if (with_segment_header()) {
            s += ondisk::segment_header_size;
        }
        return s;
    }

    static bool validate_header(const ondisk::buffer_header& bh) {
        return ondisk::validate_header(bh);
    }

    static bool validate_record_header(const ondisk::record_header& rh) {
        return ondisk::validate_record_header(rh);
    }

    void seal(segment_sequence segment_seq, std::optional<table_id> table, size_t alignment);

private:

    // table is set for segment_kind::full
    void write_header(segment_sequence segment_seq, std::optional<table_id> table);

    void pad_to_alignment(size_t alignment);
    void finalize(size_t alignment);

    friend class write_buffer;
};

// Tracks asynchronous write completion on top of raw_write_buffer.
//
// This is the buffer type used by the segment manager, buffered_writer,
// compaction, and separator flows. write() appends a record to the underlying
// raw buffer and returns a future that resolves to the final log_location once
// the buffer is flushed. The returned gate holder keeps the buffer alive for
// follow-up work such as index updates. For mixed buffers it also keeps copies
// of appended records so separator rewriting can replay them after the flush.
class write_buffer {
public:
    struct record_in_buffer {
        log_record_writer writer;
        future<log_location> loc;
        write_target target;
    };

private:
    raw_write_buffer _raw;
    shared_promise<log_location> _written;
    seastar::gate _write_gate;

    std::vector<record_in_buffer> _records_copy;

public:

    write_buffer(size_t buffer_size, segment_kind kind);

    void reset();

    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    future<> close();

    const char* data() const noexcept { return _raw.data(); }
    size_t serialized_size() const noexcept { return _raw.serialized_size(); }

    size_t get_buffer_size() const noexcept { return _raw.get_buffer_size(); }
    size_t offset_in_buffer() const noexcept { return _raw.offset_in_buffer(); }

    bool can_fit(size_t data_size) const noexcept { return _raw.can_fit(data_size); }
    bool can_fit(const log_record_writer& writer) const noexcept { return _raw.can_fit(writer); }
    bool has_data() const noexcept { return _raw.has_data(); }

    size_t max_record_size() const noexcept { return _raw.max_record_size(); }
    size_t net_data_size() const noexcept { return _raw.net_data_size(); }
    size_t record_count() const noexcept { return _raw.record_count(); }

    size_t sealed_size(size_t alignment) {
        return _raw.sealed_size(alignment);
    }

    void seal(segment_sequence segment_seq, std::optional<table_id> table, size_t alignment) {
        _raw.seal(segment_seq, table, alignment);
    }

    // Write a record to the buffer.
    // Returns a future that will be resolved with the log location once flushed and a gate holder
    // that keeps the write buffer open. The gate should be held for index updates after the write
    // is done.
    future<log_location_with_holder> write(log_record_writer, write_target target = {});

private:
    bool with_record_copy() const noexcept {
        return _raw.kind() == segment_kind::mixed;
    }

    std::vector<record_in_buffer> take_separator_records();

    /// Complete all tracked writes with their locations when the buffer is flushed to base_location
    future<> complete_writes(log_location base_location);
    future<> abort_writes(std::exception_ptr);

    friend class segment_manager_impl;
};

// Manages a fixed-size circular ring of write_buffers.
//
// Writers append to the head buffer.  A single consumer coroutine drains the
// tail.  The head advances when the current head buffer is full (can't fit the
// next write) or when the consumer seals it.  Writers wait if the ring is full
// (all buffers are pending flush).
class buffered_writer {
    // Number of buffers in the ring.  Must be >= 2 (one head + at least one
    // that can be in-flight with the consumer).
    static constexpr size_t ring_size = 5;

    segment_manager& _sm;
    seastar::scheduling_group _flush_sg;

    // The ring of buffers, indexed modulo ring_size.
    std::vector<write_buffer> _ring;

    // Monotonically increasing indices; the actual slot is idx % ring_size.
    // _head: next slot writers append to.
    // _tail: next slot the consumer will flush.
    // Invariant: _head >= _tail && _head - _tail < ring_size.
    size_t _head{0};
    size_t _tail{0};

    // Notified when _tail advances (a slot becomes free for the head to move into)
    // or when the head buffer is switched.
    seastar::condition_variable _head_can_advance;

    // Notified when data is written to the head buffer (consumer may wake up).
    seastar::condition_variable _tail_can_advance;

    seastar::gate _async_gate;

    // The single flush-consumer fiber, running for the lifetime of the writer.
    future<> _consumer{make_ready_future<>()};

    write_buffer& head_buf() noexcept { return _ring[_head % ring_size]; }
    write_buffer& tail_buf() noexcept { return _ring[_tail % ring_size]; }

    // The ring is full when all ring_size slots are occupied. Advancing the
    // head further would make the new head slot collide with the tail slot.
    bool ring_full() const noexcept { return _head - _tail == ring_size - 1; }

public:
    explicit buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg);

    buffered_writer(const buffered_writer&) = delete;
    buffered_writer& operator=(const buffered_writer&) = delete;

    future<> start();
    future<> stop();

    future<log_location_with_holder> write(log_record, db::timeout_clock::time_point timeout, write_target target = {});

private:
    // The flush consumer loop.
    future<> consumer_loop();
};

}
}
