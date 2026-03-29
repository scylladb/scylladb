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
#include "schema/schema_fwd.hh"
#include "types.hh"
#include "serializer.hh"
#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"

namespace replica {

class compaction_group;

namespace logstor {

class segment_manager;

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

enum class segment_kind : uint8_t {
    mixed = 0,
    full = 1,
};

// Manages a single aligned buffer for accumulating records and writing
// them to the segment manager.
//
// usage:
//
// create write buffer with specified size:
//     write_buffer wb(buffer_size);
// write data to the buffer if fits and get a future for the log location when flushed:
//     log_record_writer writer(record);
//     auto loc_fut = wb.write(writer);
// flush the buffer to the segment manager:
//     co_await sm.write(wb);
// await individual write locations:
//     auto record_loc = co_await std::move(loc_fut);
class write_buffer {
public:

    using ostream = seastar::simple_memory_output_stream;

    // buffer: buffer_header | (segment_header)? | record_1 | ... | record_n | 0-padding
    // record: record_header | record_data | 0-padding
    //
    // buffer_header, segment_header and record are aligned by record_alignment
    // they have explicit sizes and serialization below
    // segment_header exists when the segment_kind is segment_kind::full.

    static constexpr uint32_t buffer_header_magic = 0x4c475342;
    static constexpr size_t record_alignment = 8;
    static constexpr uint8_t current_version = 1;

    struct buffer_header {
        uint32_t magic;
        uint32_t data_size; // size of all records data following the buffer_header
        segment_generation seg_gen;
        segment_kind kind;
        uint8_t version;
        uint32_t crc;

        uint32_t calculate_crc() const;
    };
    static constexpr size_t buffer_header_size =
        2 * sizeof(uint32_t)
        + sizeof(segment_generation::underlying)
        + sizeof(std::underlying_type_t<segment_kind>)
        + sizeof(uint8_t)
        + sizeof(uint32_t);
    static_assert(buffer_header_size % record_alignment == 0, "Buffer header size must be aligned by record_alignment");

    struct segment_header {
        table_id table;
        dht::token first_token;
        dht::token last_token;
    };
    static constexpr size_t segment_header_size =
        sizeof(table_id)
        + 2 * sizeof(int64_t);
    static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

    struct record_header {
        uint32_t header_size; // size of the serialized log_record_header
        uint32_t data_size;   // size of the serialized canonical_mutation
    };
    static constexpr size_t record_header_size = 2 * sizeof(uint32_t);

private:

    using aligned_buffer_type = std::unique_ptr<char[], free_deleter>;

    size_t _buffer_size;
    aligned_buffer_type _buffer;
    segment_kind _segment_kind;
    seastar::simple_memory_output_stream _stream;
    buffer_header _buffer_header;
    seastar::simple_memory_output_stream _header_stream;
    seastar::simple_memory_output_stream _segment_header_stream;

    size_t _net_data_size{0};
    size_t _record_count{0};
    std::optional<dht::token> _min_token;
    std::optional<dht::token> _max_token;

    shared_promise<log_location> _written;

    seastar::gate _write_gate;

    struct record_in_buffer {
        log_record_writer writer;
        future<log_location> loc;
        compaction_group* cg;
        seastar::gate::holder cg_holder;
    };

    std::vector<record_in_buffer> _records_copy;

public:

    write_buffer(size_t buffer_size, segment_kind kind);

    void reset();

    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    future<> close();

    size_t get_buffer_size() const noexcept { return _buffer_size; }
    size_t offset_in_buffer() const noexcept { return _buffer_size - _stream.size(); }

    bool can_fit(size_t data_size) const noexcept;

    bool can_fit(const log_record_writer& writer) const noexcept {
        return can_fit(writer.size());
    }

    bool has_data() const noexcept;

    size_t get_max_write_size() const noexcept;

    size_t get_net_data_size() const noexcept { return _net_data_size; }
    size_t get_record_count() const noexcept { return _record_count; }

    // Write a record to the buffer.
    // Returns a future that will be resolved with the log location once flushed and a gate holder
    // that keeps the write buffer open. The gate should be held for index updates after the write
    // is done.
    future<log_location_with_holder> write(log_record_writer, compaction_group*, seastar::gate::holder cg_holder);

    future<log_location_with_holder> write(log_record_writer writer) {
        return write(std::move(writer), nullptr, {});
    }

    static size_t estimate_required_segments(size_t net_data_size, size_t record_count, size_t segment_size);

    bool with_record_copy() const noexcept {
        return _segment_kind == segment_kind::mixed;
    }

    bool with_segment_header() const noexcept {
        return _segment_kind == segment_kind::full;
    }

    size_t header_size() const noexcept {
        size_t s = buffer_header_size;
        if (with_segment_header()) {
            s += segment_header_size;
        }
        return s;
    }

    static void write_empty_header(ostream& out, segment_generation seg_gen);

    static bool validate_header(const buffer_header& bh);

private:

    const char* data() const noexcept { return _buffer.get(); }

    // table is set for segment_kind::full
    void write_header(segment_generation seg_gen, std::optional<table_id> table);

    // get all write records in the buffer.
    // with_record_copy must be to true when creating the write_buffer.
    std::vector<record_in_buffer>& records();

    /// Complete all tracked writes with their locations when the buffer is flushed to base_location
    future<> complete_writes(log_location base_location);
    future<> abort_writes(std::exception_ptr);

    void pad_to_alignment(size_t alignment);
    void finalize(size_t alignment);

    friend class segment_manager_impl;
    friend class compaction_manager_impl;
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

    future<log_location_with_holder> write(log_record, compaction_group* cg = nullptr, seastar::gate::holder cg_holder = {});

private:
    // The flush consumer loop.
    future<> consumer_loop();
};

}
}

namespace ser {

template <>
struct serializer<replica::logstor::write_buffer::buffer_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::write_buffer::buffer_header& h) {
        serializer<uint32_t>::write(out, h.magic);
        serializer<uint32_t>::write(out, h.data_size);
        serializer<replica::logstor::segment_generation>::write(out, h.seg_gen);
        serializer<uint8_t>::write(out, static_cast<uint8_t>(h.kind));
        serializer<uint8_t>::write(out, h.version);
        serializer<uint32_t>::write(out, h.crc);
    }
    template <typename Input>
    static replica::logstor::write_buffer::buffer_header read(Input& in) {
        replica::logstor::write_buffer::buffer_header h;
        h.magic = serializer<uint32_t>::read(in);
        h.data_size = serializer<uint32_t>::read(in);
        h.seg_gen = serializer<replica::logstor::segment_generation>::read(in);
        h.kind = static_cast<replica::logstor::segment_kind>(serializer<uint8_t>::read(in));
        h.version = serializer<uint8_t>::read(in);
        h.crc = serializer<uint32_t>::read(in);
        return h;
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
        serializer<uint32_t>::skip(in);
        serializer<replica::logstor::segment_generation>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint32_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::write_buffer::segment_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::write_buffer::segment_header& h) {
        serializer<table_id>::write(out, h.table);
        serializer<int64_t>::write(out, h.first_token.raw());
        serializer<int64_t>::write(out, h.last_token.raw());
    }
    template <typename Input>
    static replica::logstor::write_buffer::segment_header read(Input& in) {
        replica::logstor::write_buffer::segment_header h;
        h.table = serializer<table_id>::read(in);
        h.first_token = dht::token::from_int64(serializer<int64_t>::read(in));
        h.last_token = dht::token::from_int64(serializer<int64_t>::read(in));
        return h;
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<table_id>::skip(in);
        serializer<int64_t>::skip(in);
        serializer<int64_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::write_buffer::record_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::write_buffer::record_header& h) {
        serializer<uint32_t>::write(out, h.header_size);
        serializer<uint32_t>::write(out, h.data_size);
    }
    template <typename Input>
    static replica::logstor::write_buffer::record_header read(Input& in) {
        replica::logstor::write_buffer::record_header h;
        h.header_size = serializer<uint32_t>::read(in);
        h.data_size = serializer<uint32_t>::read(in);
        return h;
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
        serializer<uint32_t>::skip(in);
    }
};
} // namespace ser
