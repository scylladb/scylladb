/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
#include "types.hh"

namespace replica::logstor {

class segment_manager;

// Writer for log records that handles serialization and size computation
class log_record_writer {

    using ostream = seastar::simple_memory_output_stream;

    log_record _record;
    mutable std::optional<size_t> _size;

    void compute_size() const;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    // Get serialized size (computed lazily)
    size_t size() const {
        if (!_size) {
            compute_size();
        }
        return *_size;
    }

    // Write the record to an output stream
    void write(ostream& out) const;

    const log_record& record() const {
        return _record;
    }
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

    static constexpr size_t record_alignment = 8;
    static constexpr size_t record_header_size = sizeof(uint32_t);

private:

    using aligned_buffer_type = std::unique_ptr<char[], free_deleter>;

    size_t _buffer_size;
    aligned_buffer_type _buffer;
    seastar::simple_memory_output_stream _stream;

    size_t _net_data_size{0};
    size_t _record_count{0};

    shared_promise<log_location> _written;

public:

    write_buffer(size_t buffer_size);

    void reset();

    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    size_t get_buffer_size() const noexcept { return _buffer_size; }
    size_t offset_in_buffer() const noexcept { return _buffer_size - _stream.size(); }

    bool can_fit(size_t data_size) const noexcept;

    bool can_fit(const log_record_writer& writer) const noexcept {
        return can_fit(writer.size());
    }

    bool has_data() const noexcept { return offset_in_buffer() > 0; }

    size_t get_max_write_size() const noexcept;

    size_t get_net_data_size() const noexcept { return _net_data_size; }
    size_t get_record_count() const noexcept { return _record_count; }

    // Write a record to the buffer.
    // Returns a future that will be resolved with the log location once flushed.
    future<log_location> write(log_record_writer);

    static size_t estimate_required_segments(size_t net_data_size, size_t record_count, size_t segment_size);

private:

    const char* data() const noexcept { return _buffer.get(); }

    // Complete all tracked writes with their locations when the buffer is flushed to base_location
    void complete_writes(log_location base_location);
    void abort_writes(std::exception_ptr) noexcept;

    void pad_to_alignment(size_t alignment);
    void finalize(size_t alignment);

    friend class segment_manager_impl;
    friend class compaction_manager;
};

// Manages multiple buffers, a single active buffer and multiple flushing buffers.
// When switch is requested for the active buffer, it waits for a flushing buffer to
// become available, and continuing to accumulate writes until then.
class buffered_writer {
    static constexpr size_t num_flushing_buffers = 4;

    segment_manager& _sm;

    struct active_buffer {
        write_buffer buf;
        bool flush_requested{false};
    } _active_buffer;

    seastar::queue<write_buffer> _available_buffers;
    seastar::gate _async_gate;
    seastar::condition_variable _buffer_switched;
    seastar::scheduling_group _flush_sg;

public:
    explicit buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg);

    buffered_writer(const buffered_writer&) = delete;
    buffered_writer& operator=(const buffered_writer&) = delete;

    future<> start();
    future<> stop();

    future<log_location> write(log_record);

private:
    future<write_buffer> switch_buffer();
    future<> flush(write_buffer);

};

}
