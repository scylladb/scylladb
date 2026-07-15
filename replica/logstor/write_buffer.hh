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
#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/timed_out_error.hh>

#include "replica/exceptions.hh"
#include "replica/logstor/ondisk.hh"
#include "schema/schema_fwd.hh"
#include "types.hh"
#include "timeout_config.hh"

namespace replica {

class compaction_group;

namespace logstor {

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

struct buffered_write_result {
    future<log_location_with_holder> persisted;
};

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
    raw_write_buffer _raw;
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
    future<log_location_with_holder> write(log_record_writer, compaction_group*, seastar::gate::holder cg_holder);

    future<log_location_with_holder> write(log_record_writer writer) {
        return write(std::move(writer), nullptr, {});
    }

    // Complete all tracked writes with their locations when the buffer is flushed to base_location
    future<> complete_writes(log_location base_location);
    future<> abort_writes(std::exception_ptr);

private:
    bool with_record_copy() const noexcept {
        return _raw.kind() == segment_kind::mixed;
    }

    std::vector<record_in_buffer>& records_for_separator();

    friend class buffered_writer;
    friend class segment_manager_impl;
};

// Configuration passed to buffered_writer constructor (excluding the flush function).
struct buffered_writer_config {
    size_t buffer_size;
    size_t ring_size;
    seastar::scheduling_group flush_sg;
    size_t max_queued_write_bytes{0};
    std::chrono::milliseconds sync_period{0};
};

// Manages a circular ring of write_buffers.
//
// Writers append to the head buffer.  A single consumer coroutine drains the
// tail.  The head advances when the current head buffer is full (can't fit the
// next write) or when the consumer seals it.  Writers wait if the ring is full
// (all buffers are pending flush).
class buffered_writer {
    seastar::scheduling_group _flush_sg;
    size_t _buffer_size;
    size_t _ring_size;
    std::chrono::milliseconds _sync_period;
    seastar::noncopyable_function<future<>(write_buffer&)> _flush_func;

    // The ring of buffers, indexed modulo _ring_size.
    std::vector<write_buffer> _ring;

    // Monotonically increasing indices; the actual slot is idx % _ring_size.
    // _head: next slot writers append to.
    // _dispatch_tail: next slot the consumer will dispatch via _flush_func.
    // _tail: oldest slot not yet safe to reuse.
    // Invariant: _head >= _dispatch_tail >= _tail && _head - _tail < _ring_size.
    size_t _head{0};
    size_t _dispatch_tail{0};
    size_t _tail{0};

    struct in_flight_write {
        std::optional<future<>> completion;

        bool idle() const noexcept {
            return !completion;
        }

        bool ready() const noexcept {
            return completion && completion->available();
        }

        void start(future<> f) {
            completion.emplace(std::move(f));
        }

        future<> take_completion() {
            auto f = std::move(*completion);
            completion.reset();
            return f;
        }

        void reset() noexcept {
            completion.reset();
        }
    };

    std::vector<in_flight_write> _in_flight;

    struct queued_write {
        seastar::promise<buffered_write_result> accepted_pr; // written to buffer
        seastar::promise<log_location_with_holder> persisted_pr; // written to a segment
        log_record_writer writer;
        compaction_group* cg;
        seastar::gate::holder cg_holder;
        db::timeout_clock::time_point timeout;
        uint64_t id;
        size_t write_size;

        queued_write(log_record_writer writer, compaction_group* cg, seastar::gate::holder cg_holder, db::timeout_clock::time_point timeout, uint64_t id, size_t write_size)
            : writer(std::move(writer))
            , cg(cg)
            , cg_holder(std::move(cg_holder))
            , timeout(timeout)
            , id(id)
            , write_size(write_size) {
        }

        void fail_timeout() noexcept {
            auto ep = std::make_exception_ptr(seastar::timed_out_error{});
            accepted_pr.set_exception(ep);
            persisted_pr.set_exception(ep);
        }
    };

    struct on_queued_write_expiry {
        buffered_writer* owner{};

        void operator()(queued_write& w) noexcept {
            owner->on_queued_write_removed(w);
            w.fail_timeout();
            owner->on_queued_writes_changed();
        }
    };

    // Wakes the consumer whenever a state change may let it make forward
    // progress again: queued writes arrive, the head buffer gains data or
    // advances, a flush completes, a tail is reclaimed, or shutdown begins.
    seastar::condition_variable _consumer_progress_cv;

    // Notified when queued writes are removed or expired, so flush() can wait
    // for the pre-flush queue boundary to advance.
    seastar::condition_variable _queued_writes_changed;

    // Notified when the tail is advanced by the consumer.
    seastar::condition_variable _tail_advanced;

    seastar::expiring_fifo<queued_write, on_queued_write_expiry, db::timeout_clock> _queued_writes;
    size_t _queued_write_bytes{0};
    size_t _max_queued_write_bytes{0};

    seastar::timer<db::timeout_clock> _head_flush_timer;
    bool _head_deadline_expired = false;

    // Monotonically increasing id assigned to queued writes. flush() snapshots
    // this counter and waits until all queued writes with lower ids are gone.
    uint64_t _next_queued_write_id = 0;

    seastar::gate _async_gate;

    // The single flush-consumer fiber, running for the lifetime of the writer.
    future<> _consumer{make_ready_future<>()};

    auto&& head_buf(this auto&& self) noexcept { return self._ring[self._head % self._ring_size]; }
    auto&& tail_buf(this auto&& self) noexcept { return self._ring[self._tail % self._ring_size]; }

    auto&& tail_write(this auto&& self) noexcept { return self._in_flight[self._tail % self._ring_size]; }
    auto&& dispatch_tail_write(this auto&& self) noexcept { return self._in_flight[self._dispatch_tail % self._ring_size]; }

    // The ring is full when all _ring_size slots are occupied. Advancing the
    // head further would make the new head slot collide with the tail slot.
    bool ring_full() const noexcept { return _head - _tail == _ring_size - 1; }

    bool has_pending_buffers() const noexcept;
    bool should_rotate_head_for_flush() const noexcept;
    bool maybe_advance_head() noexcept;

    std::optional<future<log_location_with_holder>> append_to_head_buffer(log_record_writer&, compaction_group*, seastar::gate::holder);

    bool try_dispatch_next_buffer();
    future<> run_dispatched_write(size_t idx);
    future<bool> reclaim_completed_tails();

    future<bool> drain_queued_writes();

    void on_queued_write_removed(const queued_write&) noexcept;
    void fail_queued_write(queued_write&, std::exception_ptr) noexcept;
    void on_queued_writes_changed() noexcept;

    void arm_head_flush_timer();
    void on_head_flush_timer() noexcept;
    void cancel_head_flush_timer() noexcept;

    future<> consumer_loop();

public:
    explicit buffered_writer(buffered_writer_config cfg,
            seastar::noncopyable_function<future<>(write_buffer&)> flush_func);

    buffered_writer(const buffered_writer&) = delete;
    buffered_writer& operator=(const buffered_writer&) = delete;

    future<> start();
    future<> stop();
    future<> flush();

    future<buffered_write_result> write_to_buffer(log_record_writer, db::timeout_clock::time_point timeout, compaction_group* cg = nullptr, seastar::gate::holder cg_holder = {});
    future<log_location_with_holder> write(log_record_writer, db::timeout_clock::time_point timeout, compaction_group* cg = nullptr, seastar::gate::holder cg_holder = {});

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }

};

}
}
