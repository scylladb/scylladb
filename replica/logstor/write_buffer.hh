/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <memory>
#include <vector>
#include <seastar/core/abort_source.hh>
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

namespace logstor {

class logstor_group;

struct write_target {
    logstor_group* cg = nullptr;
    // Keeps the group from being reported as empty while the write is in flight: until the record
    // is in a segment and in the index, the group's own state does not account for it, and a tablet
    // split ACKs a pre-split group once it looks empty.
    seastar::gate::holder non_empty_holder;
    // Keeps the group from finishing its stop() while the write is in flight. It is released only
    // when the write target is dropped, which for a separator-eligible record is after the record
    // was handed to the group's separator buffer, so a group that stopped has nothing left to come.
    seastar::gate::holder alive_holder;
};

// Writer for log records that handles serialization and size computation
class log_record_writer {

    using ostream = seastar::simple_memory_output_stream;

    log_record _record;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    size_t header_size() const {
        return ondisk::record_header_size(_record.header);
    }

    size_t value_size() const {
        return _record.value.size();
    }

    // The record without its frame header: the record header and the value.
    size_t record_size() const {
        return header_size() + value_size();
    }

    // Write the record's frame - frame header, record header, value - to an output stream.
    void write_frame(ostream& out) const;

    const log_record& record() const {
        return _record;
    }

    const record_header& header() const {
        return _record.header;
    }
};

// Writer for log records that stores pre-serialized bytes.
// Used in compaction and separator rewriting to avoid deserialization.
class log_record_bytes_writer {

    using ostream = seastar::simple_memory_output_stream;

    record_header _header;
    bytes_view _header_bytes;
    bytes_view _value_bytes;

public:
    log_record_bytes_writer(record_header header, log_record_bytes_view record_view)
        : _header(std::move(header))
        , _header_bytes(record_view.header)
        , _value_bytes(record_view.value)
    {}

    const record_header& header() const { return _header; }

    size_t header_size() const { return _header_bytes.size(); }
    size_t value_size() const { return _value_bytes.size(); }
    size_t record_size() const { return header_size() + value_size(); }

    void write_frame(ostream& out) const;
};

template <typename T>
concept log_record_writer_concept = requires(const T& w, seastar::simple_memory_output_stream& out) {
    { w.header() } -> std::convertible_to<const record_header&>;
    { w.header_size() } -> std::convertible_to<size_t>;
    { w.value_size() } -> std::convertible_to<size_t>;
    { w.record_size() } -> std::convertible_to<size_t>;
    { w.write_frame(out) };
};

using record_location_with_holder = std::tuple<record_location, seastar::gate::holder>;

// Where a record whose frame sits at `frame_offset` of a buffer ends up, once that buffer has been
// written to a segment at `buffer_position`. A buffer learns where it was written once, and the
// location of every record in it follows from that.
inline record_location locate_record(segment_position buffer_position, size_t frame_offset, size_t frame_size) noexcept {
    return record_location {
        .segment = buffer_position.segment,
        .offset = static_cast<uint32_t>(buffer_position.offset + frame_offset),
        .size = static_cast<uint32_t>(frame_size),
    };
}

struct buffered_write_result {
    future<record_location_with_holder> persisted;
};

// Serializes one in-memory logstor buffer.
//
// Callers append log records one by one and then seal the buffer with a target
// segment sequence number before writing the resulting bytes out. The serialized
// layout is:
//   buffer_header
//   (segment_header)?                 // for segment_kind::full only
//   record_frame_header + record_header (fixed fields + partition key) + record value
//   ...
//   zero padding to the requested final alignment
//
// Each record frame is padded to record_alignment. For full buffers,
// the segment header stores the owning table and the min/max token range of the
// appended records. This type is serialization-only and is used directly by tests
// and internally by write_buffer.
class raw_write_buffer {
public:

    using ostream = seastar::simple_memory_output_stream;

    // Where the appended record frame starts in the buffer, and how many bytes it takes
    // without its padding - which is what record_location::size holds.
    struct append_result {
        size_t frame_offset;
        size_t frame_size;
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

    // The frame sizes of the appended records summed, their padding excluded.
    size_t _record_bytes{0};
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

    // The bytes written to the buffer so far, which after seal() is the whole buffer to write out.
    size_t serialized_size() const noexcept { return _buffer_size - _stream.size(); }

    size_t get_buffer_size() const noexcept { return _buffer_size; }

    bool can_fit(size_t record_size) const noexcept;

    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept {
        return can_fit(writer.record_size());
    }

    bool can_fit(size_t header_size, size_t value_size) const noexcept {
        return can_fit(header_size + value_size);
    }

    bool has_data() const noexcept;

    // The largest record - record header and value, the frame header aside - that fits an
    // empty buffer of this size and kind. The kinds differ in what they carry ahead of their
    // records, so a record can fit a buffer of one kind and not of the other.
    static constexpr size_t max_record_size(size_t buffer_size, segment_kind kind) noexcept {
        const size_t overhead = buffer_headers_size(kind) + ondisk::record_frame_header_size;
        return buffer_size > overhead ? buffer_size - overhead : 0;
    }

    // The largest record that fits an empty buffer of this size whatever its kind. A record is
    // written to a segment of one kind and can be rewritten into a segment of the other - the
    // separator rewrites the records of a mixed segment into full segments of their compaction
    // group - so a record that logstor accepts at all has to fit both.
    static constexpr size_t max_record_size_any_kind(size_t buffer_size) noexcept {
        return std::min(max_record_size(buffer_size, segment_kind::mixed),
                        max_record_size(buffer_size, segment_kind::full));
    }

    size_t max_record_size() const noexcept {
        return max_record_size(_buffer_size, _segment_kind);
    }

    size_t record_bytes() const noexcept { return _record_bytes; }
    size_t record_count() const noexcept { return _record_count; }
    segment_kind kind() const noexcept { return _segment_kind; }

    template <log_record_writer_concept Writer>
    append_result append(const Writer& writer);

    size_t sealed_size(size_t alignment) const noexcept;

    // How many segments of this kind `record_count` records of `record_bytes` bytes take.
    static size_t estimate_required_segments(size_t record_bytes, size_t record_count, size_t segment_size, segment_kind);

    bool with_segment_header() const noexcept {
        return _segment_kind == segment_kind::full;
    }

    // What a buffer of this kind carries ahead of its records: the buffer header, and the
    // segment header too for a full segment.
    static constexpr size_t buffer_headers_size(segment_kind kind) noexcept {
        size_t s = ondisk::buffer_header_size;
        if (kind == segment_kind::full) {
            s += ondisk::segment_header_size;
        }
        return s;
    }

    size_t buffer_headers_size() const noexcept {
        return buffer_headers_size(_segment_kind);
    }

    void seal(segment_sequence segment_seq, std::optional<table_id> table, size_t alignment);

private:

    // table is set for segment_kind::full
    void write_header(segment_sequence segment_seq, std::optional<table_id> table);

    template <std::invocable<ostream&> WriteFrame>
    append_result append_record(const record_header& header, size_t header_size, size_t value_size, WriteFrame write_frame);

    void pad_to_alignment(size_t alignment);
    void finalize(size_t alignment);

    friend class write_buffer;
};

// Tracks asynchronous write completion on top of raw_write_buffer.
//
// This is the buffer type used by the segment manager, buffered_writer,
// compaction, and separator flows. write() appends a record to the underlying
// raw buffer and returns a future that resolves to the final record_location once
// the buffer is flushed. The returned gate holder keeps the buffer alive for
// follow-up work such as index updates. For mixed buffers it also keeps copies
// of appended records so separator rewriting can replay them after the flush.
class write_buffer {
public:
    struct record_in_buffer {
        log_record_writer writer;
        // Where the record's frame sits in the buffer, rather than a future of where it ended up:
        // the separator only ever looks at these once the buffer has been written, so a record can
        // be located from the buffer's own position instead of waiting for one of its own.
        size_t frame_offset;
        size_t frame_size;
        write_target target;

        record_location location(segment_position buffer_position) const noexcept {
            return locate_record(buffer_position, frame_offset, frame_size);
        }
    };

private:
    raw_write_buffer _raw;
    shared_promise<segment_position> _written;
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
    bool is_closed() const noexcept;

    const char* data() const noexcept { return _raw.data(); }
    size_t serialized_size() const noexcept { return _raw.serialized_size(); }

    size_t get_buffer_size() const noexcept { return _raw.get_buffer_size(); }

    bool can_fit(size_t record_size) const noexcept { return _raw.can_fit(record_size); }
    template <log_record_writer_concept Writer>
    bool can_fit(const Writer& writer) const noexcept { return _raw.can_fit(writer); }
    bool has_data() const noexcept { return _raw.has_data(); }

    size_t max_record_size() const noexcept { return _raw.max_record_size(); }
    size_t record_bytes() const noexcept { return _raw.record_bytes(); }
    size_t record_count() const noexcept { return _raw.record_count(); }

    size_t sealed_size(size_t alignment) {
        return _raw.sealed_size(alignment);
    }

    void seal(segment_sequence segment_seq, std::optional<table_id> table, size_t alignment) {
        _raw.seal(segment_seq, table, alignment);
    }

    // Write a record to the buffer.
    // Returns a future that will be resolved with the record location once flushed and a gate holder
    // that keeps the write buffer open. The gate should be held for index updates after the write
    // is done.
    template <log_record_writer_concept Writer>
    future<record_location_with_holder> write(Writer writer, write_target target = {});

    // Complete all tracked writes with their locations when the buffer is flushed to buffer_position
    future<> complete_writes(segment_position buffer_position);
    future<> abort_writes(std::exception_ptr);

private:
    bool with_record_copy() const noexcept {
        return _raw.kind() == segment_kind::mixed;
    }

    std::vector<record_in_buffer> take_separator_records();

    friend class buffered_writer;
    friend class segment_manager_impl;
    friend struct separator_buffer;
};

extern template raw_write_buffer::append_result raw_write_buffer::append<log_record_writer>(const log_record_writer&);
extern template raw_write_buffer::append_result raw_write_buffer::append<log_record_bytes_writer>(const log_record_bytes_writer&);

extern template future<record_location_with_holder> write_buffer::write<log_record_writer>(log_record_writer, write_target);
extern template future<record_location_with_holder> write_buffer::write<log_record_bytes_writer>(log_record_bytes_writer, write_target);

class write_buffer_pool;

// Gives a borrowed write_buffer back to its pool, along with the pool unit acquired for it.
class write_buffer_returner {
    write_buffer_pool* _pool = nullptr;
    seastar::semaphore_units<> _units;

public:
    write_buffer_returner() = default;
    write_buffer_returner(write_buffer_pool& pool, seastar::semaphore_units<> units) noexcept
        : _pool(&pool), _units(std::move(units)) {}

    void operator()(write_buffer* wb) noexcept;
};

// A write_buffer borrowed from a write_buffer_pool. reset() gives it back, and so does destruction,
// which is what keeps the pool's accounting right on every path out of a compaction, including the
// ones that throw.
using owned_write_buffer = std::unique_ptr<write_buffer, write_buffer_returner>;

// Pool of write_buffer's for segment building, primarily for compaction output buffers.
// Hands out at most `capacity` buffers concurrently, created on demand and reused up to `max_cached`.
// Buffers must be closed before reuse; owners are responsible for closing before the owned_write_buffer
// goes out of scope. stop() must be called after all users have stopped.
class write_buffer_pool {
public:
    struct config {
        // The most buffers the pool may have out at once.
        size_t capacity{0};
        size_t buffer_size{0};
        segment_kind kind{segment_kind::full};
        // Buffers built at construction rather than at the point of use. Must not exceed max_cached,
        // or they would not all be kept.
        size_t preallocate{0};
        // The most returned buffers kept for reuse. Must not exceed capacity.
        size_t max_cached{0};
    };

    struct stats {
        uint64_t allocation_waits{0};
        uint64_t buffers_dropped{0};
        uint64_t buffers_created{0};
    };

private:
    size_t _buffer_size;
    segment_kind _kind;
    size_t _max_cached;
    size_t _capacity;

    // Buffers ready to be handed out. Never longer than _max_cached, and reserved for it up front,
    // so that returning a buffer to it never allocates - it has to be infallible, being the last
    // step of an owner's destructor.
    std::vector<std::unique_ptr<write_buffer>> _free;
    // Buffers that are currently out with an owner.
    size_t _in_use{0};

    seastar::semaphore _available_sem;
    bool _stopped{false};
    stats _stats;

public:

    explicit write_buffer_pool(config);

    future<> stop();

    future<owned_write_buffer> allocate(abort_source& as);
    future<std::vector<owned_write_buffer>> allocate_many(size_t count, abort_source& as);

    void return_buffer(write_buffer* wb, seastar::semaphore_units<> pool_units) noexcept;

    // Changes the number of buffers the pool may have out at once. Lowering it does not take back
    // any of the buffers that are already out - it only makes the pool hand out that many fewer
    // before the returns catch up - but it does free the ones it no longer needs to keep.
    void set_capacity(size_t capacity);

    size_t capacity() const noexcept { return _capacity; }

    // Buffers that are currently out with an owner.
    size_t used_buffer_count() const noexcept { return _in_use; }

    // Buffers that exist, and therefore the memory the pool holds - including the ones it dropped,
    // whose memory it never gets back, see drop_buffer().
    size_t allocated_buffer_count() const noexcept {
        return _in_use + _free.size() + static_cast<size_t>(_stats.buffers_dropped);
    }

    const stats& get_stats() const noexcept {
        return _stats;
    }

private:
    bool would_wait(size_t count) const noexcept {
        return _available_sem.available_units() < static_cast<ssize_t>(count) || _available_sem.waiters() > 0;
    }

    // Permanently removes a buffer from the pool after it could not be taken back.
    void drop_buffer(std::unique_ptr<write_buffer> wb, seastar::semaphore_units<>& pool_units) noexcept;
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
        seastar::promise<record_location_with_holder> persisted_pr; // written to a segment
        log_record_writer writer;
        write_target target;
        // Keeps the writer from finishing its stop() while the record is on the queue. It is held
        // by the request rather than by whoever waits for it, so that queueing a write needs no
        // coroutine of its own.
        seastar::gate::holder async_gate_holder;
        db::timeout_clock::time_point timeout;
        uint64_t id;
        size_t write_size;

        queued_write(log_record_writer writer, write_target target, seastar::gate::holder async_gate_holder,
                db::timeout_clock::time_point timeout, uint64_t id, size_t write_size)
            : writer(std::move(writer))
            , target(std::move(target))
            , async_gate_holder(std::move(async_gate_holder))
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

    std::optional<future<record_location_with_holder>> append_to_head_buffer(log_record_writer&, write_target&);

    // Puts a record that found no room in the ring on the queue of writes waiting for some, and
    // waits there for it to be taken into a buffer.
    future<buffered_write_result> queue_write(log_record_writer, db::timeout_clock::time_point timeout, write_target,
            seastar::gate::holder);

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

    // Takes a record. The future it returns resolves once the record is in a buffer, and carries a
    // second future that resolves once that buffer is in a segment, which is where the record's
    // location comes from.
    future<buffered_write_result> write_to_buffer(log_record_writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept;

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }

};

}
}
