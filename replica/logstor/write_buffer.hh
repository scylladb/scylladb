/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/expiring_fifo.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/timer.hh>

#include "replica/logstor/ondisk.hh"
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
    log_record _record;

public:
    explicit log_record_writer(log_record record)
        : _record(std::move(record))
    {}

    const log_record& record() const { return _record; }
    const record_header& header() const { return _record.header; }

    size_t header_size() const { return ondisk::record_header_size(_record.header); }
    size_t value_size() const { return _record.value.size(); }
    // The record without its frame header: the record header and the value.
    size_t record_size() const { return header_size() + value_size(); }

    // Write the record's frame - frame header, record header, value - to an output stream.
    void write_frame(seastar::simple_memory_output_stream& out) const;
};

// A record writer for a record whose header and value are already serialized. The writer copies the
// bytes as they are, so that a rewrite of a record does not decode the record.
class log_record_bytes_writer {
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

    void write_frame(seastar::simple_memory_output_stream& out) const;
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

// Holds the offset of a record frame in a chunk and the frame_size of the frame, as record_location
// does.
struct frame_extent {
    uint32_t offset;
    uint32_t size;
};

// Returns the location of the record whose frame is at `frame` in a chunk that is written at
// `chunk_position`.
inline record_location locate_record(segment_position chunk_position, frame_extent frame) noexcept {
    return record_location {
        .segment = chunk_position.segment,
        .offset = chunk_position.offset + frame.offset,
        .size = frame.size,
    };
}

namespace chunk_layout {

// Returns the largest record_size that fits an empty chunk of `kind` in a buffer of `buffer_size`
// bytes. The chunk header of a full segment is larger than the chunk header of a mixed segment, so
// a record can fit a chunk of one kind and not a chunk of the other kind.
constexpr size_t max_record_size(size_t buffer_size, segment_kind kind) noexcept {
    const size_t overhead = ondisk::chunk_header_size(kind) + ondisk::record_frame_header_size;
    return buffer_size > overhead ? buffer_size - overhead : 0;
}

// Returns the largest record_size that fits an empty chunk of each kind in a buffer of
// `buffer_size` bytes. The separator rewrites the records of a mixed segment into full segments, so
// a record that logstor accepts must fit both kinds.
constexpr size_t max_record_size_any_kind(size_t buffer_size) noexcept {
    return std::min(max_record_size(buffer_size, segment_kind::mixed),
                    max_record_size(buffer_size, segment_kind::full));
}

}

struct buffered_write_result {
    future<record_location_with_holder> persisted;
};

// Builds one chunk in memory, in a buffer of buffer_size() bytes:
//
//   0                     +------------------+
//                         |   chunk header   |  seal()
//   chunk_header_size()   +------------------+
//                         |   record frame   |  append(), one frame per record,
//                         |   record frame   |  each padded to record_alignment
//                         |       ...        |
//   + records_size        +------------------+
//                         |   zero padding   |  finish(), to block_alignment
//   serialized_size()     +------------------+
//                         |      unused      |
//   buffer_size()         +------------------+
//
// reset() reserves the space for the header, so the records start at chunk_header_size().
// The header of a full segment also holds the table and the token range of the records. The
// record frames are the body of the chunk, which the body CRC covers.
//
// finish() computes the body CRC and pads the chunk, and the size that finish() returns does not
// change after this. seal() then writes the chunk header, which holds the segment_seq of the
// segment that receives the chunk. So a caller can reserve room for the chunk in a segment before
// it knows the segment_seq.
class chunk_builder {
public:
    chunk_builder(size_t buffer_size, segment_kind kind);

    chunk_builder(const chunk_builder&) = delete;
    chunk_builder& operator=(const chunk_builder&) = delete;

    chunk_builder(chunk_builder&&) noexcept = default;
    chunk_builder& operator=(chunk_builder&&) noexcept = default;

    // Empties the buffer, to build a new chunk in it.
    void reset();

    // Appends the frame of the record of `writer`.
    // Throws std::runtime_error when the record does not fit, see fits(). An append that throws
    // leaves the builder unchanged.
    template <log_record_writer_concept Writer>
    frame_extent append(const Writer& writer);

    // Ends the appends, pads the chunk to block_alignment and computes the body CRC. Returns the
    // size of the chunk, which does not change after this.
    size_t finish();

    // Finishes the chunk if finish() was not called, writes the chunk header and returns the chunk.
    // The first form seals a chunk of a mixed segment, and the second form seals a chunk of a full
    // segment, whose chunk header holds `table`.
    bytes_view seal(segment_sequence segment_seq);
    bytes_view seal(segment_sequence segment_seq, table_id table);

    template <log_record_writer_concept Writer>
    bool fits(const Writer& writer) const noexcept {
        return fits_frame(ondisk::record_frame_size(writer.header_size(), writer.value_size()));
    }

    bool has_data() const noexcept { return _record_count > 0; }

    // Returns the number of bytes written to the buffer so far, which is the size of the whole
    // chunk after finish().
    size_t serialized_size() const noexcept { return _buffer_size - _stream.size(); }

    size_t buffer_size() const noexcept { return _buffer_size; }

    size_t chunk_header_size() const noexcept {
        return ondisk::chunk_header_size(_segment_kind);
    }

    size_t record_bytes() const noexcept { return _record_bytes; }
    size_t record_count() const noexcept { return _record_count; }

    // Returns the bytes of an appended record frame, without the padding after the frame.
    bytes_view frame_bytes(frame_extent frame) const noexcept {
        return bytes_view(reinterpret_cast<const int8_t*>(_buffer.get() + frame.offset), frame.size);
    }

private:
    using ostream = seastar::simple_memory_output_stream;

    size_t _buffer_size;
    std::unique_ptr<char[], free_deleter> _buffer;
    segment_kind _segment_kind;
    ostream _stream;

    // The frame sizes of the appended records summed, their padding excluded.
    size_t _record_bytes{0};
    size_t _record_count{0};
    std::optional<dht::token> _min_token;
    std::optional<dht::token> _max_token;

    // Holds the values that finish() computes from the chunk body, for the chunk header.
    struct finished_body {
        uint32_t records_size;
        uint32_t body_crc;
    };
    std::optional<finished_body> _finished_body;
    bool _sealed{false};

    bool fits_frame(size_t frame_size) const noexcept;

    void pad_to_alignment(size_t alignment);

    bytes_view do_seal(segment_sequence segment_seq, std::optional<table_id> table);
};

// A record of a mixed buffer, which the separator rewrites after the buffer is flushed.
struct separator_record {
    log_record_writer writer;
    frame_extent frame;
    write_target target;

    record_location location(segment_position chunk_position) const noexcept {
        return locate_record(chunk_position, frame);
    }
};

// A chunk builder that tracks the writes of its records. The write() of each subclass appends a
// record and returns, through track(), a future of the location of the record, which resolves after
// the chunk is written to a segment and complete_writes() is called. The gate holder that comes
// with the location keeps the buffer open for the work that follows the write, such as an index
// update.
//
// mixed_write_buffer and full_write_buffer take different records and seal different headers, and
// this class holds what they share.
class write_buffer {
    chunk_builder _builder;
    shared_promise<segment_position> _written;
    seastar::gate _write_gate;

public:
    write_buffer(const write_buffer&) = delete;
    write_buffer& operator=(const write_buffer&) = delete;

    // Ends the appends, see chunk_builder::finish().
    size_t finish() { return _builder.finish(); }

    // Completes the tracked writes once the chunk of the buffer is written at `chunk_position`, and
    // waits for the holders of the written records.
    future<> complete_writes(segment_position chunk_position);

    // Waits for the holders of the written records.
    // The caller must close the buffer before it resets the buffer.
    future<> close();
    bool is_closed() const noexcept;

    template <log_record_writer_concept Writer>
    bool fits(const Writer& writer) const noexcept { return _builder.fits(writer); }
    bool has_data() const noexcept { return _builder.has_data(); }

    size_t serialized_size() const noexcept { return _builder.serialized_size(); }
    size_t buffer_size() const noexcept { return _builder.buffer_size(); }
    size_t record_bytes() const noexcept { return _builder.record_bytes(); }
    size_t record_count() const noexcept { return _builder.record_count(); }

protected:
    write_buffer(size_t buffer_size, segment_kind kind);
    ~write_buffer() = default;

    write_buffer(write_buffer&&) noexcept = default;
    write_buffer& operator=(write_buffer&&) noexcept = default;

    // Returns the future of the location of the record whose frame is at `frame`. The future holds
    // the write gate until the write of the record completes, and passes the holder to the caller.
    future<record_location_with_holder> track(frame_extent frame);

    // Fails the tracked writes with `ex`, for a chunk that is not written, and closes the buffer.
    future<> abort_writes(std::exception_ptr ex);

    void reset();

    chunk_builder& builder() noexcept { return _builder; }
    const chunk_builder& builder() const noexcept { return _builder; }
};

// The write buffer of a mixed segment, which takes the user writes. The buffer keeps the records
// that it took until its chunk is written, so that the separator can rewrite them into full
// segments.
class mixed_write_buffer final : public write_buffer {
    std::vector<separator_record> _separator_records;

public:
    explicit mixed_write_buffer(size_t buffer_size);

    future<record_location_with_holder> write(log_record_writer writer, write_target target = {});

    bytes_view seal(segment_sequence segment_seq) {
        return builder().seal(segment_seq);
    }

    // Returns the records of the buffer for the separator, after the chunk of the buffer is
    // written.
    std::vector<separator_record> take_separator_records();

    future<> abort_writes(std::exception_ptr ex);

    void reset();
};

// The write buffer of a full segment, which takes the records of compaction and the separator.
class full_write_buffer final : public write_buffer {
public:
    explicit full_write_buffer(size_t buffer_size)
        : write_buffer(buffer_size, segment_kind::full)
    {}

    template <log_record_writer_concept Writer>
    future<record_location_with_holder> write(const Writer& writer) {
        return track(builder().append(writer));
    }

    bytes_view seal(segment_sequence segment_seq, table_id table) {
        return builder().seal(segment_seq, table);
    }

    using write_buffer::abort_writes;
    using write_buffer::reset;
};

extern template frame_extent chunk_builder::append<log_record_writer>(const log_record_writer&);
extern template frame_extent chunk_builder::append<log_record_bytes_writer>(const log_record_bytes_writer&);

class write_buffer_pool;

// Gives a borrowed full_write_buffer back to its pool, along with the pool unit acquired for it.
class write_buffer_returner {
    write_buffer_pool* _pool = nullptr;
    seastar::semaphore_units<> _units;

public:
    write_buffer_returner() = default;
    write_buffer_returner(write_buffer_pool& pool, seastar::semaphore_units<> units) noexcept
        : _pool(&pool), _units(std::move(units)) {}

    void operator()(full_write_buffer* wb) noexcept;
};

// A full_write_buffer borrowed from a write_buffer_pool. reset() gives it back, and so does destruction,
// which is what keeps the pool's accounting right on every path out of a compaction, including the
// ones that throw.
using owned_write_buffer = std::unique_ptr<full_write_buffer, write_buffer_returner>;

// Pool of full_write_buffer's for segment building, primarily for compaction output buffers.
// Hands out at most `capacity` buffers concurrently, created on demand and reused up to `max_cached`.
// Buffers must be closed before reuse; owners are responsible for closing before the owned_write_buffer
// goes out of scope. stop() must be called after all users have stopped.
class write_buffer_pool {
public:
    struct config {
        // The most buffers the pool may have out at once.
        size_t capacity{0};
        size_t buffer_size{0};
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
    size_t _max_cached;
    size_t _capacity;

    // Buffers ready to be handed out. Never longer than _max_cached, and reserved for it up front,
    // so that returning a buffer to it never allocates - it has to be infallible, being the last
    // step of an owner's destructor.
    std::vector<std::unique_ptr<full_write_buffer>> _free;
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

    void return_buffer(full_write_buffer* wb, seastar::semaphore_units<> pool_units) noexcept;

    // Changes the number of buffers the pool may have out at once. Lowering it does not take back
    // any of the buffers that are already out - it only makes the pool hand out that many fewer
    // before the returns catch up - but it does free the ones it no longer needs to keep.
    void set_capacity(size_t capacity);

    size_t capacity() const noexcept { return _capacity; }

    size_t memory_usage() const noexcept { return allocated_buffer_count() * _buffer_size; }

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
    void drop_buffer(std::unique_ptr<full_write_buffer> wb, seastar::semaphore_units<>& pool_units) noexcept;
};

// Configuration passed to write_buffer_ring constructor (excluding the flush function).
struct write_buffer_ring_config {
    size_t buffer_size;
    // The scheduling group that the ring runs its flushes in.
    seastar::scheduling_group flush_sg;
    // The number of buffers in the ring, which allows at most `ring_size - 1` flushes in flight.
    // With a sync period of 0, records collect in a buffer only while all flushes are in flight, so
    // a larger ring writes more and smaller chunks, each padded to a block, and adds no throughput.
    // The default allows four flushes in flight, which are sufficient to overlap the disk write with
    // the index and separator work.
    size_t ring_size{5};
    // The time that the ring waits after the first record of a head buffer that is not full, before
    // the head advances past the buffer. A longer period writes fewer and larger chunks at low load,
    // but each write then waits up to the period. With the default of 0, records still collect in a
    // buffer while all flushes are in flight, so the chunks are not small under load.
    std::chrono::milliseconds sync_period{0};
};

// Manages a circular ring of mixed write buffers.
//
// Writers append to the head buffer.  A single consumer coroutine drains the
// tail.  The head advances when the current head buffer is full (can't fit the
// next write) or when the consumer advances it to flush the head buffer.
// Writers wait if the ring is full (all buffers are pending flush).
class write_buffer_ring {
    seastar::scheduling_group _flush_sg;
    size_t _buffer_size;
    size_t _ring_size;
    std::chrono::milliseconds _sync_period;
    seastar::noncopyable_function<future<>(mixed_write_buffer&)> _flush_func;

    // The ring of buffers, indexed modulo _ring_size.
    std::vector<mixed_write_buffer> _ring;

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
        // Keeps the ring from finishing its stop() while the record is on the queue. It is held
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
    };

    struct on_queued_write_expiry {
        write_buffer_ring* owner{};

        void operator()(queued_write& w) noexcept {
            owner->on_queued_write_removed(w);
            owner->fail_queued_write(w, std::make_exception_ptr(seastar::timed_out_error{}));
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
    uint64_t _total_queued_writes{0};

    seastar::timer<db::timeout_clock> _head_flush_timer;
    bool _head_deadline_expired = false;

    // Monotonically increasing id assigned to queued writes. flush() snapshots
    // this counter and waits until all queued writes with lower ids are gone.
    uint64_t _next_queued_write_id = 0;

    seastar::gate _async_gate;

    // The single flush-consumer fiber, running for the lifetime of the ring.
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
    // waits there for it to be taken into a buffer. The queue has no size limit, so a write waits
    // until its timeout, as a memtable write waits for dirty memory.
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
    explicit write_buffer_ring(write_buffer_ring_config cfg,
            seastar::noncopyable_function<future<>(mixed_write_buffer&)> flush_func);

    write_buffer_ring(const write_buffer_ring&) = delete;
    write_buffer_ring& operator=(const write_buffer_ring&) = delete;

    future<> start();
    future<> stop();
    future<> flush();

    // Takes a record. The future it returns resolves once the record is in a buffer, and carries a
    // second future that resolves once that buffer is in a segment, which is where the record's
    // location comes from.
    future<buffered_write_result> write_to_buffer(log_record_writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept;

    // Returns a future that resolves with the location of the record once the record is persisted.
    future<record_location_with_holder> write(log_record_writer writer, db::timeout_clock::time_point timeout, write_target target = {}) noexcept {
        return write_to_buffer(std::move(writer), timeout, std::move(target)).then([] (buffered_write_result r) {
            return std::move(r.persisted);
        });
    }

    size_t queued_write_count() const noexcept { return _queued_writes.size(); }
    size_t queued_write_bytes() const noexcept { return _queued_write_bytes; }
    uint64_t total_queued_writes() const noexcept { return _total_queued_writes; }

    // Returns the memory of the ring buffers. The queued writes are not counted, because the limits
    // on open requests already hold their memory, and their memory is freed when the flushes continue.
    size_t memory_usage() const noexcept { return _ring_size * _buffer_size; }

};

}
}
