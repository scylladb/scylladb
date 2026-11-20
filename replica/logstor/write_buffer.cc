/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "replica/logstor/write_buffer.hh"

#include <algorithm>
#include <chrono>

#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>

#include "dht/token.hh"
#include "replica/logstor/logstor.hh"

namespace replica::logstor {

void log_record_writer::write_frame(seastar::simple_memory_output_stream& out) const {
    ondisk::write_record_frame(out, _record.header, _record.value);
}

void log_record_bytes_writer::write_frame(seastar::simple_memory_output_stream& out) const {
    ondisk::write_record_frame(out, log_record_bytes_view{_header_bytes, _value_bytes});
}

// chunk_builder

chunk_builder::chunk_builder(size_t buffer_size, segment_kind kind)
        : _buffer_size(buffer_size)
        , _buffer(seastar::allocate_aligned_buffer<char>(buffer_size, ondisk::block_alignment))
        , _segment_kind(kind)
{
    reset();
}

void chunk_builder::reset() {
    _stream = seastar::simple_memory_output_stream(_buffer.get(), _buffer_size);
    _stream.skip(chunk_header_size());
    _record_bytes = 0;
    _record_count = 0;
    _min_token = std::nullopt;
    _max_token = std::nullopt;
    _finished_body = std::nullopt;
    _sealed = false;
}

template <log_record_writer_concept Writer>
frame_extent chunk_builder::append(const Writer& writer) {
    const auto& header = writer.header();
    const size_t header_size = writer.header_size();
    const size_t value_size = writer.value_size();

    if (header_size < ondisk::record_header_fixed_size) {
        on_internal_error(logstor_logger, fmt::format("Record header size {} is below its fixed size {}", header_size, ondisk::record_header_fixed_size));
    }
    if (const size_t key_size = header_size - ondisk::record_header_fixed_size; key_size > ondisk::max_key_size) {
        on_internal_error(logstor_logger, fmt::format("Partition key size {} exceeds the maximum {}", key_size, ondisk::max_key_size));
    }

    if (_finished_body) {
        on_internal_error(logstor_logger, "Cannot append a record to a finished chunk");
    }

    const size_t frame_size = ondisk::record_frame_size(header_size, value_size);
    if (!fits_frame(frame_size)) {
        throw std::runtime_error(fmt::format("Record frame of {} bytes, {} with its padding, does not fit the {} bytes left in the buffer",
                frame_size, align_up(frame_size, ondisk::record_alignment), _stream.size()));
    }

    const size_t padded_size = align_up(frame_size, ondisk::record_alignment);
    const size_t frame_offset = serialized_size();

    // Write the frame and its padding through a view of the next padded_size bytes, and move the
    // stream past them only when the frame is complete, so that an append that throws leaves the
    // chunk as it was.
    ostream out(_stream.begin(), padded_size);
    auto frame_out = out.write_substream(frame_size);
    writer.write_frame(frame_out);
    if (frame_out.size() != 0) {
        on_internal_error(logstor_logger, fmt::format("Record frame of {} bytes left {} unwritten", frame_size, frame_out.size()));
    }
    out.fill('\0', out.size());
    _stream.skip(padded_size);

    _record_bytes += frame_size;
    _record_count++;
    _min_token = _min_token ? std::min(*_min_token, header.key.token()) : header.key.token();
    _max_token = _max_token ? std::max(*_max_token, header.key.token()) : header.key.token();

    return frame_extent {
        .offset = static_cast<uint32_t>(frame_offset),
        .size = static_cast<uint32_t>(frame_size),
    };
}

size_t chunk_builder::finish() {
    if (_finished_body) {
        return serialized_size();
    }
    const size_t records_size = serialized_size() - chunk_header_size();
    const auto body_crc = ondisk::calculate_body_crc(std::span(_buffer.get() + chunk_header_size(), records_size));

    pad_to_alignment(ondisk::block_alignment);

    _finished_body = finished_body{
        .records_size = static_cast<uint32_t>(records_size),
        .body_crc = body_crc,
    };

    return serialized_size();
}

bytes_view chunk_builder::seal(segment_sequence segment_seq) {
    if (_segment_kind != segment_kind::mixed) {
        on_internal_error(logstor_logger, "Cannot seal a chunk of a full segment without a table");
    }
    return do_seal(segment_seq, std::nullopt);
}

bytes_view chunk_builder::seal(segment_sequence segment_seq, table_id table) {
    if (_segment_kind != segment_kind::full) {
        on_internal_error(logstor_logger, "Cannot seal a chunk of a mixed segment with a table");
    }
    return do_seal(segment_seq, table);
}

bytes_view chunk_builder::do_seal(segment_sequence segment_seq, std::optional<table_id> table) {
    if (_sealed) {
        throw std::runtime_error("Cannot seal a chunk more than once");
    }
    finish();

    ondisk::chunk_header ch {
        .segment = { .segment_seq = segment_seq },
        .flags = 0,
        .records_size = _finished_body->records_size,
        .body_crc = _finished_body->body_crc,
    };

    switch (_segment_kind) {
    case segment_kind::mixed:
        break;
    case segment_kind::full:
        ch.segment.v = segment_info::full {
            .table = table.value(),
            .first_token = _min_token.value_or(dht::minimum_token()),
            .last_token = _max_token.value_or(dht::minimum_token()),
        };
        break;
    }

    // The header goes into the room that reset() left ahead of the records.
    ostream out(_buffer.get(), chunk_header_size());
    ondisk::write_chunk_header(out, ch);
    _sealed = true;
    return bytes_view(reinterpret_cast<const int8_t*>(_buffer.get()), serialized_size());
}

bool chunk_builder::fits_frame(size_t frame_size) const noexcept {
    // The frame is followed by the padding to record_alignment.
    return align_up(frame_size, ondisk::record_alignment) <= _stream.size();
}

void chunk_builder::pad_to_alignment(size_t alignment) {
    auto current_pos = serialized_size();
    auto next_pos = align_up(current_pos, alignment);
    auto padding = next_pos - current_pos;
    if (padding > 0) {
        _stream.fill('\0', padding);
    }
}

template frame_extent chunk_builder::append<log_record_writer>(const log_record_writer&);
template frame_extent chunk_builder::append<log_record_bytes_writer>(const log_record_bytes_writer&);

// write_buffer

write_buffer::write_buffer(size_t buffer_size, segment_kind kind)
        : _builder(buffer_size, kind)
{}

future<> write_buffer::complete_writes(segment_position chunk_position) {
    _written.set_value(chunk_position);
    co_await close();
}

future<> write_buffer::close() {
    if (!_write_gate.is_closed()) {
        co_await _write_gate.close();
    }
}

bool write_buffer::is_closed() const noexcept {
    return _write_gate.is_closed();
}

future<record_location_with_holder> write_buffer::track(frame_extent frame) {
    auto op = _write_gate.hold();

    return _written.get_shared_future().then(
            [frame, op = std::move(op)] (segment_position chunk_position) mutable {
        return std::make_tuple(locate_record(chunk_position, frame), std::move(op));
    });
}

future<> write_buffer::abort_writes(std::exception_ptr ex) {
    if (!_written.available()) {
        _written.set_exception(std::move(ex));
    }
    co_await close();
}

void write_buffer::reset() {
    if (!is_closed()) {
        on_internal_error(logstor_logger, "Cannot reset a write buffer that is not closed");
    }
    _builder.reset();
    _written = {};
    _write_gate = {};
}

mixed_write_buffer::mixed_write_buffer(size_t buffer_size)
        : write_buffer(buffer_size, segment_kind::mixed)
{
    _separator_records.reserve(buffer_size / 100);
}

future<record_location_with_holder> mixed_write_buffer::write(log_record_writer writer, write_target target) {
    const auto frame = builder().append(writer);
    _separator_records.push_back(separator_record {
        .writer = std::move(writer),
        .frame = frame,
        .target = std::move(target)
    });
    return track(frame);
}

std::vector<separator_record> mixed_write_buffer::take_separator_records() {
    return std::move(_separator_records);
}

future<> mixed_write_buffer::abort_writes(std::exception_ptr ex) {
    // The separator never gets the records of a failed flush, and the records are in no segment, so
    // drop them and the holders of their write targets.
    _separator_records.clear();
    return write_buffer::abort_writes(std::move(ex));
}

void mixed_write_buffer::reset() {
    write_buffer::reset();
    _separator_records.clear();
}

// write_buffer_pool

void write_buffer_returner::operator()(full_write_buffer* wb) noexcept {
    _pool->return_buffer(wb, std::move(_units));
}

write_buffer_pool::write_buffer_pool(config cfg)
    : _buffer_size(cfg.buffer_size)
    , _max_cached(cfg.max_cached)
    , _capacity(cfg.capacity)
    , _available_sem(cfg.capacity)
{
    if (cfg.preallocate > cfg.max_cached || cfg.max_cached > cfg.capacity) {
        on_internal_error(logstor_logger, fmt::format(
                "Invalid logstor write buffer pool configuration: capacity {}, preallocate {}, max cached {}",
                cfg.capacity, cfg.preallocate, cfg.max_cached));
    }
    _free.reserve(_max_cached);
    for (size_t i = 0; i < cfg.preallocate; ++i) {
        _free.push_back(std::make_unique<full_write_buffer>(_buffer_size));
        ++_stats.buffers_created;
    }
}

future<owned_write_buffer> write_buffer_pool::allocate(abort_source& as) {
    auto buffers = co_await allocate_many(1, as);
    co_return std::move(buffers[0]);
}

future<std::vector<owned_write_buffer>> write_buffer_pool::allocate_many(size_t count, abort_source& as) {
    if (count > _capacity) {
        on_internal_error(logstor_logger, fmt::format("Cannot allocate {} logstor write buffers from a pool of capacity {}", count, _capacity));
    }

    if (would_wait(count)) {
        ++_stats.allocation_waits;
    }
    auto units = co_await get_units(_available_sem, count, as);

    // Take the cached buffers and build the ones that are missing before handing any of them out,
    // so that failing to build one leaves the pool as it was and signals the units back.
    std::vector<std::unique_ptr<full_write_buffer>> bufs;
    bufs.reserve(count);
    while (bufs.size() < count) {
        if (!_free.empty()) {
            bufs.push_back(std::move(_free.back()));
            _free.pop_back();
        } else {
            bufs.push_back(std::make_unique<full_write_buffer>(_buffer_size));
            ++_stats.buffers_created;
        }
    }

    std::vector<owned_write_buffer> buffers;
    buffers.reserve(count);
    for (auto& buf : bufs) {
        buffers.emplace_back(buf.release(), write_buffer_returner(*this, units.split(1)));
        ++_in_use;
    }
    co_return buffers;
}

void write_buffer_pool::return_buffer(full_write_buffer* wb, seastar::semaphore_units<> pool_units) noexcept {
    // Take the buffer back, so that it is destroyed when this returns unless it is kept for reuse.
    auto buf = std::unique_ptr<full_write_buffer>(wb);
    --_in_use;

    if (!buf->is_closed()) {
        // reset() below reinstalls the buffer's write gate, which is only valid once the gate was
        // closed and all the writes it held completed. Closing is asynchronous and there is nothing
        // left here that can wait for it, so drop the buffer instead.
        on_internal_error_noexcept(logstor_logger, "Returning a logstor write buffer that was not closed");
        drop_buffer(std::move(buf), pool_units);
        return;
    }

    // Hold on to the buffer only as long as the pool is meant to keep one - the units are signalled
    // back either way, so a buffer that is let go here is simply built again when it is needed.
    if (_stopped || _free.size() >= _max_cached || _in_use + _free.size() >= _capacity) {
        return;
    }

    try {
        buf->reset();
        _free.push_back(std::move(buf));
    } catch (...) {
        logstor_logger.error("Failed to return a write buffer, dropping it from the pool: {:t}", std::current_exception());
        drop_buffer(std::move(buf), pool_units);
    }
}

void write_buffer_pool::drop_buffer(std::unique_ptr<full_write_buffer> wb, seastar::semaphore_units<>& pool_units) noexcept {
    ++_stats.buffers_dropped;
    (void)pool_units.release();
    // Leaked deliberately - see drop_buffer()'s declaration for why the buffer must not be destroyed.
    [[maybe_unused]] auto* leaked = wb.release();
}

void write_buffer_pool::set_capacity(size_t capacity) {
    if (capacity == _capacity) {
        return;
    }

    if (capacity > _capacity) {
        _available_sem.signal(capacity - _capacity);
    } else {
        // consume() does not wait: it drives the available units negative when the capacity is
        // lowered while the buffers it stood for are still out, and the returns absorb it.
        _available_sem.consume(_capacity - capacity);
        // Give back the memory the pool no longer needs to hold.
        while (!_free.empty() && _in_use + _free.size() > capacity) {
            _free.pop_back();
        }
    }

    _capacity = capacity;
}

future<> write_buffer_pool::stop() {
    if (_stopped) {
        co_return;
    }
    _stopped = true;
    // Fail pending and future allocations, so that nothing can start using a buffer of a pool that
    // is being stopped.
    _available_sem.broken();
    // Buffers are expected to be back by now - the callers stop everything that allocates from the
    // pool before stopping it, and a compaction closes and returns its buffer before it completes.
    // A buffer that is still out is one an owner never gave back at all; one that came back but
    // could not be taken in was already accounted for and reported by drop_buffer().
    if (used_buffer_count() != 0) {
        on_internal_error_noexcept(logstor_logger, fmt::format(
                "Stopping logstor write buffer pool with {} of {} buffers still in use",
                used_buffer_count(), _capacity));
    }
    _free.clear();
}

// write_buffer_ring

write_buffer_ring::write_buffer_ring(write_buffer_ring_config cfg, seastar::noncopyable_function<future<>(mixed_write_buffer&)> flush_func)
        : _flush_sg(cfg.flush_sg)
        , _buffer_size(cfg.buffer_size)
        , _ring_size(cfg.ring_size)
        , _sync_period(cfg.sync_period)
        , _flush_func(std::move(flush_func))
        , _ring()
        , _in_flight(cfg.ring_size)
        , _queued_writes(on_queued_write_expiry{this})
        , _head_flush_timer([this] { on_head_flush_timer(); }) {

    if (_ring_size < 2) {
        on_internal_error(logstor_logger, fmt::format("Write buffer ring needs at least 2 buffers, got {}", _ring_size));
    }

    if (_buffer_size == 0) {
        on_internal_error(logstor_logger, "Write buffer ring needs a buffer size above 0");
    }

    _ring.reserve(_ring_size);
    for (size_t i = 0; i < _ring_size; ++i) {
        _ring.emplace_back(_buffer_size);
    }
}

bool write_buffer_ring::has_pending_buffers() const noexcept {
    return _tail < _head || head_buf().has_data();
}

bool write_buffer_ring::should_rotate_head_for_flush() const noexcept {
    return _dispatch_tail == _head && head_buf().has_data() && (_async_gate.is_closed() || _head_deadline_expired);
}

void write_buffer_ring::arm_head_flush_timer() {
    if (_head_deadline_expired || _head_flush_timer.armed()) {
        return;
    }
    if (_sync_period <= std::chrono::milliseconds(0)) {
        on_head_flush_timer();
        return;
    }
    _head_flush_timer.arm(db::timeout_clock::now() + _sync_period);
}

void write_buffer_ring::on_head_flush_timer() noexcept {
    _head_deadline_expired = true;
    _consumer_progress_cv.signal();
}

void write_buffer_ring::cancel_head_flush_timer() noexcept {
    _head_flush_timer.cancel();
    _head_deadline_expired = false;
}

void write_buffer_ring::on_queued_writes_changed() noexcept {
    _queued_writes_changed.broadcast();
}

void write_buffer_ring::on_queued_write_removed(const queued_write& w) noexcept {
    _queued_write_bytes -= w.write_size;
}

void write_buffer_ring::fail_queued_write(queued_write& request, std::exception_ptr ep) noexcept {
    request.accepted_pr.set_exception(std::move(ep));
}

bool write_buffer_ring::maybe_advance_head() noexcept {
    if (ring_full() || !head_buf().has_data()) {
        return false;
    }
    cancel_head_flush_timer();
    ++_head;
    _consumer_progress_cv.signal();
    return true;
}

// The target is taken by reference and moved from only once the record is in a buffer: a caller
// whose append does not happen goes on to queue the record, and the write target it queues has to
// be the one it came with, holders and all.
std::optional<future<record_location_with_holder>> write_buffer_ring::append_to_head_buffer(log_record_writer& writer, write_target& target) {
    if (!head_buf().fits(writer) && !maybe_advance_head()) {
        return std::nullopt;
    }

    bool was_empty = !head_buf().has_data();
    auto persisted = head_buf().write(std::move(writer), std::move(target));
    if (was_empty) {
        arm_head_flush_timer();
        _consumer_progress_cv.signal();
    }
    return persisted;
}

bool write_buffer_ring::try_dispatch_next_buffer() {
    auto idx = _dispatch_tail;
    auto& state = dispatch_tail_write();
    auto& buf = _ring[idx % _ring_size];

    if (_dispatch_tail >= _head || !state.idle() || !buf.has_data()) {
        return false;
    }

    try {
        state.start(run_dispatched_write(idx));
    } catch (...) {
        state.start(make_exception_future<>(std::current_exception()));
    }
    ++_dispatch_tail;
    return true;
}

future<> write_buffer_ring::run_dispatched_write(size_t idx) {
    auto& buf = _ring[idx % _ring_size];
    auto f = co_await coroutine::as_future(_flush_func(buf));
    _consumer_progress_cv.signal();
    if (f.failed()) {
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
}

future<bool> write_buffer_ring::reclaim_completed_tails() {
    bool reclaimed = false;
    while (_tail < _dispatch_tail) {
        auto& state = tail_write();
        if (!state.ready()) {
            break;
        }

        auto completion = state.take_completion();
        auto f = co_await coroutine::as_future(std::move(completion));

        if (f.failed()) {
            co_await tail_buf().abort_writes(f.get_exception());
        }

        tail_buf().reset();
        state.reset();
        ++_tail;
        reclaimed = true;
        _tail_advanced.broadcast();

        co_await coroutine::maybe_yield();
    }

    co_return reclaimed;
}

future<bool> write_buffer_ring::drain_queued_writes() {
    bool removed_queued_writes = false;
    while (!_queued_writes.empty()) {
        auto& next = _queued_writes.front();

        if (!head_buf().fits(next.writer) && !maybe_advance_head()) {
            break;
        }

        auto request = std::move(next);
        _queued_writes.pop_front();
        on_queued_write_removed(request);
        removed_queued_writes = true;
        try {
            auto persisted = append_to_head_buffer(request.writer, request.target).value();
            request.accepted_pr.set_value(buffered_write_result{request.persisted_pr.get_future()});
            std::move(persisted).forward_to(std::move(request.persisted_pr));
        } catch (...) {
            fail_queued_write(request, std::current_exception());
        }
        co_await coroutine::maybe_yield();
    }

    if (removed_queued_writes) {
        on_queued_writes_changed();
    }

    co_return removed_queued_writes;
}

future<> write_buffer_ring::start() {
    logstor_logger.info("Starting the write buffer ring");
    _consumer = with_scheduling_group(_flush_sg, [this] {
        return consumer_loop();
    });
    co_return;
}

future<> write_buffer_ring::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping the write buffer ring");

    cancel_head_flush_timer();

    // Wake the consumer so it can observe the closing gate and exit.
    auto close_fut = _async_gate.close();
    _consumer_progress_cv.broadcast();
    co_await std::move(close_fut);
    co_await std::move(_consumer);

    if (!std::ranges::all_of(_in_flight, &in_flight_write::idle)) {
        on_fatal_internal_error(logstor_logger, "Write buffer ring stopped with a flush in flight");
    }

    logstor_logger.info("Write buffer ring stopped");
}

future<> write_buffer_ring::flush() {
    auto holder = _async_gate.hold();

    // Snapshot the queue insertion boundary. New writes pushed after this point
    // get higher ids and are not our responsibility.
    const uint64_t target_next_id = _next_queued_write_id;

    auto preflush_queued_writes_gone = [this, target_next_id] {
        return _queued_writes.empty() || _queued_writes.front().id >= target_next_id;
    };

    // Drain or let expire all writes that were queued when flush() was called.
    while (!preflush_queued_writes_gone()) {
        co_await drain_queued_writes();
        co_await _queued_writes_changed.wait(preflush_queued_writes_gone);
    }

    // If the current head buffer is non-empty, flush() must first move _head
    // past it so the consumer can dispatch that buffer. The head slot itself
    // is always the buffer that writers are still appending to, so as long as
    // _head stays unchanged that buffer is not yet eligible for dispatch.
    //
    // There are two cases:
    // 1. The head buffer is empty. There is nothing new to seal, so flush()
    //    only needs to wait for buffers that were already dispatched or are
    //    already behind _head.
    // 2. The head buffer is non-empty. flush() must rotate _head to the next
    //    ring slot. After that rotation, the old head becomes dispatchable and
    //    the new _head value is the tail boundary that proves the old head was
    //    fully flushed and reclaimed.
    auto head_before_forced_rotation = _head;
    while (_head == head_before_forced_rotation && head_buf().has_data()) {
        if (maybe_advance_head()) {
            break;
        }
        // Ring is full; wait for tail flush to free a slot.
        co_await _tail_advanced.wait();
    }

    // Wait until all buffers that flush() is responsible for are flushed and
    // reclaimed. If the head was empty, this is just the current _head. If the
    // head had data, this is the new post-rotation _head, which means the old
    // head buffer is now behind the tail.
    co_await _tail_advanced.when([this, flush_tail_goal = _head] {
        return _tail >= flush_tail_goal;
    });
}

// A record that goes straight into the head buffer waits for nothing - it is accepted by returning,
// and only its place in a segment is still to come - so this is not a coroutine: one that suspends
// nowhere would allocate a frame per write to do nothing with it. A record that has to be queued
// waits in queue_write(), which is where the frame belongs.
//
// Not being a coroutine, it has to report a failure as a failed future itself, which is what the
// noexcept says: the caller counts a failed write off the future it gets back, and a throw out of
// here would go past that accounting. The try costs nothing on the path that does not throw.
future<buffered_write_result> write_buffer_ring::write_to_buffer(log_record_writer writer, db::timeout_clock::time_point timeout, write_target target) noexcept {
  try {
    auto holder = _async_gate.hold();

    // The record has to fit the mixed buffer it goes into here and the full segment the separator
    // later rewrites it into, so it is bounded by whichever of the two takes less: one that only
    // fits the buffer it is written to first would be accepted here and then never fit anywhere the
    // separator could put it.
    const size_t max_size = chunk_layout::max_record_size_any_kind(head_buf().buffer_size());
    if (writer.record_size() > max_size) [[unlikely]] {
        return make_exception_future<buffered_write_result>(std::runtime_error(
                fmt::format("Write size {} exceeds the maximum record size {}", writer.record_size(), max_size)));
    }
    const size_t key_size = writer.header_size() - ondisk::record_header_fixed_size;
    if (key_size > ondisk::max_key_size) [[unlikely]] {
        return make_exception_future<buffered_write_result>(std::runtime_error(
                fmt::format("Partition key size {} exceeds the maximum {}", key_size, ondisk::max_key_size)));
    }

    // fast path - if there are no queued writes and there is space in the current head buffer or the next, advance the
    // head buffer if needed and write to it.
    if (_queued_writes.empty()) {
        if (auto persisted = append_to_head_buffer(writer, target)) {
            return make_ready_future<buffered_write_result>(buffered_write_result{std::move(*persisted)});
        }
    }

    // either there are queued writes or there is no space in the head buffer and ring is full - queue the write.
    return queue_write(std::move(writer), timeout, std::move(target), std::move(holder));
  } catch (...) {
    return current_exception_as_future<buffered_write_result>();
  }
}

// The future of the request is what this returns, so there is nothing to wait for here either: the
// consumer resolves it when it makes room for the record. The request carries the gate holder of the
// write, which is what has to outlive the wait.
future<buffered_write_result> write_buffer_ring::queue_write(log_record_writer writer, db::timeout_clock::time_point timeout,
        write_target target, seastar::gate::holder holder) {
    const bool queue_was_empty = _queued_writes.empty();
    const auto write_size = writer.record_size();
    queued_write request(std::move(writer), std::move(target), std::move(holder), timeout, _next_queued_write_id++, write_size);
    auto accepted = request.accepted_pr.get_future();
    _queued_writes.push_back(std::move(request), timeout);
    _queued_write_bytes += write_size;
    ++_total_queued_writes;
    if (queue_was_empty) {
        _consumer_progress_cv.signal();
    }
    return accepted;
}

future<> write_buffer_ring::consumer_loop() {
    try {
        while (true) {
            bool progressed = false;

            progressed |= co_await reclaim_completed_tails();

            progressed |= co_await drain_queued_writes();

            if (should_rotate_head_for_flush()) {
                progressed |= maybe_advance_head();
            }

            while (try_dispatch_next_buffer()) {
                progressed = true;
                co_await coroutine::maybe_yield();
            }

            if (_async_gate.is_closed() && _queued_writes.empty() && !has_pending_buffers()) {
                break;
            }

            if (!progressed) {
                co_await _consumer_progress_cv.wait();
            }

            co_await coroutine::maybe_yield();
        }
    } catch (...) {
        on_internal_error(logstor_logger, fmt::format("The consumer of the write buffer ring failed: {:t}", std::current_exception()));
    }
}

}
