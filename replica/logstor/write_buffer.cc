/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "write_buffer.hh"
#include "dht/token.hh"
#include "bytes_fwd.hh"
#include "logstor.hh"
#include "replica/logstor/types.hh"
#include <seastar/core/simple-stream.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/as_future.hh>
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include "utils/crc.hh"

namespace replica::logstor {

void log_record_writer::compute_sizes() const {
    seastar::measuring_output_stream ms_header;
    ser::serialize(ms_header, _record.header);
    _header_size = ms_header.size();

    seastar::measuring_output_stream ms_data;
    ser::serialize(ms_data, _record.mut);
    _data_size = ms_data.size();
}

void log_record_writer::write(ostream& out) const {
    ser::serialize(out, _record.header);
    ser::serialize(out, _record.mut);
}

// raw_write_buffer

raw_write_buffer::raw_write_buffer(size_t buffer_size, segment_kind kind)
        : _buffer_size(buffer_size)
        , _buffer(seastar::allocate_aligned_buffer<char>(buffer_size, 4096))
        , _segment_kind(kind)
{
    reset();
}

void raw_write_buffer::reset() {
    _stream = seastar::simple_memory_output_stream(_buffer.get(), _buffer_size);
    _header_stream = _stream.write_substream(ondisk::buffer_header_size);
    if (with_segment_header()) {
        _segment_header_stream = _stream.write_substream(ondisk::segment_header_size);
    }
    _buffer_header = {};
    _net_data_size = 0;
    _record_count = 0;
    _min_token = std::nullopt;
    _max_token = std::nullopt;
    _sealed = false;
}

size_t raw_write_buffer::max_record_size() const noexcept {
    return _buffer_size - (header_size() + ondisk::record_header_size);
}

bool raw_write_buffer::can_fit(size_t data_size) const noexcept {
    // Calculate total space needed including header, data, and alignment padding
    auto total_size = ondisk::record_header_size + data_size;
    auto aligned_size = align_up(total_size, ondisk::record_alignment);
    return aligned_size <= _stream.size();
}

bool raw_write_buffer::has_data() const noexcept {
    return offset_in_buffer() > header_size();
}

raw_write_buffer::append_result raw_write_buffer::append(const log_record_writer& writer) {
    const auto content_size = writer.size();

    if (!can_fit(content_size)) {
        throw std::runtime_error(fmt::format("Write size {} exceeds buffer size {}", content_size, _stream.size()));
    }
    if (content_size == 0) {
        throw std::runtime_error("Cannot write empty record");
    }

    size_t record_header_offset = offset_in_buffer();
    auto rh = ondisk::record_header {
        .header_size = static_cast<uint32_t>(writer.header_size()),
        .data_size = static_cast<uint32_t>(writer.data_size())
    };
    ser::serialize(_stream, rh);

    // Write actual data
    auto data_out = _stream.write_substream(content_size);
    writer.write(data_out);

    const size_t total_size = ondisk::record_header_size + content_size;

    _net_data_size += total_size;
    _record_count++;
    if (!_min_token || writer.record().header.key.dk.token() < *_min_token) {
        _min_token = writer.record().header.key.dk.token();
    }
    if (!_max_token || writer.record().header.key.dk.token() > *_max_token) {
        _max_token = writer.record().header.key.dk.token();
    }

    // Add padding to align record
    pad_to_alignment(ondisk::record_alignment);

    return append_result {
        .record_header_offset = record_header_offset,
        .total_size = total_size,
    };
}

size_t raw_write_buffer::sealed_size(size_t alignment) const noexcept {
    auto size = offset_in_buffer();
    return align_up(size, alignment);
}

void raw_write_buffer::pad_to_alignment(size_t alignment) {
    auto current_pos = offset_in_buffer();
    auto next_pos = align_up(current_pos, alignment);
    auto padding = next_pos - current_pos;
    if (padding > 0) {
        _stream.fill('\0', padding);
    }
}

void raw_write_buffer::finalize(size_t alignment) {
    _buffer_header.data_size = static_cast<uint32_t>(offset_in_buffer() - header_size());
    pad_to_alignment(alignment);
}

void raw_write_buffer::seal(segment_sequence segment_seq, std::optional<table_id> table, size_t alignment) {
    if (_sealed) {
        throw std::runtime_error("Cannot seal write buffer more than once");
    }
    finalize(alignment);
    write_header(segment_seq, table);
    _sealed = true;
}

void raw_write_buffer::write_header(segment_sequence segment_seq, std::optional<table_id> table) {
    _buffer_header.magic = ondisk::buffer_header_magic;
    _buffer_header.kind = _segment_kind;
    _buffer_header.version = ondisk::current_version;
    _buffer_header.reserved = 0;
    _buffer_header.segment_seq = segment_seq;

    _buffer_header.crc = _buffer_header.calculate_crc();

    ser::serialize<ondisk::buffer_header>(_header_stream, _buffer_header);

    if (_segment_kind == segment_kind::full) {
        ondisk::segment_header seg_hdr {
            .table = table.value(),
            .first_token = _min_token.value_or(dht::minimum_token()),
            .last_token = _max_token.value_or(dht::minimum_token()),
        };

        ser::serialize<ondisk::segment_header>(_segment_header_stream, seg_hdr);
    }
}

future<> write_buffer::complete_writes(log_location base_location) {
    _written.set_value(base_location);
    co_await close();
}

future<> write_buffer::abort_writes(std::exception_ptr ex) {
    if (!_written.available()) {
        _written.set_exception(std::move(ex));
    }
    co_await close();
}

// write_buffer

write_buffer::write_buffer(size_t buffer_size, segment_kind kind)
        : _raw(buffer_size, kind)
{
    if (with_record_copy()) {
        _records_copy.reserve(_raw.get_buffer_size() / 100);
    }
}

void write_buffer::reset() {
    _raw.reset();
    _written = {};
    _records_copy.clear();
    _write_gate = {};
}

future<> write_buffer::close() {
    if (!_write_gate.is_closed()) {
        co_await _write_gate.close();
    }
}

future<log_location_with_holder> write_buffer::write(log_record_writer writer, compaction_group* cg, seastar::gate::holder cg_holder) {
    auto append_result = _raw.append(writer);

    auto record_location = [record_header_offset = append_result.record_header_offset, total_size = append_result.total_size] (log_location base_location) {
        return log_location {
            .segment = base_location.segment,
            .offset = static_cast<uint32_t>(base_location.offset + record_header_offset),
            .size = static_cast<uint32_t>(total_size)
        };
    };

    if (with_record_copy()) {
        _records_copy.push_back(record_in_buffer {
            .writer = std::move(writer),
            .loc = _written.get_shared_future().then(record_location),
            .cg = cg,
            .cg_holder = std::move(cg_holder)
        });
    }

    // hold the write buffer until the write is complete, and pass the holder to the
    // caller for follow-up operations that should continue holding the buffer, such
    // as index updates.
    auto op = _write_gate.hold();

    return _written.get_shared_future().then([record_location, op = std::move(op)] (log_location base_location) mutable {
        return std::make_tuple(record_location(base_location), std::move(op));
    });
}

std::vector<write_buffer::record_in_buffer>& write_buffer::records_for_separator() {
    if (!with_record_copy()) {
        on_internal_error(logstor_logger, "requesting records but the write buffer has no record copy enabled");
    }
    return _records_copy;
}

size_t raw_write_buffer::estimate_required_segments(size_t net_data_size, size_t record_count, size_t segment_size) {
    // Calculate total size needed including headers and alignment padding.
    // net_data_size includes record headers.
    size_t total_size = net_data_size;

    // not perfect so let's multiply by some overhead constant
    total_size = static_cast<size_t>(total_size * 1.1);

    return align_up(total_size, segment_size) / segment_size;

}

uint32_t ondisk::buffer_header::calculate_crc() const {
    utils::crc32 c;
    c.process_le(magic);
    c.process_le(static_cast<uint8_t>(kind));
    c.process_le(version);
    c.process_le(reserved);
    c.process_le(segment_seq.value);
    c.process_le(data_size);
    return c.get();
}

bool ondisk::validate_header(const ondisk::buffer_header& bh) {
    if (bh.magic != ondisk::buffer_header_magic) {
        return false;
    }

    switch (bh.kind) {
    case segment_kind::mixed:
    case segment_kind::full:
        break;
    default:
        return false;
    }

    if (bh.version != ondisk::current_version) {
        return false;
    }

    return bh.calculate_crc() == bh.crc;
}

bool ondisk::validate_record_header(const ondisk::record_header& rh) {
    return rh.header_size != 0;
}

// buffered_writer

buffered_writer::buffered_writer(buffered_writer_config cfg, seastar::noncopyable_function<future<>(write_buffer&)> flush_func)
        : _flush_sg(cfg.flush_sg)
        , _buffer_size(cfg.buffer_size)
        , _ring_size(cfg.ring_size)
        , _flush_func(std::move(flush_func))
        , _ring()
        , _in_flight(cfg.ring_size)
        , _queued_writes(on_queued_write_expiry{this})
        , _max_queued_write_bytes(cfg.max_queued_write_bytes) {

    if (_ring_size < 2) {
        on_internal_error(logstor_logger, fmt::format("buffered_writer ring_size must be >= 2 (got {})", _ring_size));
    }

    if (_buffer_size == 0) {
        on_internal_error(logstor_logger, "buffered_writer buffer_size must be > 0");
    }

    _ring.reserve(_ring_size);
    for (size_t i = 0; i < _ring_size; ++i) {
        _ring.emplace_back(_buffer_size, segment_kind::mixed);
    }
}

bool buffered_writer::has_pending_buffers() const noexcept {
    return _tail < _head || head_buf().has_data();
}

bool buffered_writer::should_rotate_head_for_flush() const noexcept {
    return _dispatch_tail == _head && head_buf().has_data();
}

void buffered_writer::on_queued_write_removed(const queued_write& w) noexcept {
    _queued_write_bytes -= w.write_size;
}

void buffered_writer::fail_queued_write(queued_write& request, std::exception_ptr ep) noexcept {
    request.accepted_pr.set_exception(ep);
    request.persisted_pr.set_exception(std::move(ep));
}

bool buffered_writer::maybe_advance_head() noexcept {
    if (ring_full() || !head_buf().has_data()) {
        return false;
    }
    ++_head;
    _consumer_progress_cv.signal();
    return true;
}

std::optional<future<log_location_with_holder>> buffered_writer::append_to_head_buffer(log_record_writer& writer, compaction_group* cg, seastar::gate::holder cg_holder) {
    if (!head_buf().can_fit(writer) && !maybe_advance_head()) {
        return std::nullopt;
    }

    bool was_empty = !head_buf().has_data();
    auto persisted = head_buf().write(std::move(writer), cg, std::move(cg_holder));
    if (was_empty) {
        _consumer_progress_cv.signal();
    }
    return persisted;
}

bool buffered_writer::try_dispatch_next_buffer() {
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

future<> buffered_writer::run_dispatched_write(size_t idx) {
    auto& buf = _ring[idx % _ring_size];
    auto f = co_await coroutine::as_future(_flush_func(buf));
    _consumer_progress_cv.signal();
    if (f.failed()) {
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
}

future<bool> buffered_writer::reclaim_completed_tails() {
    bool reclaimed = false;
    while (_tail < _dispatch_tail) {
        auto& state = tail_write();
        if (!state.ready()) {
            break;
        }

        auto completion = state.take_completion();
        auto f = co_await coroutine::as_future(std::move(completion));

        if (f.failed()) {
            auto ex = f.get_exception();
            auto abort_f = co_await coroutine::as_future(tail_buf().abort_writes(std::move(ex)));
            if (abort_f.failed()) {
                logstor_logger.warn("Failed to abort buffered writes: {}. Ignoring.", abort_f.get_exception());
            }
        }

        tail_buf().reset();
        state.reset();
        ++_tail;
        reclaimed = true;

        co_await coroutine::maybe_yield();
    }

    co_return reclaimed;
}

future<bool> buffered_writer::drain_queued_writes() {
    bool removed_queued_writes = false;
    while (!_queued_writes.empty()) {
        auto& next = _queued_writes.front();

        if (!head_buf().can_fit(next.writer) && !maybe_advance_head()) {
            break;
        }

        auto request = std::move(next);
        _queued_writes.pop_front();
        on_queued_write_removed(request);
        removed_queued_writes = true;
        try {
            auto persisted = append_to_head_buffer(request.writer, request.cg, std::move(request.cg_holder)).value();
            request.accepted_pr.set_value(buffered_write_result{request.persisted_pr.get_future()});
            std::move(persisted).forward_to(std::move(request.persisted_pr));
        } catch (...) {
            fail_queued_write(request, std::current_exception());
        }
        co_await coroutine::maybe_yield();
    }

    co_return removed_queued_writes;
}

future<> buffered_writer::start() {
    logstor_logger.info("Starting write buffer");
    _consumer = with_scheduling_group(_flush_sg, [this] {
        return consumer_loop();
    });
    co_return;
}

future<> buffered_writer::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping write buffer");

    // Wake the consumer so it can observe the closing gate and exit.
    auto close_fut = _async_gate.close();
    _consumer_progress_cv.broadcast();
    co_await std::move(close_fut);
    co_await std::move(_consumer);

    for (auto& state : _in_flight) {
        if (state.completion) {
            auto f = co_await coroutine::as_future(std::move(*state.completion));
            state.completion.reset();
            if (f.failed()) {
                logstor_logger.warn("Buffered write completion failed during stop: {}. Ignoring.", f.get_exception());
            }
        }
    }

    logstor_logger.info("Write buffer stopped");
}

future<buffered_write_result> buffered_writer::write_to_buffer(log_record_writer writer, db::timeout_clock::time_point timeout, compaction_group* cg, seastar::gate::holder cg_holder) {
    auto holder = _async_gate.hold();

    if (writer.size() > head_buf().max_record_size()) {
        co_await coroutine::return_exception(std::runtime_error(fmt::format("Write size {} exceeds buffer size {}", writer.size(), head_buf().max_record_size())));
    }

    // fast path - if there are no queued writes and there is space in the current head buffer or the next, advance the
    // head buffer if needed and write to it.
    if (_queued_writes.empty()) {
        if (auto persisted = append_to_head_buffer(writer, cg, std::move(cg_holder))) {
            co_return buffered_write_result{std::move(*persisted)};
        }
    }

    // either there are queued writes or there is no space in the head buffer and ring is full - queue the write.

    if (_max_queued_write_bytes != 0 && _queued_write_bytes + writer.size() > _max_queued_write_bytes) {
        co_await coroutine::return_exception(replica::rate_limit_exception());
    }

    const bool queue_was_empty = _queued_writes.empty();
    const auto write_size = writer.size();
    queued_write request(std::move(writer), cg, std::move(cg_holder), timeout, write_size);
    auto accepted = request.accepted_pr.get_future();
    _queued_write_bytes += write_size;
    _queued_writes.push_back(std::move(request), timeout);
    if (queue_was_empty) {
        _consumer_progress_cv.signal();
    }
    co_return co_await std::move(accepted);
}

future<log_location_with_holder> buffered_writer::write(log_record_writer writer, db::timeout_clock::time_point timeout, compaction_group* cg, seastar::gate::holder cg_holder) {
    auto result = co_await write_to_buffer(std::move(writer), timeout, cg, std::move(cg_holder));
    co_return co_await std::move(result.persisted);
}

future<> buffered_writer::consumer_loop() {
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

        while (!_queued_writes.empty()) {
            auto request = std::move(_queued_writes.front());
            _queued_writes.pop_front();
            on_queued_write_removed(request);
            fail_queued_write(request, std::make_exception_ptr(seastar::gate_closed_exception()));
            co_await coroutine::maybe_yield();
        }
    } catch (...) {
        on_internal_error(logstor_logger, format("buffered_writer consumer loop aborted unexpectedly: {}", std::current_exception()));
    }
}

}
