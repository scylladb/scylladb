/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include "write_buffer.hh"
#include "dht/token.hh"
#include "segment_manager.hh"
#include "bytes_fwd.hh"
#include "logstor.hh"
#include "replica/logstor/types.hh"
#include <seastar/core/simple-stream.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/core/on_internal_error.hh>
#include "serializer_impl.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include "utils/crc.hh"

namespace replica::logstor {

void log_record_writer::compute_sizes() const {
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
        .data_size = static_cast<uint32_t>(writer.data_size())
    };
    ser::serialize(_stream, rh);

    // Write actual data
    auto data_out = _stream.write_substream(content_size);
    writer.write(data_out);

    const size_t total_size = ondisk::record_header_size + content_size;

    _net_data_size += total_size;
    _record_count++;
    if (!_min_token || writer.record().header.key.token() < *_min_token) {
        _min_token = writer.record().header.key.token();
    }
    if (!_max_token || writer.record().header.key.token() > *_max_token) {
        _max_token = writer.record().header.key.token();
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

future<log_location_with_holder> write_buffer::write(log_record_writer writer, write_target target) {
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
            .target = std::move(target)
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
    return true;
}

// buffered_writer

buffered_writer::buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg)
        : _sm(sm)
        , _flush_sg(flush_sg) {
    _ring.reserve(ring_size);
    for (size_t i = 0; i < ring_size; ++i) {
        _ring.emplace_back(_sm.get_segment_size(), segment_kind::mixed);
    }
}

future<> buffered_writer::start() {
    logstor_logger.info("Starting write buffer");
    _consumer = with_gate(_async_gate, [this] {
        return with_scheduling_group(_flush_sg, [this] {
            return consumer_loop();
        });
    });
    co_return;
}

future<> buffered_writer::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping write buffer");

    // Wake the consumer so it can observe the closing gate and exit.
    _tail_can_advance.broadcast();
    // Wake any writer blocked waiting for a free ring slot.
    _head_can_advance.broadcast();

    co_await _async_gate.close();
    co_await std::move(_consumer);

    logstor_logger.info("Write buffer stopped");
}

future<log_location_with_holder> buffered_writer::write(log_record record, db::timeout_clock::time_point timeout, write_target target) {
    auto holder = _async_gate.hold();

    log_record_writer writer(std::move(record));

    auto check_timeout = [timeout] {
        if (timeout != db::no_timeout && timeout <= db::timeout_clock::now()) {
            throw timed_out_error{};
        }
    };

    check_timeout();

    if (writer.size() > head_buf().max_record_size()) {
        throw std::runtime_error(fmt::format("Write size {} exceeds buffer size {}", writer.size(), head_buf().max_record_size()));
    }

    // Wait until the head buffer can fit this write.
    while (!head_buf().can_fit(writer)) {
        // Capture the current head before waiting.  Multiple concurrent writers
        // can all reach this point; only the first one to proceed actually
        // advances _head — the rest see _head != current_head and simply
        // re-check the (already advanced) head buffer.
        auto current_head = _head;
        while (ring_full() && !_async_gate.is_closed()) {
            co_await _head_can_advance.wait();
            check_timeout();
        }
        _async_gate.check();
        if (_head == current_head) {
            ++_head;
            // Wake other writers that are also waiting for a new head buffer.
            _head_can_advance.broadcast();
        }
    }

    auto fut = head_buf().write(std::move(writer), std::move(target));

    // Wake the consumer: there is now data at the tail.
    _tail_can_advance.broadcast();

    co_return co_await std::move(fut);
}

future<> buffered_writer::consumer_loop() {
    while (true) {
        // Wait for something to flush at the tail.
        while (!_async_gate.is_closed() && !tail_buf().has_data()) {
            co_await _tail_can_advance.wait();
        }

        if (!tail_buf().has_data()) {
            // Gate is closing and tail is empty — we are done.
            break;
        }

        // If head == tail, the head buffer is still being written to.  Seal it
        // by advancing the head to give writers a fresh slot.
        if (_head == _tail) {
            ++_head;
            // Wake writers that may be waiting for a new head slot.
            _head_can_advance.broadcast();
        }

        co_await _sm.write(tail_buf());

        tail_buf().reset();
        ++_tail;

        // A slot has been freed: wake any writer waiting to advance the head.
        _head_can_advance.broadcast();
    }
}

}
