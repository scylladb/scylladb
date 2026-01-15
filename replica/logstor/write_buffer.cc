/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "write_buffer.hh"
#include "segment_manager.hh"
#include "bytes_fwd.hh"
#include "logstor.hh"
#include "replica/logstor/types.hh"
#include <seastar/core/simple-stream.hh>
#include <seastar/core/with_scheduling_group.hh>
#include "serializer_impl.hh"
#include "idl/logstor.dist.hh"
#include "idl/logstor.dist.impl.hh"
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>

namespace replica::logstor {

void log_record_writer::compute_size() const {
    seastar::measuring_output_stream ms;
    ser::serialize(ms, _record);
    _size = ms.size();
}

void log_record_writer::write(ostream& out) const {
    ser::serialize(out, _record);
}

// write_buffer

write_buffer::write_buffer(size_t buffer_size)
        : _buffer_size(buffer_size)
        , _buffer(seastar::allocate_aligned_buffer<char>(buffer_size, 4096))
{
    reset();
}

void write_buffer::reset() {
    _stream = seastar::simple_memory_output_stream(_buffer.get(), _buffer_size);
    _net_data_size = 0;
    _record_count = 0;
    _written = {};
}

size_t write_buffer::get_max_write_size() const noexcept {
    return _buffer_size - record_header_size;
}

bool write_buffer::can_fit(size_t data_size) const noexcept {
    // Calculate total space needed including header, data, and alignment padding
    auto total_size = record_header_size + data_size;
    auto aligned_size = align_up(total_size, record_alignment);
    return aligned_size <= _stream.size();
}

future<log_location> write_buffer::write(log_record_writer writer) {
    const auto data_size = writer.size();

    if (!can_fit(data_size)) {
        throw std::runtime_error(fmt::format("Write size {} exceeds buffer size {}", data_size, _stream.size()));
    }

    // Write header
    auto header_out = _stream.write_substream(record_header_size);
    ser::serialize<uint32_t>(header_out, data_size);

    // Write actual data
    size_t data_offset_in_buffer = offset_in_buffer();
    auto data_out = _stream.write_substream(data_size);
    writer.write(data_out);

    _net_data_size += data_size;
    _record_count++;

    // Add padding to align record
    pad_to_alignment(record_alignment);

    return _written.get_shared_future().then([data_offset_in_buffer, data_size] (log_location base_location) {
        return log_location {
            .segment = base_location.segment,
            .offset = base_location.offset + data_offset_in_buffer,
            .size = data_size
        };
    });
}

void write_buffer::pad_to_alignment(size_t alignment) {
    auto current_pos = offset_in_buffer();
    auto next_pos = align_up(current_pos, alignment);
    auto padding = next_pos - current_pos;
    if (padding > 0) {
        _stream.fill('\0', padding);
    }
}

void write_buffer::finalize(size_t alignment) {
    pad_to_alignment(alignment);
}

void write_buffer::complete_writes(log_location base_location) {
    _written.set_value(base_location);
}

void write_buffer::abort_writes(std::exception_ptr ex) noexcept {
    if (!_written.available()) {
        _written.set_exception(std::move(ex));
    }
}

size_t write_buffer::estimate_required_segments(size_t net_data_size, size_t record_count, size_t segment_size) {
    // Calculate total size needed including headers and alignment padding
    size_t total_size = record_header_size * record_count + net_data_size;

    // not perfect so let's multiply by some overhead constant
    total_size = static_cast<size_t>(total_size * 1.1);

    return align_up(total_size, segment_size) / segment_size;

}

// buffered_writer

buffered_writer::buffered_writer(segment_manager& sm, seastar::scheduling_group flush_sg)
        : _sm(sm)
        , _active_buffer({
            .buf = write_buffer(_sm.get_segment_size()),
        })
        , _available_buffers(num_flushing_buffers)
        , _flush_sg(flush_sg) {
    for (size_t i = 0; i < num_flushing_buffers; ++i) {
        _available_buffers.push(write_buffer(_sm.get_segment_size()));
    }
}

future<> buffered_writer::start() {
    logstor_logger.info("Starting write buffer");
    co_return;
}

future<> buffered_writer::stop() {
    if (_async_gate.is_closed()) {
        co_return;
    }
    logstor_logger.info("Stopping write buffer");

    co_await _async_gate.close();
    logstor_logger.info("Write buffer stopped");
}

future<log_location> buffered_writer::write(log_record record) {
    auto holder = _async_gate.hold();

    log_record_writer writer(std::move(record));

    if (writer.size() > _active_buffer.buf.get_max_write_size()) {
        throw std::runtime_error(fmt::format("Write size {} exceeds buffer size {}", writer.size(), _active_buffer.buf.get_max_write_size()));
    }

    // Check if write fits in current buffer
    while (!_active_buffer.buf.can_fit(writer)) {
        co_await _buffer_switched.wait();
    }

    // Write to buffer at current position
    auto fut = _active_buffer.buf.write(std::move(writer));

    // Trigger flush for the active buffer if not in progress
    if (!std::exchange(_active_buffer.flush_requested, true)) {
        (void)with_gate(_async_gate, [this] {
            return switch_buffer().then([this] (write_buffer old_buf) mutable {
                return with_scheduling_group(_flush_sg, [this, buf = std::move(old_buf)] mutable {
                    return flush(std::move(buf));
                });
            });
        });
    }

    co_return co_await std::move(fut);
}

future<write_buffer> buffered_writer::switch_buffer() {
    // Wait for and get the next available buffer
    auto new_buf = co_await _available_buffers.pop_eventually();

    auto next_active_buffer = active_buffer {
        .buf = std::move(new_buf),
    };

    auto old_active_buffer = std::exchange(_active_buffer, std::move(next_active_buffer));
    _buffer_switched.broadcast();

    co_return std::move(old_active_buffer.buf);
}

future<> buffered_writer::flush(write_buffer buf) {
    co_await _sm.write(buf);

    // Return the flushed buffer to the available queue
    buf.reset();
    _available_buffers.push(std::move(buf));
}

}
