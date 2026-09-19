/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "replica/logstor/segment_io.hh"
#include "replica/logstor/logstor.hh"

#include <seastar/core/align.hh>
#include <seastar/core/simple-stream.hh>

#include "replica/logstor/write_buffer.hh"
#include "serializer_impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

segment_info make_segment_info(const ondisk::buffer_header& bh, std::optional<ondisk::segment_header> sh) {
    segment_info seg_info {
        .kind = bh.kind,
        .segment_seq = bh.segment_seq,
    };

    switch (bh.kind) {
    case segment_kind::full:
        if (!sh) {
            throw std::runtime_error("Full segment buffer header without segment header");
        }
        seg_info.v = segment_info::full {
            .table = sh->table,
            .first_token = sh->first_token,
            .last_token = sh->last_token,
        };
        break;
    case segment_kind::mixed:
        seg_info.v = segment_info::mixed{};
        break;
    }
    return seg_info;
}

future<std::optional<segment_info>> read_segment_info(seastar::input_stream<char>& in) {
    auto bh_buf = co_await in.read_exactly(ondisk::buffer_header_size);
    if (bh_buf.size() < ondisk::buffer_header_size) {
        co_return std::nullopt;
    }
    auto bh = ser::deserialize_from_buffer(bh_buf, std::type_identity<ondisk::buffer_header>{});

    if (!ondisk::validate_header(bh)) {
        co_return std::nullopt;
    }

    std::optional<ondisk::segment_header> sh;
    if (bh.kind == segment_kind::full) {
        auto sh_buf = co_await in.read_exactly(ondisk::segment_header_size);
        if (sh_buf.size() < ondisk::segment_header_size) {
            co_return std::nullopt;
        }
        sh = ser::deserialize_from_buffer(sh_buf, std::type_identity<ondisk::segment_header>{});
    }

    co_return make_segment_info(bh, sh);
}

log_record deserialize_log_record(simple_memory_input_stream buf_stream) {
    auto frame_header = ser::deserialize(buf_stream, std::type_identity<ondisk::record_frame_header>{});

    auto header_stream = buf_stream.read_substream(ondisk::record_header_size(frame_header.key_size));
    auto value_stream = buf_stream.read_substream(frame_header.value_size);

    return log_record {
        .header = ondisk::read_record_header(header_stream, frame_header.key_size),
        .value = record_value(bytes_view(reinterpret_cast<const int8_t*>(value_stream.begin()), value_stream.size())),
    };
}

future<log_record> read_log_record(seastar::input_stream<char>& in, log_location loc) {
    auto buf = co_await in.read_exactly(loc.size);
    if (buf.size() < loc.size) {
        throw std::runtime_error(fmt::format("Truncated log record at {}", loc));
    }
    co_return deserialize_log_record(simple_memory_input_stream(buf.begin(), buf.size()));
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        record_bytes_consumer on_record) {
    size_t current_position = 0;
    std::optional<segment_sequence> segment_seq;

    logstor_logger.trace("Reading records from segment {}", segment_id);

    while (current_position < segment_size) {
        // Align to block boundary
        auto skip_bytes = align_up(current_position, ondisk::block_alignment) - current_position;
        if (skip_bytes > 0) {
            co_await in.skip(skip_bytes);
            current_position += skip_bytes;
        }

        if (current_position >= segment_size) {
            break;
        }

        // read buffer header
        auto buffer_header_buf = co_await in.read_exactly(ondisk::buffer_header_size);
        current_position += ondisk::buffer_header_size;
        if (buffer_header_buf.size() < ondisk::buffer_header_size) {
            break;
        }
        auto bh = ser::deserialize_from_buffer(buffer_header_buf, std::type_identity<ondisk::buffer_header>{});

        // if the buffer is invalid then skip the rest of the segment - buffer writes are sequential and serialized.
        if (!ondisk::validate_header(bh)) {
            break;
        }

        if (!segment_seq) {
            segment_seq = bh.segment_seq;
        } else if (bh.segment_seq != *segment_seq) {
            break;
        }

        std::optional<ondisk::segment_header> sh;
        if (bh.kind == segment_kind::full) {
            // read segment header
            auto segment_header_buf = co_await in.read_exactly(ondisk::segment_header_size);
            current_position += ondisk::segment_header_size;
            if (segment_header_buf.size() < ondisk::segment_header_size) {
                break;
            }
            sh = ser::deserialize_from_buffer(segment_header_buf, std::type_identity<ondisk::segment_header>{});
        }

        auto seg_info = make_segment_info(bh, sh);
        co_await on_segment_info(seg_info);

        // TODO crc, torn writes

        const auto records_end_position = current_position + bh.records_size;
        // The smallest record frame: a record_frame_header and a record_header with an empty key.
        constexpr size_t min_frame_size = ondisk::record_frame_header_size + ondisk::record_header_fixed_size;

        while (current_position < records_end_position) {
            const auto record_offset = current_position;
            // What is left of this buffer's records. The stream spans the whole segment and is not
            // bounded per buffer, so each size is checked against this before its bytes are read;
            // otherwise a short tail or a record claiming more than the buffer holds would consume
            // the next buffer's bytes. The loop condition keeps this at 1 or more, so the
            // subtraction below cannot wrap.
            const size_t buffer_bytes_left = records_end_position - current_position;
            if (buffer_bytes_left < min_frame_size) {
                break;
            }
            auto frame_header_buf = co_await in.read_exactly(ondisk::record_frame_header_size);
            current_position += ondisk::record_frame_header_size;
            if (frame_header_buf.size() < ondisk::record_frame_header_size) {
                break;
            }
            auto frame_header = ser::deserialize_from_buffer(frame_header_buf, std::type_identity<ondisk::record_frame_header>{});
            if (!ondisk::validate_record_frame_header(frame_header) || size_t(frame_header.key_size) + frame_header.value_size > buffer_bytes_left - min_frame_size) {
                // invalid record size
                break;
            }
            const size_t header_size = ondisk::record_header_size(frame_header.key_size);

            logstor_logger.trace("Found record of size {} bytes in segment {}",
                                header_size + frame_header.value_size, segment_id);

            // Read the record_header bytes
            auto header_buf = co_await in.read_exactly(header_size);
            current_position += header_size;
            if (header_buf.size() < header_size) {
                break;
            }
            auto header_stream = simple_memory_input_stream(header_buf.get(), header_buf.size());
            auto header = ondisk::read_record_header(header_stream, frame_header.key_size);

            log_location loc {
                .segment = segment_id,
                .offset = static_cast<uint32_t>(record_offset),
                .size = static_cast<uint32_t>(ondisk::record_frame_header_size + header_size + frame_header.value_size)
            };

            if (on_record_header(loc, header) == want_data::yes) {
                auto value_buf = co_await in.read_exactly(frame_header.value_size);
                current_position += frame_header.value_size;
                if (value_buf.size() < frame_header.value_size) {
                    break;
                }
                log_record_bytes_view record_view{
                    .header = bytes_view(reinterpret_cast<const int8_t*>(header_buf.get()), header_buf.size()),
                    .value = bytes_view(reinterpret_cast<const int8_t*>(value_buf.get()), value_buf.size()),
                };
                co_await on_record(loc, header, record_view);
            } else {
                // Skip the value bytes without reading them
                co_await in.skip(frame_header.value_size);
                current_position += frame_header.value_size;
            }

            // align up to next record
            auto padding = align_up(current_position, ondisk::record_alignment) - current_position;
            if (padding > 0) {
                co_await in.skip(padding);
                current_position += padding;
            }
        }

        if (seg_info.kind == segment_kind::full) {
            // A segment of this kind has only a single buffer
            break;
        }

        if (current_position < records_end_position) {
            // skip remaining buffer data
            auto bytes_to_skip = records_end_position - current_position;
            co_await in.skip(bytes_to_skip);
            current_position += bytes_to_skip;
        }
    }
}

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_info_consumer on_segment_info,
        record_header_consumer on_record_header,
        record_consumer on_record) {
    co_await scan_segment(in, segment_id, segment_size,
            std::move(on_segment_info), std::move(on_record_header),
            [on_record = std::move(on_record)] (log_location loc, const record_header& header, log_record_bytes_view record_view) mutable -> future<> {
                co_await on_record(loc, log_record{header, record_value(record_view.value)});
            });
}

streamed_segment_rewriter::streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer)
    : _target_segment(target_segment)
    , _target_seq(target_seq)
    , _on_buffer(std::move(on_buffer)) {
}

ondisk::buffer_header streamed_segment_rewriter::read_buffer_header() const {
    simple_memory_input_stream bh_stream(_pending_data.data(), ondisk::buffer_header_size);
    return ser::deserialize(bh_stream, std::type_identity<ondisk::buffer_header>{});
}

void streamed_segment_rewriter::maybe_parse_initial_header() {
    if (_initial_header_size || _pending_data.size() < ondisk::buffer_header_size) {
        return;
    }

    auto bh = read_buffer_header();
    if (!ondisk::validate_header(bh)) {
        throw std::runtime_error("Invalid streamed logstor buffer header");
    }

    size_t header_size = ondisk::buffer_header_size;
    if (bh.kind == segment_kind::full) {
        header_size += ondisk::segment_header_size;
    }
    _initial_header_size = header_size;
}

void streamed_segment_rewriter::rewrite_buffer_header() {
    auto bh = read_buffer_header();

    logstor_logger.trace("Rewriting buffer header for segment {} seq {} with seq {}", _target_segment, bh.segment_seq, _target_seq);

    bh.segment_seq = _target_seq;
    bh.crc = bh.calculate_crc();

    simple_memory_output_stream bh_stream(_pending_data.data(), ondisk::buffer_header_size);
    ser::serialize<ondisk::buffer_header>(bh_stream, bh);
}

future<> streamed_segment_rewriter::flush_pending_data() {
    if (_pending_data.empty()) {
        co_return;
    }
    co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(_pending_data.data()), _pending_data.size()));
    _pending_data.clear();
}

future<> streamed_segment_rewriter::put(std::span<temporary_buffer<char>> data) {
    for (auto& buf : data) {
        if (buf.empty()) {
            continue;
        }

        if (_header_rewritten) {
            co_await _on_buffer(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()));
            continue;
        }

        _pending_data.insert(_pending_data.end(), buf.get(), buf.get() + buf.size());
        maybe_parse_initial_header();
        if (_initial_header_size && _pending_data.size() >= *_initial_header_size) {
            rewrite_buffer_header();
            _header_rewritten = true;
            co_await flush_pending_data();
        }
    }
}

future<> streamed_segment_rewriter::close() {
    if (!_header_rewritten && !_pending_data.empty()) {
        throw std::runtime_error("Truncated streamed logstor segment header");
    }
    co_return;
}

size_t streamed_segment_rewriter::buffer_size() const noexcept {
    return 128 * 1024;
}

}
