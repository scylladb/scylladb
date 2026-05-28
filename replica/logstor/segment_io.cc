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
#include "idl/frozen_schema.dist.hh"
#include "idl/frozen_schema.dist.impl.hh"

namespace replica::logstor {

extern seastar::logger logstor_logger;

segment_header make_segment_header(const ondisk::buffer_header& bh, std::optional<ondisk::segment_header> sh) {
    segment_header seg_hdr {
        .kind = bh.kind,
        .segment_seq = bh.segment_seq,
    };

    switch (bh.kind) {
    case segment_kind::full:
        if (!sh) {
            throw std::runtime_error("Full segment buffer header without segment header");
        }
        seg_hdr.v = segment_header::full {
            .table = sh->table,
            .first_token = sh->first_token,
            .last_token = sh->last_token,
        };
        break;
    case segment_kind::mixed:
        seg_hdr.v = segment_header::mixed{};
        break;
    }
    return seg_hdr;
}

future<std::optional<segment_header>> read_segment_header(seastar::input_stream<char>& in) {
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

    co_return make_segment_header(bh, sh);
}

log_record deserialize_log_record(simple_memory_input_stream buf_stream) {
    auto rh_stream = buf_stream.read_substream(ondisk::record_header_size);
    auto rh = ser::deserialize(rh_stream, std::type_identity<ondisk::record_header>{});
    auto header_stream = buf_stream.read_substream(ondisk::serialized_log_record_header_size);
    auto data_stream = buf_stream.read_substream(rh.data_size);

    return log_record {
        .header = ser::deserialize(header_stream, std::type_identity<log_record_header>{}),
        .mut = ser::deserialize(data_stream, std::type_identity<canonical_mutation>{})
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
        segment_header_consumer on_segment_header,
        record_header_consumer on_record_header,
        record_consumer on_record) {
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

        auto seg_hdr = make_segment_header(bh, sh);
        co_await on_segment_header(seg_hdr);

        // TODO crc, torn writes

        const auto buffer_data_end_position = current_position + bh.data_size;
        while (current_position < buffer_data_end_position) {
            // Read record header
            const auto record_offset = current_position;
            auto size_buf = co_await in.read_exactly(ondisk::record_header_size);
            current_position += ondisk::record_header_size;
            if (size_buf.size() < ondisk::record_header_size) {
                break;
            }
            auto rh = ser::deserialize_from_buffer(size_buf, std::type_identity<ondisk::record_header>{});
            if (ondisk::serialized_log_record_header_size + rh.data_size > buffer_data_end_position - current_position) {
                // invalid record size
                break;
            }

            logstor_logger.trace("Found record of size {} bytes in segment {}",
                                ondisk::serialized_log_record_header_size + rh.data_size, segment_id);

            // Read the log_record_header bytes
            auto header_buf = co_await in.read_exactly(ondisk::serialized_log_record_header_size);
            current_position += ondisk::serialized_log_record_header_size;
            if (header_buf.size() < ondisk::serialized_log_record_header_size) {
                break;
            }
            auto record_header = ser::deserialize_from_buffer(header_buf, std::type_identity<log_record_header>{});

            log_location loc {
                .segment = segment_id,
                .offset = static_cast<uint32_t>(record_offset),
                .size = static_cast<uint32_t>(ondisk::record_header_size + ondisk::serialized_log_record_header_size + rh.data_size)
            };

            if (on_record_header(loc, record_header) == want_data::yes) {
                auto mut_buf = co_await in.read_exactly(rh.data_size);
                current_position += rh.data_size;
                if (mut_buf.size() < rh.data_size) {
                    break;
                }
                auto mut = ser::deserialize_from_buffer(mut_buf, std::type_identity<canonical_mutation>{});
                co_await on_record(loc, log_record{std::move(record_header), std::move(mut)});
            } else {
                // Skip the canonical_mutation bytes without reading them
                co_await in.skip(rh.data_size);
                current_position += rh.data_size;
            }

            // align up to next record
            auto padding = align_up(current_position, ondisk::record_alignment) - current_position;
            if (padding > 0) {
                co_await in.skip(padding);
                current_position += padding;
            }
        }

        if (seg_hdr.kind == segment_kind::full) {
            // A segment of this kind has only a single buffer
            break;
        }

        if (current_position < buffer_data_end_position) {
            // skip remaining buffer data
            auto bytes_to_skip = buffer_data_end_position - current_position;
            co_await in.skip(bytes_to_skip);
            current_position += bytes_to_skip;
        }
    }
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
