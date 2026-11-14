/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/rle.hh"

#include <seastar/core/on_internal_error.hh>

#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger rle_logger("parquet_rle");

// The specification limits every run to 2^31 - 1 values.
static constexpr uint64_t max_run_length = (uint64_t(1) << 31) - 1;
// The bit-packed run header is one byte: (groups << 1) | 1 < 128.
static constexpr unsigned max_bp_groups = 63;

static unsigned value_bytes(unsigned bit_width) noexcept {
    return (bit_width + 7) / 8;
}

// rle_bp_encoder

rle_bp_encoder::rle_bp_encoder(unsigned bit_width)
    : _bit_width(bit_width)
{
    if (bit_width > 32) {
        on_internal_error(rle_logger, seastar::format("rle_bp_encoder: bit width {} is above 32", bit_width));
    }
}

void rle_bp_encoder::append_run(uint32_t v, uint64_t n) {
    if (n == 0) {
        return;
    }
    _count += n;
    if (_run_length && v == _run_value) {
        _run_length += n;
        return;
    }
    flush_run();
    _run_value = v;
    _run_length = n;
}

// Moves the pending run into the output. Literals precede the run, and they
// must form whole groups of 8 before an RLE run can follow. If the run is
// long enough, its first values complete the group of literals, and the
// rest becomes an RLE run. Otherwise the run becomes literals. Either way,
// at most 7 + min_rle_run values are handled one by one, so this takes
// constant time.
void rle_bp_encoder::flush_run() {
    uint64_t n = std::exchange(_run_length, 0);
    const uint32_t v = _run_value;
    const unsigned fill = _literal_count ? 8 - _literal_count : 0;
    if (n < fill + min_rle_run) {
        for (; n; --n) {
            push_literal(v);
        }
        return;
    }
    for (unsigned i = 0; i < fill; ++i) {
        push_literal(v);
    }
    n -= fill;
    close_bp_run();
    write_rle_run(v, n);
}

void rle_bp_encoder::push_literal(uint32_t v) {
    _literals[_literal_count++] = v;
    if (_literal_count == 8) {
        write_group();
    }
}

void rle_bp_encoder::write_group() {
    if (!_bp_header || _bp_groups == max_bp_groups) {
        close_bp_run();
        _bp_header = _out.write_place_holder(1);
        _bp_groups = 0;
    }
    uint8_t packed[32];
    pack8(_literals.data(), _bit_width, packed);
    _out.write(reinterpret_cast<const char*>(packed), _bit_width);
    ++_bp_groups;
    _literal_count = 0;
}

void rle_bp_encoder::close_bp_run() {
    if (_bp_header) {
        *_bp_header = static_cast<bytes_ostream::value_type>((_bp_groups << 1) | 1);
        _bp_header = nullptr;
    }
}

void rle_bp_encoder::write_rle_run(uint32_t v, uint64_t n) {
    const unsigned vb = value_bytes(_bit_width);
    while (n) {
        const uint64_t len = std::min(n, max_run_length);
        write_uleb128(_out, len << 1);
        _out.write(reinterpret_cast<const char*>(&v), vb);
        n -= len;
    }
}

uint64_t rle_bp_encoder::estimated_size() const noexcept {
    // A pending bit-packed group, plus one header byte in case it opens a run.
    const uint64_t group = _bit_width + 1;
    uint64_t pending = 0;
    const unsigned fill = _literal_count ? 8 - _literal_count : 0;
    if (_run_length >= fill + min_rle_run) {
        const uint64_t rle = _run_length - fill;
        const uint64_t runs = (rle + max_run_length - 1) / max_run_length;
        pending = (_literal_count ? group : 0) + runs * (uleb128_size(std::min(rle, max_run_length) << 1) + value_bytes(_bit_width));
    } else {
        pending = (_literal_count + _run_length + 7) / 8 * group;
    }
    return _out.size() + pending;
}

bytes_ostream rle_bp_encoder::finish() {
    flush_run();
    if (_literal_count) {
        std::fill(_literals.begin() + _literal_count, _literals.end(), 0);
        write_group();
    }
    close_bp_run();
    _count = 0;
    return std::exchange(_out, bytes_ostream());
}

// rle_bp_decoder

rle_bp_decoder::rle_bp_decoder(byte_array_view data, unsigned bit_width)
    : _in(data)
    , _bit_width(bit_width)
    , _value_bytes(value_bytes(bit_width))
{
    if (bit_width > 32) {
        on_internal_error(rle_logger, seastar::format("rle_bp_decoder: bit width {} is above 32", bit_width));
    }
}

void rle_bp_decoder::next_header() {
    // Runs of length zero are not valid, but they consume input, so
    // skipping them cannot loop forever.
    do {
        const uint32_t header = _in.read_uleb128_32();
        if (header & 1) {
            _repeated = false;
            _left = uint64_t(header >> 1) * 8;
            _group_pos = 8;
        } else {
            _repeated = true;
            _left = header >> 1;
            uint32_t v = 0;
            _in.read(&v, _value_bytes);
            if (_bit_width < 32 && (v >> _bit_width)) {
                _in.fail("RLE run value does not fit in the bit width");
            }
            _value = v;
        }
    } while (_left == 0);
}

void rle_bp_decoder::load_group() {
    if (const uint8_t* in = _in.try_read_padded(_bit_width, unpack_padding)) [[likely]] {
        unpack8_padded(in, _bit_width, _group.data());
    } else {
        uint8_t packed[32];
        _in.read(packed, _bit_width);
        unpack8(packed, _bit_width, _group.data());
    }
    _group_pos = 0;
}

rle_bp_decoder::run rle_bp_decoder::next_run(uint64_t max) {
    if (_left == 0) {
        next_header();
    }
    if (_repeated) {
        const uint64_t n = std::min(_left, max);
        _left -= n;
        return {_value, n};
    }
    if (_group_pos == 8) {
        load_group();
    }
    const uint32_t v = _group[_group_pos];
    uint64_t n = 0;
    // Collect equal values up to the end of the group. A run that
    // continues into the next group is returned by the next call.
    do {
        ++_group_pos;
        --_left;
        ++n;
    } while (n < max && _group_pos < 8 && _left && _group[_group_pos] == v);
    return {v, n};
}

size_t rle_bp_decoder::next_packed(std::span<uint8_t> out) {
    if (_left == 0) {
        next_header();
    }
    if (_repeated) {
        return 0;
    }
    size_t i = 0;
    while (i < out.size() && _left) {
        if (_group_pos == 8) {
            load_group();
        }
        const size_t n = std::min<uint64_t>({_left, uint64_t(8 - _group_pos), out.size() - i});
        for (size_t k = 0; k < n; ++k) {
            out[i++] = uint8_t(_group[_group_pos++]);
        }
        _left -= n;
    }
    return i;
}

template <typename T>
void rle_bp_decoder::decode(std::span<T> out) {
    size_t i = 0;
    while (i < out.size()) {
        if (_left == 0) {
            next_header();
        }
        if (_repeated) {
            const uint64_t n = std::min<uint64_t>(_left, out.size() - i);
            std::fill_n(out.begin() + i, n, T(_value));
            _left -= n;
            i += n;
            continue;
        }
        if (_group_pos == 8) {
            load_group();
        }
        const uint64_t n = std::min<uint64_t>({_left, uint64_t(8 - _group_pos), out.size() - i});
        for (uint64_t k = 0; k < n; ++k) {
            out[i++] = T(_group[_group_pos++]);
        }
        _left -= n;
    }
}

template void rle_bp_decoder::decode<uint32_t>(std::span<uint32_t>);
template void rle_bp_decoder::decode<uint16_t>(std::span<uint16_t>);
template void rle_bp_decoder::decode<int16_t>(std::span<int16_t>);
template void rle_bp_decoder::decode<uint8_t>(std::span<uint8_t>);
template void rle_bp_decoder::decode<bool>(std::span<bool>);

void rle_bp_decoder::skip(uint64_t n) {
    while (n) {
        n -= next_run(n).length;
    }
}

// rle_size_estimator

void rle_size_estimator::close_run() noexcept {
    if (_run_length >= rle_bp_encoder::min_rle_run) {
        ++_rle_runs;
        _rle_header_bytes += uleb128_size(std::min(_run_length, max_run_length) << 1);
    } else {
        _packed_values += _run_length;
    }
    _run_length = 0;
}

uint64_t rle_size_estimator::estimate(unsigned bit_width) const noexcept {
    uint64_t rle_runs = _rle_runs;
    uint64_t header_bytes = _rle_header_bytes;
    uint64_t packed = _packed_values;
    if (_run_length >= rle_bp_encoder::min_rle_run) {
        ++rle_runs;
        header_bytes += uleb128_size(std::min(_run_length, max_run_length) << 1);
    } else {
        packed += _run_length;
    }
    const uint64_t groups = (packed + 7) / 8;
    // Bit-packed runs are interrupted by RLE runs, so each RLE run may also
    // cost one bit-packed run header.
    const uint64_t bp_headers = groups ? groups / max_bp_groups + 1 + rle_runs : 0;
    return header_bytes + rle_runs * value_bytes(bit_width) + groups * bit_width + bp_headers;
}

} // namespace sstables::parquet
