/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <stdexcept>
#include <cstdlib>

#include <seastar/core/align.hh>
#include <seastar/core/bitops.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/on_internal_error.hh>

#include "compress.hh"
#include "compressor.hh"
#include "framed_chunk.hh"
#include "exceptions.hh"
#include "unimplemented.hh"
#include "sstables/stats.hh"
#include "sstables/version.hh"
#include "sstables/checksum_utils.hh"
#include "segmented_compress_params.hh"
#include "utils/assert.hh"
#include "utils/unqualified_name.hh"
#include "reader_permit.hh"
#include "data_source_types.hh"
#include "utils/div_ceil.hh"

namespace sstables {

extern logging::logger sstlog;

enum class mask_type : uint8_t {
    set,
    clear
};

// size_bits cannot be >= 64
static inline uint64_t make_mask(uint8_t size_bits, uint8_t offset, mask_type t) noexcept {
    const uint64_t mask = ((1 << size_bits) - 1) << offset;
    return t == mask_type::set ? mask : ~mask;
}

/*
 * ----> memory addresses
 * MSB           LSB
 * | | | | |3|2|1|0| CPU integer (big or little endian byte order)
 *          -------
 *             |
 *       +-----+ << shift = prefix bits
 *       |
 *    -------
 *  7 6 5 4 3 2 1 0  index*
 * | |3|2|1|0| | | | raw storage (unaligned, little endian byte order)
 *  = ------- =====
 *  |           |
 *  |           +-> prefix bits
 *  +-> suffix bits
 *
 * |0|1|1|1|1|0|0|0| read/write mask
 *
 * * On big endian systems the indices in storage are reversed and
 *   run left to right: 0 1 .. 6 7. To avoid differences in the
 *   encoding logic on machines with different native byte orders
 *   reads and writes to storage must be explicitly little endian.
 */
struct bit_displacement {
    uint64_t shift;
    uint64_t mask;
};

inline bit_displacement displacement_for(uint64_t prefix_bits, uint8_t size_bits, mask_type t) {
    return {prefix_bits, make_mask(size_bits, prefix_bits, t)};
}

size_t chunk_length_field_bits(uint32_t uncompressed_chunk_length) {
    // A valid compressed chunk is at most compressed_chunk_length_limit bytes, so
    // this many bits can represent any valid length.
    return log2ceil(compressed_chunk_length_limit(uncompressed_chunk_length));
}

size_t chunk_length_field_size(uint32_t uncompressed_chunk_length) {
    // One length, rounded up to whole bytes.
    return div_ceil(chunk_length_field_bits(uncompressed_chunk_length), 8);
}

// Read/write an n-byte (n <= 8) little-endian integer. Unlike seastar's
// read_le/write_le these touch exactly n bytes, so they are safe when the
// backing buffer is only chunk_length_field_size bytes long.
static uint64_t read_le_bytes(const char* p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) {
        v |= uint64_t(static_cast<uint8_t>(p[i])) << (8 * i);
    }
    return v;
}

static void write_le_bytes(char* p, size_t n, uint64_t v) {
    for (size_t i = 0; i < n; ++i) {
        p[i] = static_cast<char>(static_cast<uint8_t>(v >> (8 * i)));
    }
}

void write_chunk_length_field(char* dst, uint32_t uncompressed_chunk_length,
        uint32_t compressed_len) {
    write_le_bytes(dst, chunk_length_field_size(uncompressed_chunk_length), compressed_len);
}

uint32_t read_chunk_length_field(const char* src, uint32_t uncompressed_chunk_length) {
    const uint64_t compressed_len = read_le_bytes(src, chunk_length_field_size(uncompressed_chunk_length));
    const uint64_t bound = compressed_chunk_length_limit(uncompressed_chunk_length);
    if (compressed_len > bound) {
        throw_malformed_sstable_exception(format(
                "compressed chunk length field out of range: got {}, expected at most {}",
                compressed_len, bound));
    }
    return compressed_len;
}

void framed_chunk::throw_checksum_mismatch(uint64_t file_offset, uint32_t actual) const {
    throw_malformed_sstable_exception(format("compressed chunk of size {} at file offset {} failed checksum, expected={}, actual={}",
            extent(), file_offset, stored_checksum, actual));
}

framed_chunk_codec::framed_chunk_codec(sstable_version_types v, uint32_t uncompressed_chunk_length)
    : framed_chunk_codec(uncompressed_chunk_length, chunk_has_length_framing(v) ? chunk_length_field_size(uncompressed_chunk_length) : 0)
{}

framed_chunk_codec framed_chunk_codec::for_framed_chunks(uint32_t uncompressed_chunk_length) {
    return framed_chunk_codec(uncompressed_chunk_length, chunk_length_field_size(uncompressed_chunk_length));
}

uint64_t framed_chunk_codec::extent_from_frame(const char* frame) const {
    return extent(read_chunk_length_field(frame, _uncompressed_chunk_length));
}

framed_chunk framed_chunk_codec::parse(std::string_view chunk) const {
    if (chunk.size() < extent(0)) {
        throw_malformed_sstable_exception(format("compressed chunk of size {} is too small to hold its {}-byte framing and its checksum",
                chunk.size(), 2 * _frame_len));
    }
    if (framed()) {
        auto result = parse(chunk.substr(0, _frame_len), chunk.substr(_frame_len));
        if (result.extent() != chunk.size()) {
            throw_malformed_sstable_exception(format("compressed chunk header declares size {}, but the chunk has size {}",
                    result.extent(), chunk.size()));
        }
        return result;
    }
    const auto compressed_len = chunk.size() - sizeof(uint32_t);
    return framed_chunk{
        .compressed = chunk.substr(0, compressed_len),
        .stored_checksum = read_be<uint32_t>(chunk.data() + compressed_len),
    };
}

framed_chunk framed_chunk_codec::parse(std::string_view header, std::string_view rest) const {
    SCYLLA_ASSERT(framed() && header.size() == _frame_len);
    const uint64_t compressed_len = read_chunk_length_field(header.data(), _uncompressed_chunk_length);
    if (rest.size() < compressed_len + sizeof(uint32_t) + _frame_len) {
        throw_malformed_sstable_exception(format("compressed chunk header declares size {}, but got buffer of size {}",
                extent(compressed_len), _frame_len + rest.size()));
    }
    return framed_chunk{
        .header = header,
        .compressed = rest.substr(0, compressed_len),
        .stored_checksum = read_be<uint32_t>(rest.data() + compressed_len),
        .footer = rest.substr(compressed_len + sizeof(uint32_t), _frame_len),
    };
}

temporary_buffer<char> framed_chunk_codec::decompress(const compressor& c, const framed_chunk& chunk) const {
    // The uncompressed data takes exactly uncompressed_chunk_length bytes, or
    // less for the last chunk.
    temporary_buffer<char> out(_uncompressed_chunk_length);
    out.trim(c.uncompress(chunk.compressed.data(), chunk.compressed.size(), out.get_write(), out.size()));
    return out;
}

void framed_chunk_codec::write_frames(char* chunk, uint32_t compressed_len) const {
    SCYLLA_ASSERT(framed());
    write_chunk_length_field(chunk, _uncompressed_chunk_length, compressed_len);
    write_chunk_length_field(chunk + _frame_len + compressed_len + sizeof(uint32_t), _uncompressed_chunk_length, compressed_len);
}

std::pair<bucket_info, segment_info> params_for_chunk_size(uint32_t chunk_size) {
    const uint8_t chunk_size_log2 = log2ceil(chunk_size);

    auto it = std::ranges::find_if(bucket_infos, [&] (const bucket_info& bi) {
        return bi.chunk_size_log2 == chunk_size_log2;
    });

    // This scenario should be so rare that we only fall back to a safe
    // set of parameters, not optimal ones.
    if (it == bucket_infos.end()) {
        const uint8_t data_size = bucket_infos.front().best_data_size_log2;
        return {{chunk_size_log2, data_size, (8 * bucket_size - 56) / data_size},
            {chunk_size_log2, data_size, uint8_t(1)}};
    }

    auto b = *it;
    auto s = *std::ranges::find_if(segment_infos, [&] (const segment_info& si) {
        return si.data_size_log2 == b.best_data_size_log2 && si.chunk_size_log2 == b.chunk_size_log2;
    });

    return {std::move(b), std::move(s)};
}

void compression::segmented_offsets::bucket_storage_deleter::operator()(char* p) const noexcept {
    delete[] p;
    sstables_stats::on_compression_offsets_memory_freed(bucket_size);
}

uint64_t compression::segmented_offsets::read(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits) const {
    const uint64_t offset_byte = offset_bits / 8;
    uint64_t value = seastar::read_le<uint64_t>(_storage[bucket_index].storage.get() + offset_byte);

    const auto displacement = displacement_for(offset_bits % 8, size_bits, mask_type::set);

    value &= displacement.mask;
    value >>= displacement.shift;

    return value;
}

void compression::segmented_offsets::write(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits, uint64_t value) {
    const uint64_t offset_byte = offset_bits / 8;

    uint64_t old_value = seastar::read_le<uint64_t>(_storage[bucket_index].storage.get() + offset_byte);

    const auto displacement = displacement_for(offset_bits % 8, size_bits, mask_type::clear);

    value <<= displacement.shift;

    if ((~displacement.mask | value) != ~displacement.mask) {
        throw std::invalid_argument(format("{}: to-be-written value would overflow the allocated bits", __FUNCTION__));
    }

    old_value &= displacement.mask;
    value |= old_value;

    seastar::write_le(_storage[bucket_index].storage.get() + offset_byte, value);
}

void compression::segmented_offsets::state::update_position_trackers(std::size_t index, uint16_t segment_size_bits,
        uint32_t segments_per_bucket, uint8_t grouped_offsets) {
    if (_current_index != index - 1) {
        _current_index = index;
        const uint64_t current_segment_index = _current_index / grouped_offsets;
        _current_bucket_segment_index = current_segment_index % segments_per_bucket;
        _current_segment_relative_index = _current_index % grouped_offsets;
        _current_bucket_index = current_segment_index / segments_per_bucket;
        _current_segment_offset_bits = (_current_bucket_segment_index % segments_per_bucket) * segment_size_bits;
    } else {
        ++_current_index;
        ++_current_segment_relative_index;

        // Crossed segment boundary.
        if (_current_segment_relative_index == grouped_offsets) {
            ++_current_bucket_segment_index;
            _current_segment_relative_index = 0;

            // Crossed bucket boundary.
            if (_current_bucket_segment_index == segments_per_bucket) {
                ++_current_bucket_index;
                _current_bucket_segment_index = 0;
                _current_segment_offset_bits = 0;
            } else {
                _current_segment_offset_bits += segment_size_bits;
            }
        }
    }
}

void compression::segmented_offsets::init(uint32_t chunk_size) {
    if (chunk_size == 0) {
        throw_malformed_sstable_exception("Segmented offsets chunk size is zero.");
    }

    _chunk_size = chunk_size;

    const auto params = params_for_chunk_size(chunk_size);

    sstlog.trace(
            "{} {}(): chunk size {} (log2)",
            fmt::ptr(this),
            __FUNCTION__,
            static_cast<int>(params.first.chunk_size_log2));

    _grouped_offsets = params.second.grouped_offsets;
    _segment_base_offset_size_bits = params.second.data_size_log2;
    _segmented_offset_size_bits = static_cast<uint64_t>(log2ceil((_chunk_size + 64) * (_grouped_offsets - 1)));
    _segment_size_bits = _segment_base_offset_size_bits + (_grouped_offsets - 1) * _segmented_offset_size_bits;
    _segments_per_bucket = params.first.segments_per_bucket;
}

uint64_t compression::segmented_offsets::at(std::size_t i, compression::segmented_offsets::state& s) const {
    if (i >= _size) {
        throw std::out_of_range(format("{}: index {} is out of range", __FUNCTION__, i));
    }

    s.update_position_trackers(i, _segment_size_bits, _segments_per_bucket, _grouped_offsets);
    const uint64_t bucket_base_offset = _storage[s._current_bucket_index].base_offset;
    const uint64_t segment_base_offset = bucket_base_offset + read(s._current_bucket_index, s._current_segment_offset_bits, _segment_base_offset_size_bits);

    if (s._current_segment_relative_index == 0) {
        return segment_base_offset;
    }

    return segment_base_offset
        + read(s._current_bucket_index,
                s._current_segment_offset_bits + _segment_base_offset_size_bits + (s._current_segment_relative_index - 1) * _segmented_offset_size_bits,
                _segmented_offset_size_bits);
}

void compression::segmented_offsets::push_back(uint64_t offset, compression::segmented_offsets::state& s) {
    s.update_position_trackers(_size, _segment_size_bits, _segments_per_bucket, _grouped_offsets);

    if (s._current_bucket_index == _storage.size()) {
        auto storage = bucket_storage(new char[bucket_size]);
        sstables_stats::on_compression_offsets_memory_allocated(bucket_size);
        _storage.push_back(bucket{_last_written_offset, std::move(storage)});
    }

    const uint64_t bucket_base_offset = _storage[s._current_bucket_index].base_offset;

    if (s._current_segment_relative_index == 0) {
        write(s._current_bucket_index, s._current_segment_offset_bits, _segment_base_offset_size_bits, offset - bucket_base_offset);
    } else {
        const uint64_t segment_base_offset = bucket_base_offset + read(s._current_bucket_index, s._current_segment_offset_bits, _segment_base_offset_size_bits);
        write(s._current_bucket_index,
                s._current_segment_offset_bits + _segment_base_offset_size_bits + (s._current_segment_relative_index - 1) * _segmented_offset_size_bits,
                _segmented_offset_size_bits,
                offset - segment_base_offset);
    }
    _last_written_offset = offset;
    ++_size;
}

void compression::set_compressor(compressor_ptr c) {
    options.elements.clear();
    if (c) {
        unqualified_name uqn(compression_parameters::name_prefix, c->name());
        const sstring& cn = uqn;
        name.value = bytes(cn.begin(), cn.end());
        for (auto& [k, v] : c->options()) {
            if (k != compression_parameters::SSTABLE_COMPRESSION) {
                options.elements.push_back({
                    {bytes(k.begin(), k.end())},
                    {bytes(v.begin(), v.end())}
                });
            }
        }
    }
    _compressor = std::move(c);
}

void compression::discard_hidden_options() {
    auto is_hidden_option = [] (const option& o) -> bool {
        auto k_str = std::string_view(reinterpret_cast<const char*>(o.key.value.data()), o.key.value.size());
        return compressor::is_hidden_option_name(k_str);
    };
    decltype(options) filtered_options;
    for (auto& e : options.elements) {
        if (!is_hidden_option(e)) {
            filtered_options.elements.emplace_back(std::move(e));
        }
    }
    options = std::move(filtered_options);
}

compressor& compression::get_compressor() const {
    SCYLLA_ASSERT(_compressor);
    return *_compressor.get();
}

void compression::update(uint64_t compressed_file_length) {
    _compressed_file_length = compressed_file_length;
}

// locate() takes a byte position in the uncompressed stream, and finds the
// the location of the compressed chunk on disk which contains it, and the
// offset in this chunk.
// locate() may only be used for offsets of actual bytes, and in particular
// the end-of-file position (one past the last byte) MUST not be used. If the
// caller wants to read from the end of file, it should simply read nothing.
compression::chunk_and_offset
compression::locate(uint64_t position, const compression::segmented_offsets::accessor& accessor) const {
    auto ucl = uncompressed_chunk_length();
    auto chunk_index = position / ucl;
    decltype(ucl) chunk_offset = position % ucl;
    auto chunk_start = accessor.at(chunk_index);
    auto chunk_end = (chunk_index + 1 == offsets.size())
            ? _compressed_file_length
            : accessor.at(chunk_index + 1);
    return { chunk_start, chunk_end - chunk_start, chunk_offset };
}

std::map<sstring, sstring> options_from_compression(const compression& c) {
    std::map<sstring, sstring> result;
    result.emplace(compression_parameters::SSTABLE_COMPRESSION, sstring(c.name.value.begin(), c.name.value.end()));
    result.emplace(compression_parameters::CHUNK_LENGTH_KB, to_sstring(c.uncompressed_chunk_length() / 1024));
    for (const auto& [k, v] : c.options.elements) {
        auto k_str = sstring(k.value.begin(), k.value.end());
        auto v_str = sstring(v.value.begin(), v.value.end());
        if (compressor::is_hidden_option_name(k_str)) {
            continue;
        }
        result.emplace(std::move(k_str), std::move(v_str));
    }
    return result;
}

} // namespace sstables

// For SSTables 2.x (formats 'ka' and 'la'), the full checksum is a combination of checksums of compressed chunks.
// For SSTables 3.x (format 'mc'), however, it is supposed to contain the full checksum of the file written so
// the per-chunk checksums also count.
enum class compressed_checksum_mode {
    checksum_chunks_only,
    checksum_all,
};

template <ChecksumUtils ChecksumType, bool check_digest, compressed_checksum_mode mode>
class compressed_file_data_source_impl : public data_source_impl {
    std::function<future<input_stream<char>>()> _stream_creator;
    std::optional<input_stream<char>> _input_stream;
    sstables::compression* _compression_metadata;
    sstables::compression::segmented_offsets::accessor _offsets;
    sstables::framed_chunk_codec _codec;
    [[no_unique_address]] sstables::digest_members<check_digest> _digests;
    reader_permit _permit;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    compressed_file_data_source_impl(sstables::stream_creator_fn stream_creator, sstables::compression* cm,
                sstables::sstable_version_types version,
                sstables::disk_read_range range, file_input_stream_options options,
                reader_permit permit, std::optional<uint32_t> digest)
            : _compression_metadata(cm)
            , _offsets(_compression_metadata->offsets.get_accessor())
            , _codec(version, cm->uncompressed_chunk_length())
            , _permit(std::move(permit))
    {
        // This class is not supposed to be used with sstables indexed by physical position.
        // They are handled elsewhere.
        SCYLLA_ASSERT(!_codec.framed());

        uint64_t pos = range.start.to_logical();
        size_t len = range.end.to_logical() - range.start.to_logical();
        _pos = _beg_pos = pos;
        if (pos > _compression_metadata->uncompressed_file_length()) {
            throw std::runtime_error("attempt to uncompress beyond end");
        }
        if (len == 0 || pos == _compression_metadata->uncompressed_file_length()) {
            // Nothing to read
            _end_pos = _pos;
            return;
        }
        if (len <= _compression_metadata->uncompressed_file_length() - pos) {
            _end_pos = pos + len;
        } else {
            _end_pos = _compression_metadata->uncompressed_file_length();
        }
        if constexpr (check_digest) {
            if (!digest) {
                on_internal_error(sstables::sstlog, "Requested digest check but no digest was provided.");
            }
            if (_end_pos - _pos < _compression_metadata->uncompressed_file_length()) {
                sstables::sstlog.debug("Compressed reader cannot calculate digest with partial read: current pos={}, end pos={}, uncompressed file len={}. Disabling digest check.",
                        _pos, _end_pos, _compression_metadata->uncompressed_file_length());
                _digests = {false};
            } else {
                _digests = {true, *digest, ChecksumType::init_checksum()};
            }
        }
        // _beg_pos and _end_pos specify positions in the uncompressed stream.
        // We need to translate them into a range of compressed chunks,
        // and open a file_input_stream to read that range.
        auto start = _compression_metadata->locate(_beg_pos, _offsets);
        auto end = _compression_metadata->locate(_end_pos - 1, _offsets);
        _stream_creator = [stream_creator{std::move(stream_creator)}, start = start.chunk_start, length = end.chunk_start + end.chunk_len - start.chunk_start, options] mutable {
            return stream_creator(start, length, std::move(options));
        };
        _underlying_pos = start.chunk_start;
    }
    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _end_pos) {
            co_return temporary_buffer<char>();
        }

        if (!_input_stream) {
            _input_stream = co_await _stream_creator();
        }
        auto addr = _compression_metadata->locate(_pos, _offsets);
        // Uncompress the next chunk. We need to skip part of the first
        // chunk, but then continue to read from beginning of chunks.
        if (_pos != _beg_pos && addr.offset != 0) {
            throw std::runtime_error(format("compressed reader not aligned to chunk boundary: pos={} offset={}", _pos, addr.offset));
        }
        if (!addr.chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed chunk_len must be greater than zero, chunk_start={}", addr.chunk_start));
        }
        auto buf = co_await _input_stream->read_exactly(addr.chunk_len);
        if (buf.size() != addr.chunk_len) {
            sstables::throw_malformed_sstable_exception(format("compressed reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _underlying_pos, addr.chunk_len, buf.size()));
        }
        auto res_units = co_await _permit.request_memory(_compression_metadata->uncompressed_chunk_length());
        auto chunk = _codec.parse(std::string_view(buf.get(), buf.size()));
        // FIXME: Do not always calculate checksum - Cassandra has a
        // probability (defaulting to 1.0, but still...)
        auto actual_checksum = chunk.template verify_checksum<ChecksumType>(_underlying_pos);

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                _digests.actual_digest = chunk.template fold_into_digest<ChecksumType>(_digests.actual_digest, actual_checksum,
                        mode == compressed_checksum_mode::checksum_all);
            }
        }

        auto out = _codec.decompress(_compression_metadata->get_compressor(), chunk);
        out.trim_front(addr.offset);
        _pos += out.size();
        _underlying_pos += addr.chunk_len;

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest
                    && _pos == _compression_metadata->uncompressed_file_length()
                    && _digests.expected_digest != _digests.actual_digest) {
                sstables::throw_malformed_sstable_exception(seastar::format("Digest mismatch: expected={}, actual={}", _digests.expected_digest, _digests.actual_digest));
            }
        }
        co_return make_tracked_temporary_buffer(std::move(out), std::move(res_units));
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                sstables::sstlog.debug("Compressed reader cannot calculate digest with skipped data: current pos={}, end pos={}, skip len={}. Disabling digest check.", _pos, _end_pos, n);
                _digests.can_calculate_digest = false;
            }
        }
        if (_pos + n > _end_pos) {
            on_internal_error(sstables::sstlog, format("Skipping over the end position is disallowed: current pos={}, end pos={}, skip len={}", _pos, _end_pos, n));
        }
        _pos += n;
        if (_pos == _end_pos) {
            co_return temporary_buffer<char>();
        }
        auto addr = _compression_metadata->locate(_pos, _offsets);
        auto underlying_n = addr.chunk_start - _underlying_pos;
        _underlying_pos = addr.chunk_start;
        _beg_pos = _pos;
        if (!_input_stream) {
            _input_stream = co_await _stream_creator();
        }
        co_await _input_stream->skip(underlying_n);
        co_return temporary_buffer<char>();
    }
};

template <bool check_digest>
class compressed_raw_file_data_source_impl : public data_source_impl {
    std::function<future<input_stream<char>>()> _stream_creator;
    std::optional<input_stream<char>> _input_stream;
    sstables::compression* _compression_metadata;
    sstables::framed_chunk_codec _codec;
    sstables::compression::segmented_offsets::accessor _offsets;
    [[no_unique_address]] sstables::digest_members<check_digest> _digests;
    reader_permit _permit;
    uint64_t _pos{0};
    uint64_t _current_chunk_index{0};

private:
    uint64_t get_chunk_len(uint64_t chunk_index) const {
        auto chunk_start = _offsets.at(chunk_index);
        auto chunk_end = (chunk_index + 1 == _compression_metadata->offsets.size())
                ? _compression_metadata->compressed_file_length()
                : _offsets.at(chunk_index + 1);
        auto len = chunk_end - chunk_start;
        return len;
    }

public:
    compressed_raw_file_data_source_impl(sstables::stream_creator_fn stream_creator, sstables::compression* cm,
                sstables::sstable_version_types version,
                file_input_stream_options options,
                reader_permit permit, std::optional<uint32_t> digest)
            : _compression_metadata(cm)
            , _codec(version, cm->uncompressed_chunk_length())
            , _offsets(_compression_metadata->offsets.get_accessor())
            , _permit(std::move(permit))
    {
        if constexpr (check_digest) {
            if (!digest) {
                on_internal_error(sstables::sstlog, "Requested digest check but no digest was provided.");
            }
            _digests = {true, *digest, crc32_utils::init_checksum()};
        }

        _stream_creator = [stream_creator{std::move(stream_creator)}, start = _pos, length = _compression_metadata->compressed_file_length(), options] mutable {
            return stream_creator(start, length, std::move(options));
        };
    }

    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _compression_metadata->compressed_file_length()) {
            co_return temporary_buffer<char>();
        }

        if (!_input_stream) {
            _input_stream = co_await _stream_creator();
        }

        uint64_t chunk_len;
        temporary_buffer<char> buf;
        std::optional<reader_permit::resource_units> res_units;
        if (const auto frame_len = _codec.frame_len()) {
            // A framed chunk's header holds its length.
            auto header = co_await _input_stream->read_exactly(frame_len);
            if (header.size() != frame_len) {
                sstables::throw_malformed_sstable_exception(format("compressed raw reader hit premature end-of-file at file offset {}, expected chunk header of {} bytes, actual={}", _pos, frame_len, header.size()));
            }
            chunk_len = _codec.extent_from_frame(header.get());
            res_units = co_await _permit.request_memory(chunk_len);
            auto body = co_await _input_stream->read_exactly(chunk_len - frame_len);
            if (body.size() != chunk_len - frame_len) {
                sstables::throw_malformed_sstable_exception(format("compressed raw reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _pos, chunk_len, frame_len + body.size()));
            }
            // Reassemble the header with the rest into a single buffer.
            buf = temporary_buffer<char>(chunk_len);
            std::copy_n(header.get(), frame_len, buf.get_write());
            std::copy_n(body.get(), body.size(), buf.get_write() + frame_len);
        } else {
            chunk_len = get_chunk_len(_current_chunk_index);
            if (!chunk_len) {
                sstables::throw_malformed_sstable_exception(format("compressed raw reader chunk_len must be greater than zero, pos={}", _pos));
            }
            res_units = co_await _permit.request_memory(chunk_len);
            buf = co_await _input_stream->read_exactly(chunk_len);
            if (buf.size() != chunk_len) {
                sstables::throw_malformed_sstable_exception(format("compressed raw reader hit premature end-of-file at file offset {}, expected chunk_len={}, actual={}", _pos, chunk_len, buf.size()));
            }
        }

        auto chunk = _codec.parse(std::string_view(buf.get(), buf.size()));
        auto actual_checksum = chunk.verify_checksum<crc32_utils>(_pos);

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                _digests.actual_digest = chunk.fold_into_digest<crc32_utils>(_digests.actual_digest, actual_checksum, true);
            }
        }

        _current_chunk_index++;
        _pos += buf.size();

        if constexpr (check_digest) {
            if (_digests.can_calculate_digest
                    && _pos == _compression_metadata->compressed_file_length()
                    && _digests.expected_digest != _digests.actual_digest) {
                sstables::throw_malformed_sstable_exception(seastar::format("Digest mismatch: expected={}, actual={}", _digests.expected_digest, _digests.actual_digest));
            }
        }

        co_return make_tracked_temporary_buffer(std::move(buf), std::move(*res_units));
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        throw std::runtime_error("compressed raw file data source does not support skip()");
    }
};

template <bool check_digest>
class compressed_raw_file_data_source : public data_source {
public:
    compressed_raw_file_data_source(sstables::stream_creator_fn stream_creator, sstables::compression* cm,
            sstables::sstable_version_types version, file_input_stream_options options, reader_permit permit, std::optional<uint32_t> digest)
        : data_source(std::make_unique<compressed_raw_file_data_source_impl<check_digest>>(
                std::move(stream_creator), cm, version, std::move(options), std::move(permit), digest))
        {}
};

template <ChecksumUtils ChecksumType, bool check_digest, compressed_checksum_mode mode>
class compressed_file_data_source : public data_source {
public:
    compressed_file_data_source(sstables::stream_creator_fn stream_creator, sstables::compression* cm,
            sstables::sstable_version_types version, sstables::disk_read_range range, file_input_stream_options options, reader_permit permit,
            std::optional<uint32_t> digest)
        : data_source(std::make_unique<compressed_file_data_source_impl<ChecksumType, check_digest, mode>>(
                std::move(stream_creator), cm, version, range, std::move(options), std::move(permit), digest))
        {}
};

template <ChecksumUtils ChecksumType, compressed_checksum_mode mode>
inline input_stream<char> make_compressed_file_input_stream(sstables::stream_creator_fn stream_creator, sstables::compression *cm,
        sstables::sstable_version_types version, sstables::disk_read_range range,
        file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest)
{
    if (digest) [[unlikely]] {
        return input_stream<char>(compressed_file_data_source<ChecksumType, true, mode>(
                std::move(stream_creator), cm, version, range, std::move(options), std::move(permit), digest));
    }
    return input_stream<char>(compressed_file_data_source<ChecksumType, false, mode>(
            std::move(stream_creator), cm, version, range, std::move(options), std::move(permit), digest));
}

// compressed_file_data_sink_impl works as a filter for a file output stream,
// where the buffer flushed will be compressed and its checksum computed, then
// the result passed to a regular output stream.
template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
class compressed_file_data_sink_impl : public data_sink_impl {
    output_stream<char> _out;
    sstables::compression* _compression_metadata;
    sstables::framed_chunk_codec _codec;
    sstables::compression::segmented_offsets::writer _offsets;
    size_t _pos = 0;
    uint32_t _full_checksum;
    // Called after each compressed chunk is written. See compressed_chunk_observer.
    sstables::compressed_chunk_observer _observer;
public:
    compressed_file_data_sink_impl(output_stream<char> out, sstables::compression* cm,
            sstables::sstable_version_types version, sstables::compressed_chunk_observer observer)
            : _out(std::move(out))
            , _compression_metadata(cm)
            , _codec(version, cm->uncompressed_chunk_length())
            , _offsets(_compression_metadata->offsets.get_writer())
            , _full_checksum(ChecksumType::init_checksum())
            , _observer(std::move(observer))
    {}

private:
    future<> do_put(temporary_buffer<char> buf) {
        // Leave room for the header, if any, before the compressed data, and
        // for the checksum and the footer, if any, after it.
        const size_t frame = _codec.frame_len();
        auto output_len = _compression_metadata->get_compressor().compress_max_size(buf.size());
        temporary_buffer<char> compressed(_codec.extent(output_len));

        // compress flushed data, placing it right after the header.
        auto len = _compression_metadata->get_compressor().compress(buf.get(), buf.size(), compressed.get_write() + frame, output_len);
        if (len > output_len) {
            return make_exception_future(std::runtime_error("possible overflow during compression"));
        }
        // The length fields and the index entries of a framed chunk can't
        // represent a longer chunk.
        if (_codec.framed() && len > sstables::compressed_chunk_length_limit(_codec.uncompressed_chunk_length())) {
            return make_exception_future(std::runtime_error(format("compressed chunk length {} exceeds the limit {} for an uncompressed chunk length of {}",
                    len, sstables::compressed_chunk_length_limit(_codec.uncompressed_chunk_length()), _codec.uncompressed_chunk_length())));
        }

        // position of this chunk in the pre-compression (uncompressed) stream,
        // i.e. the sum of the lengths of all prior buffers.
        auto pre_compression_pos = _compression_metadata->uncompressed_file_length();
        // total length of the uncompressed data.
        _compression_metadata->set_uncompressed_file_length(pre_compression_pos + buf.size());

        // position of this chunk in the post-compression stream.
        auto post_compression_pos = _pos;
        _offsets.push_back(_pos);
        auto chunk_size = _codec.extent(len);
        _pos += chunk_size;
        _compression_metadata->set_compressed_file_length(_pos);
        compressed.trim(chunk_size);

        // The footer lets a backward reader at the start of the next chunk find
        // the start of this chunk.
        if (_codec.framed()) {
            _codec.write_frames(compressed.get_write(), uint32_t(len));
        }

        // A corrupt footer is caught indirectly: a backward reader that uses it
        // finds a wrong chunk start, and the data there fails the checksum.
        uint32_t per_chunk_checksum = ChecksumType::checksum(compressed.get(), frame + len);
        write_be<uint32_t>(compressed.get_write() + frame + len, per_chunk_checksum);
        auto chunk = _codec.parse(std::string_view(compressed.get(), compressed.size()));
        _full_checksum = chunk.template fold_into_digest<ChecksumType>(_full_checksum, per_chunk_checksum,
                mode == compressed_checksum_mode::checksum_all);

        _compression_metadata->set_full_checksum(_full_checksum);

        if (_observer) {
            _observer(post_compression_pos, pre_compression_pos, chunk_size, buf.size());
        }

        auto f = _out.write(compressed.get(), compressed.size());
        return f.then([compressed = std::move(compressed)] {});
    }
public:
    virtual future<> put(std::span<temporary_buffer<char>> bufs) override {
        return data_sink_impl::fallback_put(bufs, [this] (temporary_buffer<char>&& buf) {
            return do_put(std::move(buf));
        });
    }

    virtual future<> close() override {
        return _out.close();
    }

    virtual size_t buffer_size() const noexcept override {
        return _compression_metadata->uncompressed_chunk_length();
    }
};

template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
class compressed_file_data_sink : public data_sink {
public:
    compressed_file_data_sink(output_stream<char> out, sstables::compression* cm,
            sstables::sstable_version_types version, sstables::compressed_chunk_observer observer)
        : data_sink(std::make_unique<compressed_file_data_sink_impl<ChecksumType, mode>>(
                std::move(out), cm, version, std::move(observer))) {}
};

template <typename ChecksumType, compressed_checksum_mode mode>
requires ChecksumUtils<ChecksumType>
inline output_stream<char> make_compressed_file_output_stream(output_stream<char> out,
         sstables::compression* cm,
         sstables::sstable_version_types version,
         const compression_parameters& cp,
         compressor_ptr p,
         sstables::compressed_chunk_observer observer) {
    cm->set_compressor(std::move(p));
    // buffer of output stream is set to chunk length, because flush must
    // happen every time a chunk was filled up.
    cm->set_uncompressed_chunk_length(cp.chunk_length());
    // FIXME: crc_check_chance can be configured by the user.
    // probability to verify the checksum of a compressed chunk we read.
    // defaults to 1.0.
    cm->options.elements.push_back({{"crc_check_chance"}, {"1.0"}});

    return output_stream<char>(compressed_file_data_sink<ChecksumType, mode>(std::move(out), cm, version, std::move(observer)));
}

input_stream<char> sstables::make_compressed_file_k_l_format_input_stream(stream_creator_fn stream_creator,
        sstables::compression* cm, sstable_version_types version, disk_read_range range,
        class file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest)
{
    return make_compressed_file_input_stream<adler32_utils, compressed_checksum_mode::checksum_chunks_only>(
            std::move(stream_creator), cm, version, range, std::move(options), std::move(permit), digest);
}

input_stream<char> sstables::make_compressed_file_m_format_input_stream(stream_creator_fn stream_creator,
        sstables::compression *cm, sstable_version_types version, disk_read_range range,
        class file_input_stream_options options, reader_permit permit,
        std::optional<uint32_t> digest) {
    return make_compressed_file_input_stream<crc32_utils, compressed_checksum_mode::checksum_all>(
            std::move(stream_creator), cm, version, range, std::move(options), std::move(permit), digest);
}

output_stream<char> sstables::make_compressed_file_m_format_output_stream(output_stream<char> out,
        sstables::compression* cm,
        sstable_version_types version,
        const compression_parameters& cp,
        compressor_ptr p,
        sstables::compressed_chunk_observer observer) {
    return make_compressed_file_output_stream<crc32_utils, compressed_checksum_mode::checksum_all>(
            std::move(out), cm, version, cp, std::move(p), std::move(observer));
}

input_stream<char> sstables::make_compressed_raw_file_input_stream(sstables::stream_creator_fn stream_creator, sstables::compression *cm,
        sstable_version_types version, file_input_stream_options options, reader_permit permit, std::optional<uint32_t> digest)
{
    if (digest) [[unlikely]] {
        return input_stream<char>(compressed_raw_file_data_source<true>(
                std::move(stream_creator), cm, version, std::move(options), std::move(permit), digest));
    }
    return input_stream<char>(compressed_raw_file_data_source<false>(
            std::move(stream_creator), cm, version, std::move(options), std::move(permit), digest));
}
