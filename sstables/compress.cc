/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdexcept>
#include <cstdlib>

#include <boost/range/algorithm/find_if.hpp>
#include <seastar/core/align.hh>
#include <seastar/core/bitops.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/fstream.hh>

#include "compress.hh"

#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>

#include "unimplemented.hh"
#include "stdx.hh"
#include "segmented_compress_params.hh"

namespace sstables {

static logging::logger sstlog("sstable");

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
 *
 * Little Endian (e.g. x86)
 *
 * |1|2|3|4| | | | | CPU integer
 *  -------
 *     |
 *     +-+ << shift = prefix bits
 *       |
 *
 * | |1|2|3|4| | | | raw storage (unaligned)
 *  = ------- =====
 *  |           |
 *  |           +-> suffix bits
 *  +-> prefix bits
 *
 *
 * Big Endian (e.g. PPC)
 *
 * | | | | |4|3|2|1| CPU integer
 *          -------
 *             |
 *       +-----+ << shift = suffix bits
 *       |
 *
 * | |4|3|2|1| | | | raw storage (unaligned)
 *  = ------- =====
 *  |           |
 *  |           +-> suffix bits
 *  +-> prefix bits
 *
 * |0|1|1|1|1|0|0|0| read/write mask
 */
struct bit_displacement {
    uint64_t shift;
    uint64_t mask;
};

inline uint64_t displacement_bits(uint64_t prefix_bits, uint8_t size_bits) {
// Works with gcc and clang
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return prefix_bits;
#elif defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    return 64 - prefix_bits - size_bits;
#else
#error "Unsupported platform or compiler, cannot detect endianness"
#endif
}

inline bit_displacement displacement_for(uint64_t prefix_bits, uint8_t size_bits, mask_type t) {
    const uint64_t d = displacement_bits(prefix_bits, size_bits);
    return {d, make_mask(size_bits, d, t)};
}

std::pair<bucket_info, segment_info> params_for_chunk_size(uint32_t chunk_size) {
    const uint8_t chunk_size_log2 = log2ceil(chunk_size);

    auto it = boost::find_if(bucket_infos, [&] (const bucket_info& bi) {
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
    auto s = *boost::find_if(segment_infos, [&] (const segment_info& si) {
        return si.data_size_log2 == b.best_data_size_log2 && si.chunk_size_log2 == b.chunk_size_log2;
    });

    return {std::move(b), std::move(s)};
}

uint64_t compression::segmented_offsets::read(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits) const {
    const uint64_t offset_byte = offset_bits / 8;

    uint64_t value{0};

    std::copy_n(_storage[bucket_index].storage.get() + offset_byte, sizeof(value), reinterpret_cast<char*>(&value));

    const auto displacement = displacement_for(offset_bits % 8, size_bits, mask_type::set);

    value &= displacement.mask;
    value >>= displacement.shift;

    return value;
}

void compression::segmented_offsets::write(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits, uint64_t value) {
    const uint64_t offset_byte = offset_bits / 8;

    uint64_t old_value{0};

    std::copy_n(_storage[bucket_index].storage.get() + offset_byte, sizeof(old_value), reinterpret_cast<char*>(&old_value));

    const auto displacement = displacement_for(offset_bits % 8, size_bits, mask_type::clear);

    value <<= displacement.shift;

    if ((~displacement.mask | value) != ~displacement.mask) {
        throw std::invalid_argument(sprint("{}: to-be-written value would overflow the allocated bits", __FUNCTION__));
    }

    old_value &= displacement.mask;
    value |= old_value;

    std::copy_n(reinterpret_cast<char*>(&value), sizeof(value), _storage[bucket_index].storage.get() + offset_byte);
}

void compression::segmented_offsets::update_position_trackers(std::size_t index) const {
    if (_current_index != index - 1) {
        _current_index = index;
        const uint64_t current_segment_index = _current_index / _grouped_offsets;
        _current_bucket_segment_index = current_segment_index % _segments_per_bucket;
        _current_segment_relative_index = _current_index % _grouped_offsets;
        _current_bucket_index = current_segment_index / _segments_per_bucket;
        _current_segment_offset_bits = (_current_bucket_segment_index % _segments_per_bucket) * _segment_size_bits;
    } else {
        ++_current_index;
        ++_current_segment_relative_index;

        // Crossed segment boundary.
        if (_current_segment_relative_index == _grouped_offsets) {
            ++_current_bucket_segment_index;
            _current_segment_relative_index = 0;

            // Crossed bucket boundary.
            if (_current_bucket_segment_index == _segments_per_bucket) {
                ++_current_bucket_index;
                _current_bucket_segment_index = 0;
                _current_segment_offset_bits = 0;
            } else {
                _current_segment_offset_bits += _segment_size_bits;
            }
        }
    }
}

void compression::segmented_offsets::init(uint32_t chunk_size) {
    assert(chunk_size != 0);

    _chunk_size = chunk_size;

    const auto params = params_for_chunk_size(chunk_size);

    sstlog.trace(
            "{} {}(): chunk size {} (log2)",
            this,
            __FUNCTION__,
            static_cast<int>(params.first.chunk_size_log2));

    _grouped_offsets = params.second.grouped_offsets;
    _segment_base_offset_size_bits = params.second.data_size_log2;
    _segmented_offset_size_bits = static_cast<uint64_t>(log2ceil((_chunk_size + 64) * (_grouped_offsets - 1)));
    _segment_size_bits = _segment_base_offset_size_bits + (_grouped_offsets - 1) * _segmented_offset_size_bits;
    _segments_per_bucket = params.first.segments_per_bucket;
}

uint64_t compression::segmented_offsets::at(std::size_t i) const {
    if (i >= _size) {
        throw std::out_of_range(sprint("{}: index {} is out of range", __FUNCTION__, i));
    }

    update_position_trackers(i);

    const uint64_t bucket_base_offset = _storage[_current_bucket_index].base_offset;
    const uint64_t segment_base_offset = bucket_base_offset + read(_current_bucket_index, _current_segment_offset_bits, _segment_base_offset_size_bits);

    if (_current_segment_relative_index == 0) {
        return  segment_base_offset;
    }

    return segment_base_offset
        + read(_current_bucket_index,
                _current_segment_offset_bits + _segment_base_offset_size_bits + (_current_segment_relative_index - 1) * _segmented_offset_size_bits,
                _segmented_offset_size_bits);
}

void compression::segmented_offsets::push_back(uint64_t offset) {
    update_position_trackers(_size);

    if (_current_bucket_index == _storage.size()) {
        _storage.push_back(bucket{_last_written_offset, std::unique_ptr<char[]>(new char[bucket_size])});
    }

    const uint64_t bucket_base_offset = _storage[_current_bucket_index].base_offset;

    if (_current_segment_relative_index == 0) {
        write(_current_bucket_index, _current_segment_offset_bits, _segment_base_offset_size_bits, offset - bucket_base_offset);
    } else {
        const uint64_t segment_base_offset = bucket_base_offset + read(_current_bucket_index, _current_segment_offset_bits, _segment_base_offset_size_bits);
        write(_current_bucket_index,
                _current_segment_offset_bits + _segment_base_offset_size_bits + (_current_segment_relative_index - 1) * _segmented_offset_size_bits,
                _segmented_offset_size_bits,
                offset - segment_base_offset);
    }
    _last_written_offset = offset;
    ++_size;
}

void compression::update(uint64_t compressed_file_length) {
    // FIXME: also process _compression.options (just for crc-check frequency)
     if (name.value == "LZ4Compressor") {
         _uncompress = uncompress_lz4;
     } else if (name.value == "SnappyCompressor") {
         _uncompress = uncompress_snappy;
     } else if (name.value == "DeflateCompressor") {
         _uncompress = uncompress_deflate;
     } else {
         throw std::runtime_error("unsupported compression type");
     }

     _compressed_file_length = compressed_file_length;
}

void compression::set_compressor(compressor c) {
     if (c == compressor::lz4) {
         _compress = compress_lz4;
         _compress_max_size = compress_max_size_lz4;
         name.value = "LZ4Compressor";
     } else if (c == compressor::snappy) {
         _compress = compress_snappy;
         _compress_max_size = compress_max_size_snappy;
         name.value = "SnappyCompressor";
     } else if (c == compressor::deflate) {
         _compress = compress_deflate;
         _compress_max_size = compress_max_size_deflate;
         name.value = "DeflateCompressor";
     } else {
         throw std::runtime_error("unsupported compressor type");
     }
}

// locate() takes a byte position in the uncompressed stream, and finds the
// the location of the compressed chunk on disk which contains it, and the
// offset in this chunk.
// locate() may only be used for offsets of actual bytes, and in particular
// the end-of-file position (one past the last byte) MUST not be used. If the
// caller wants to read from the end of file, it should simply read nothing.
compression::chunk_and_offset
compression::locate(uint64_t position) const {
    auto ucl = uncompressed_chunk_length();
    auto chunk_index = position / ucl;
    decltype(ucl) chunk_offset = position % ucl;
    auto chunk_start = offsets.at(chunk_index);
    auto chunk_end = (chunk_index + 1 == offsets.size())
            ? _compressed_file_length
            : offsets.at(chunk_index + 1);
    return { chunk_start, chunk_end - chunk_start, chunk_offset };
}

}

size_t uncompress_lz4(const char* input, size_t input_len,
        char* output, size_t output_len) {
    // We use LZ4_decompress_safe(). According to the documentation, the
    // function LZ4_decompress_fast() is slightly faster, but maliciously
    // crafted compressed data can cause it to overflow the output buffer.
    // Theoretically, our compressed data is created by us so is not malicious
    // (and accidental corruption is avoided by the compressed-data checksum),
    // but let's not take that chance for now, until we've actually measured
    // the performance benefit that LZ4_decompress_fast() would bring.

    // Cassandra's LZ4Compressor prepends to the chunk its uncompressed length
    // in 4 bytes little-endian (!) order. We don't need this information -
    // we already know the uncompressed data is at most the given chunk size
    // (and usually is exactly that, except in the last chunk). The advance
    // knowledge of the uncompressed size could be useful if we used
    // LZ4_decompress_fast(), but we prefer LZ4_decompress_safe() anyway...
    input += 4;
    input_len -= 4;

    auto ret = LZ4_decompress_safe(input, output, input_len, output_len);
    if (ret < 0) {
        throw std::runtime_error("LZ4 uncompression failure");
    }
    return ret;
}

size_t compress_lz4(const char* input, size_t input_len,
        char* output, size_t output_len) {
    if (output_len < LZ4_COMPRESSBOUND(input_len) + 4) {
        throw std::runtime_error("LZ4 compression failure: length of output is too small");
    }
    // Write input_len (32-bit data) to beginning of output in little-endian representation.
    output[0] = input_len & 0xFF;
    output[1] = (input_len >> 8) & 0xFF;
    output[2] = (input_len >> 16) & 0xFF;
    output[3] = (input_len >> 24) & 0xFF;
#ifdef HAVE_LZ4_COMPRESS_DEFAULT
    auto ret = LZ4_compress_default(input, output + 4, input_len, LZ4_compressBound(input_len));
#else
    auto ret = LZ4_compress(input, output + 4, input_len);
#endif
    if (ret == 0) {
        throw std::runtime_error("LZ4 compression failure: LZ4_compress() failed");
    }
    return ret + 4;
}

size_t uncompress_deflate(const char* input, size_t input_len,
        char* output, size_t output_len) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("deflate uncompression init failure");
    }
    // yuck, zlib is not const-correct, and also uses unsigned char while we use char :-(
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
    zs.avail_in = input_len;
    zs.next_out = reinterpret_cast<unsigned char*>(output);
    zs.avail_out = output_len;
    auto res = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate uncompression failure");
    }
}

size_t compress_deflate(const char* input, size_t input_len,
        char* output, size_t output_len) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
        throw std::runtime_error("deflate compression init failure");
    }
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
    zs.avail_in = input_len;
    zs.next_out = reinterpret_cast<unsigned char*>(output);
    zs.avail_out = output_len;
    auto res = deflate(&zs, Z_FINISH);
    deflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate compression failure");
    }
}

size_t uncompress_snappy(const char* input, size_t input_len,
        char* output, size_t output_len) {
    if (snappy_uncompress(input, input_len, output, &output_len)
            == SNAPPY_OK) {
        return output_len;
    } else {
        throw std::runtime_error("snappy uncompression failure");
    }
}

size_t compress_snappy(const char* input, size_t input_len,
        char* output, size_t output_len) {
    auto ret = snappy_compress(input, input_len, output, &output_len);
    if (ret != SNAPPY_OK) {
        throw std::runtime_error("snappy compression failure: snappy_compress() failed");
    }
    return output_len;
}

size_t compress_max_size_lz4(size_t input_len) {
    return LZ4_COMPRESSBOUND(input_len) + 4;
}

size_t compress_max_size_deflate(size_t input_len) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
        throw std::runtime_error("deflate compression init failure");
    }
    auto res = deflateBound(&zs, input_len);
    deflateEnd(&zs);
    return res;
}

size_t compress_max_size_snappy(size_t input_len) {
    return snappy_max_compressed_length(input_len);
}

class compressed_file_data_source_impl : public data_source_impl {
    stdx::optional<input_stream<char>> _input_stream;
    sstables::compression* _compression_metadata;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    compressed_file_data_source_impl(file f, sstables::compression* cm,
                uint64_t pos, size_t len, file_input_stream_options options)
            : _compression_metadata(cm)
    {
        _beg_pos = pos;
        if (pos > _compression_metadata->uncompressed_file_length()) {
            throw std::runtime_error("attempt to uncompress beyond end");
        }
        if (len == 0 || pos == _compression_metadata->uncompressed_file_length()) {
            // Nothing to read
            _end_pos = _pos = _beg_pos;
            return;
        }
        if (len <= _compression_metadata->uncompressed_file_length() - pos) {
            _end_pos = pos + len;
        } else {
            _end_pos = _compression_metadata->uncompressed_file_length();
        }
        // _beg_pos and _end_pos specify positions in the compressed stream.
        // We need to translate them into a range of uncompressed chunks,
        // and open a file_input_stream to read that range.
        auto start = _compression_metadata->locate(_beg_pos);
        auto end = _compression_metadata->locate(_end_pos - 1);
        _input_stream = make_file_input_stream(std::move(f),
                start.chunk_start,
                end.chunk_start + end.chunk_len - start.chunk_start,
                std::move(options));
        _underlying_pos = start.chunk_start;
        _pos = _beg_pos;
    }
    virtual future<temporary_buffer<char>> get() override {
        if (_pos >= _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        auto addr = _compression_metadata->locate(_pos);
        // Uncompress the next chunk. We need to skip part of the first
        // chunk, but then continue to read from beginning of chunks.
        if (_pos != _beg_pos && addr.offset != 0) {
            throw std::runtime_error("compressed reader out of sync");
        }
        return _input_stream->read_exactly(addr.chunk_len).
            then([this, addr](temporary_buffer<char> buf) {
                // The last 4 bytes of the chunk are the adler32 checksum
                // of the rest of the (compressed) chunk.
                auto compressed_len = addr.chunk_len - 4;
                // FIXME: Do not always calculate checksum - Cassandra has a
                // probability (defaulting to 1.0, but still...)
                auto checksum = read_be<uint32_t>(buf.get() + compressed_len);
                if (checksum != checksum_adler32(buf.get(), compressed_len)) {
                    throw std::runtime_error("compressed chunk failed checksum");
                }

                // We know that the uncompressed data will take exactly
                // chunk_length bytes (or less, if reading the last chunk).
                temporary_buffer<char> out(
                        _compression_metadata->uncompressed_chunk_length());
                // The compressed data is the whole chunk, minus the last 4
                // bytes (which contain the checksum verified above).
                auto len = _compression_metadata->uncompress(
                        buf.get(), compressed_len,
                        out.get_write(), out.size());
                out.trim(len);
                out.trim_front(addr.offset);
                _pos += out.size();
                _underlying_pos += addr.chunk_len;
                return out;
        });
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        _pos += n;
        assert(_pos <= _end_pos);
        if (_pos == _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        auto addr = _compression_metadata->locate(_pos);
        auto underlying_n = addr.chunk_start - _underlying_pos;
        _underlying_pos = addr.chunk_start;
        _beg_pos = _pos;
        return _input_stream->skip(underlying_n).then([] {
            return make_ready_future<temporary_buffer<char>>();
        });
    }
};

class compressed_file_data_source : public data_source {
public:
    compressed_file_data_source(file f, sstables::compression* cm,
            uint64_t offset, size_t len, file_input_stream_options options)
        : data_source(std::make_unique<compressed_file_data_source_impl>(
                std::move(f), cm, offset, len, std::move(options)))
        {}
};

input_stream<char> make_compressed_file_input_stream(
        file f, sstables::compression* cm, uint64_t offset, size_t len,
        file_input_stream_options options)
{
    return input_stream<char>(compressed_file_data_source(
            std::move(f), cm, offset, len, std::move(options)));
}
