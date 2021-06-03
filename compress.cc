/*
 * Copyright (C) 2016-present ScyllaDB
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

#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>

#include "compress.hh"
#include "utils/class_registrator.hh"

const sstring compressor::namespace_prefix = "org.apache.cassandra.io.compress.";

class lz4_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
};

class snappy_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
};

class deflate_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
};

compressor::compressor(sstring name)
    : _name(std::move(name))
{}

std::set<sstring> compressor::option_names() const {
    return {};
}

std::map<sstring, sstring> compressor::options() const {
    return {};
}

shared_ptr<compressor> compressor::create(const sstring& name, const opt_getter& opts) {
    if (name.empty()) {
        return {};
    }

    qualified_name qn(namespace_prefix, name);

    for (auto& c : { lz4, snappy, deflate }) {
        if (c->name() == static_cast<const sstring&>(qn)) {
            return c;
        }
    }

    return compressor_registry::create(qn, opts);
}

shared_ptr<compressor> compressor::create(const std::map<sstring, sstring>& options) {
    auto i = options.find(compression_parameters::SSTABLE_COMPRESSION);
    if (i != options.end() && !i->second.empty()) {
        return create(i->second, [&options](const sstring& key) -> opt_string {
            auto i = options.find(key);
            if (i == options.end()) {
                return std::nullopt;
            }
            return { i->second };
        });
    }
    return {};
}

thread_local const shared_ptr<compressor> compressor::lz4 = ::make_shared<lz4_processor>(namespace_prefix + "LZ4Compressor");
thread_local const shared_ptr<compressor> compressor::snappy = ::make_shared<snappy_processor>(namespace_prefix + "SnappyCompressor");
thread_local const shared_ptr<compressor> compressor::deflate = ::make_shared<deflate_processor>(namespace_prefix + "DeflateCompressor");

const sstring compression_parameters::SSTABLE_COMPRESSION = "sstable_compression";
const sstring compression_parameters::CHUNK_LENGTH_KB = "chunk_length_in_kb";
const sstring compression_parameters::CHUNK_LENGTH_KB_ERR = "chunk_length_kb";
const sstring compression_parameters::CRC_CHECK_CHANCE = "crc_check_chance";

compression_parameters::compression_parameters()
    : compression_parameters(compressor::lz4)
{}

compression_parameters::~compression_parameters()
{}

compression_parameters::compression_parameters(compressor_ptr c)
    : _compressor(std::move(c))
{}

compression_parameters::compression_parameters(const std::map<sstring, sstring>& options) {
    _compressor = compressor::create(options);

    validate_options(options);

    auto chunk_length = options.find(CHUNK_LENGTH_KB) != options.end() ?
        options.find(CHUNK_LENGTH_KB) : options.find(CHUNK_LENGTH_KB_ERR);

    if (chunk_length != options.end()) {
        try {
            _chunk_length = std::stoi(chunk_length->second) * 1024;
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid integer value ") + chunk_length->second + " for " + chunk_length->first);
        }
    }
    auto crc_chance = options.find(CRC_CHECK_CHANCE);
    if (crc_chance != options.end()) {
        try {
            _crc_check_chance = std::stod(crc_chance->second);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid double value ") + crc_chance->second + "for " + CRC_CHECK_CHANCE);
        }
    }
}

void compression_parameters::validate() {
    if (_chunk_length) {
        auto chunk_length = _chunk_length.value();
        if (chunk_length <= 0) {
            throw exceptions::configuration_exception(
                fmt::sprintf("Invalid negative or null for %s/%s", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
        // _chunk_length must be a power of two
        if (chunk_length & (chunk_length - 1)) {
            throw exceptions::configuration_exception(
                fmt::sprintf("%s/%s must be a power of 2.", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
    }
    if (_crc_check_chance && (_crc_check_chance.value() < 0.0 || _crc_check_chance.value() > 1.0)) {
        throw exceptions::configuration_exception(sstring(CRC_CHECK_CHANCE) + " must be between 0.0 and 1.0.");
    }
}

std::map<sstring, sstring> compression_parameters::get_options() const {
    if (!_compressor) {
        return std::map<sstring, sstring>();
    }
    auto opts = _compressor->options();

    opts.emplace(compression_parameters::SSTABLE_COMPRESSION, _compressor->name());
    if (_chunk_length) {
        opts.emplace(sstring(CHUNK_LENGTH_KB), std::to_string(_chunk_length.value() / 1024));
    }
    if (_crc_check_chance) {
        opts.emplace(sstring(CRC_CHECK_CHANCE), std::to_string(_crc_check_chance.value()));
    }
    return opts;
}

bool compression_parameters::operator==(const compression_parameters& other) const {
    return _compressor == other._compressor
           && _chunk_length == other._chunk_length
           && _crc_check_chance == other._crc_check_chance;
}

bool compression_parameters::operator!=(const compression_parameters& other) const {
    return !(*this == other);
}

void compression_parameters::validate_options(const std::map<sstring, sstring>& options) {
    // currently, there are no options specific to a particular compressor
    static std::set<sstring> keywords({
        sstring(SSTABLE_COMPRESSION),
        sstring(CHUNK_LENGTH_KB),
        sstring(CHUNK_LENGTH_KB_ERR),
        sstring(CRC_CHECK_CHANCE),
    });
    std::set<sstring> ckw;
    if (_compressor) {
        ckw = _compressor->option_names();
    }
    for (auto&& opt : options) {
        if (!keywords.contains(opt.first) && !ckw.contains(opt.first)) {
            throw exceptions::configuration_exception(format("Unknown compression option '{}'.", opt.first));
        }
    }
}

size_t lz4_processor::uncompress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
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

size_t lz4_processor::compress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
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

size_t lz4_processor::compress_max_size(size_t input_len) const {
    return LZ4_COMPRESSBOUND(input_len) + 4;
}

size_t deflate_processor::uncompress(const char* input,
                size_t input_len, char* output, size_t output_len) const {
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

size_t deflate_processor::compress(const char* input,
                size_t input_len, char* output, size_t output_len) const {
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
    auto res = ::deflate(&zs, Z_FINISH);
    deflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate compression failure");
    }
}

size_t deflate_processor::compress_max_size(size_t input_len) const {
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

size_t snappy_processor::uncompress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    if (snappy_uncompress(input, input_len, output, &output_len)
            == SNAPPY_OK) {
        return output_len;
    } else {
        throw std::runtime_error("snappy uncompression failure");
    }
}

size_t snappy_processor::compress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    auto ret = snappy_compress(input, input_len, output, &output_len);
    if (ret != SNAPPY_OK) {
        throw std::runtime_error("snappy compression failure: snappy_compress() failed");
    }
    return output_len;
}

size_t snappy_processor::compress_max_size(size_t input_len) const {
    return snappy_max_compressed_length(input_len);
}

