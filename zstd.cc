/*
 * Copyright (C) 2019-present ScyllaDB
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

#include <seastar/core/aligned_buffer.hh>

// We need to use experimental features of the zstd library (to allocate compression/decompression context),
// which are available only when the library is linked statically.
#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h"

#include "compress.hh"
#include "utils/class_registrator.hh"

static const sstring COMPRESSION_LEVEL = "compression_level";
static const sstring COMPRESSOR_NAME = compressor::namespace_prefix + "ZstdCompressor";

class zstd_processor : public compressor {
    int _compression_level = 3;

    // Manages memory for the compression context.
    std::unique_ptr<char[], free_deleter> _cctx_raw;
    // Compression context. Observer of _cctx_raw.
    ZSTD_CCtx* _cctx;

    // Manages memory for the decompression context.
    std::unique_ptr<char[], free_deleter> _dctx_raw;
    // Decompression context. Observer of _dctx_raw.
    ZSTD_DCtx* _dctx;
public:
    zstd_processor(const opt_getter&);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    std::set<sstring> option_names() const override;
    std::map<sstring, sstring> options() const override;
};

zstd_processor::zstd_processor(const opt_getter& opts)
    : compressor(COMPRESSOR_NAME) {
    auto level = opts(COMPRESSION_LEVEL);
    if (level) {
        try {
            _compression_level = std::stoi(*level);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(
                format("Invalid integer value {} for {}", *level, COMPRESSION_LEVEL));
        }

        auto min_level = ZSTD_minCLevel();
        auto max_level = ZSTD_maxCLevel();
        if (min_level > _compression_level || _compression_level > max_level) {
            throw exceptions::configuration_exception(
                format("{} must be between {} and {}, got {}", COMPRESSION_LEVEL, min_level, max_level, _compression_level));
        }
    }

    auto chunk_len_kb = opts(compression_parameters::CHUNK_LENGTH_KB);
    if (!chunk_len_kb) {
        chunk_len_kb = opts(compression_parameters::CHUNK_LENGTH_KB_ERR);
    }
    auto chunk_len = chunk_len_kb
       // This parameter has already been validated.
       ? std::stoi(*chunk_len_kb) * 1024
       : compression_parameters::DEFAULT_CHUNK_LENGTH;

    // We assume that the uncompressed input length is always <= chunk_len.
    auto cparams = ZSTD_getCParams(_compression_level, chunk_len, 0);
    auto cctx_size = ZSTD_estimateCCtxSize_usingCParams(cparams);
    // According to the ZSTD documentation, pointer to the context buffer must be 8-bytes aligned.
    _cctx_raw = allocate_aligned_buffer<char>(cctx_size, 8);
    _cctx = ZSTD_initStaticCCtx(_cctx_raw.get(), cctx_size);
    if (!_cctx) {
        throw std::runtime_error("Unable to initialize ZSTD compression context");
    }

    auto dctx_size = ZSTD_estimateDCtxSize();
    _dctx_raw = allocate_aligned_buffer<char>(dctx_size, 8);
    _dctx = ZSTD_initStaticDCtx(_dctx_raw.get(), dctx_size);
    if (!_cctx) {
        throw std::runtime_error("Unable to initialize ZSTD decompression context");
    }
}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = ZSTD_decompressDCtx(_dctx, output, output_len, input, input_len);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD decompression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}


size_t zstd_processor::compress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = ZSTD_compressCCtx(_cctx, output, output_len, input, input_len, _compression_level);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD compression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

size_t zstd_processor::compress_max_size(size_t input_len) const {
    return ZSTD_compressBound(input_len);
}

std::set<sstring> zstd_processor::option_names() const {
    return {COMPRESSION_LEVEL};
}

std::map<sstring, sstring> zstd_processor::options() const {
    return {{COMPRESSION_LEVEL, std::to_string(_compression_level)}};
}

static const class_registrator<compressor_ptr, zstd_processor, const compressor::opt_getter&>
    registrator(COMPRESSOR_NAME);
