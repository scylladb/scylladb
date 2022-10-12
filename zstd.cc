/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

    // We share buffers among instances of the compressor to limit the number
    // of large allocations and to prevent OOM in case there are many
    // compressors alive simultaneously.
    //
    // However, we don't share buffers bigger than a certain arbitrary
    // threshold. The threshold is expected to be bigger than any buffer
    // encountered with reasonable settings.
    //
    // This is a simple protection from huge shared leftover compression
    // buffers staying in memory indefinitely. Huge buffers are not expected to
    // arise during normal operation, but if a user messes up his table
    // compression settings by setting 64 MiB chunks and max compression level,
    // the compression context can grow up to 680 MiB. If we share this buffer,
    // it will live and take up the memory even after the user fixes the
    // settings.
    //
    // The max context size for the default compression level 3 is 1.3 MiB.
    // The max context size for max compression level 22 and chunks of size
    // 64 KiB is 1.8 MiB. So 2 MiB seems like a good threshold for "reasonable"
    // setups.
    constexpr static size_t max_shared_cctx_size = size_t(2)*1024*1024;

    static thread_local size_t _shared_cctx_size;
    static thread_local std::unique_ptr<char[], free_deleter> _shared_cctx;
    static thread_local std::unique_ptr<char[], free_deleter> _shared_dctx;

    // Used for compression iff this instance needs more memory than max_shared_cctx_size.
    // This should only happen if the user messes up his compression settings.
    // Otherwise, the shared cctx is used.
    std::unique_ptr<char[], free_deleter> _private_cctx;

    size_t _cctx_size;

    ZSTD_DCtx* get_dctx() const;
    ZSTD_CCtx* get_cctx();
public:
    zstd_processor(const opt_getter&);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) override;
    size_t compress_max_size(size_t input_len) const override;

    std::set<sstring> option_names() const override;
    std::map<sstring, sstring> options() const override;
};

thread_local size_t zstd_processor::_shared_cctx_size;
thread_local std::unique_ptr<char[], free_deleter> zstd_processor::_shared_cctx;
thread_local std::unique_ptr<char[], free_deleter> zstd_processor::_shared_dctx;

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
    _cctx_size = ZSTD_estimateCCtxSize_usingCParams(cparams);

    if (!_shared_dctx) [[unlikely]] {
        auto dctx_size = ZSTD_estimateDCtxSize();
        _shared_dctx = allocate_aligned_buffer<char>(dctx_size, 8);
        if (!ZSTD_initStaticDCtx(_shared_dctx.get(), dctx_size)) {
            // This should never happen.
            throw std::runtime_error("Unable to initialize ZSTD decompression context");
        }
    }
}

ZSTD_DCtx* zstd_processor::get_dctx() const {
    return reinterpret_cast<ZSTD_DCtx*>(_shared_dctx.get());
}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = ZSTD_decompressDCtx(get_dctx(), output, output_len, input, input_len);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(format("ZSTD decompression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

ZSTD_CCtx* zstd_processor::get_cctx() {
    // We allocate the compression buffer lazily in compress(),
    // rather than in the constructor, so that readers never allocate it.
    // With default compression settings, the compression buffer is shared,
    // so this doesn't matter. But in the event that misconfigured compression
    // caused huge chunks to appear in an SSTable, every reader of this SSTable
    // would allocate a huge context buffer for no reason, potentially
    // preventing reads from this SSTable due to OOM.
    void* cctx_buf;
    if (_cctx_size <= _shared_cctx_size) [[likely]] {
        cctx_buf = _shared_cctx.get();
    } else {
        auto new_buf = allocate_aligned_buffer<char>(_cctx_size, 8);
        if (!ZSTD_initStaticCCtx(new_buf.get(), _cctx_size)) {
            // This should never happen.
            throw std::runtime_error("Unable to initialize ZSTD compression context");
        }
        if (_cctx_size <= max_shared_cctx_size) {
            _shared_cctx_size = _cctx_size;
            _shared_cctx = std::move(new_buf);
            cctx_buf = _shared_cctx.get();
        } else {
            _private_cctx = std::move(new_buf);
            cctx_buf = _private_cctx.get();
        }
    }
    return static_cast<ZSTD_CCtx*>(cctx_buf);
}

size_t zstd_processor::compress(const char* input, size_t input_len, char* output, size_t output_len) {
    auto ret = ZSTD_compressCCtx(get_cctx(), output, output_len, input, input_len, _compression_level);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(format("ZSTD compression failure: {}", ZSTD_getErrorName(ret)));
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

static const class_registrator<compressor, zstd_processor, const compressor::opt_getter&>
    registrator(COMPRESSOR_NAME);
