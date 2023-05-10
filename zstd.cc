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
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"

static const sstring COMPRESSION_LEVEL = "compression_level";
static const sstring COMPRESSOR_NAME = compressor::namespace_prefix + "ZstdCompressor";
static const size_t DCTX_SIZE = ZSTD_estimateDCtxSize();

class zstd_processor : public compressor {
    int _compression_level;

    // Note: std::shared_ptr because seastar ones do not allow
    // custom deleter objects, and lw_shared does not allow adopting
    // opaque structures.
    std::shared_ptr<ZSTD_CCtx> _cctx;
    std::shared_ptr<ZSTD_DCtx> _dctx;

    static int read_compression_level(const opt_getter&);
public:
    zstd_processor(const opt_getter&);
    zstd_processor(const zstd_processor&, int);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    size_t uncompress(bytes_source& in, bytes_ostream&) const override;

    size_t compress(bytes_source& in, bytes_ostream&) const override;

    ptr_type replace(const opt_getter&) const override;

    std::set<sstring> option_names() const override;
    std::map<sstring, sstring> options() const override;
};

zstd_processor::zstd_processor(const opt_getter& opts)
    : compressor(COMPRESSOR_NAME)
    // Code here was very clever, trying to minimize allocated zctx sizes
    // based on (maybe) set chunk size from sstables. However, doing so
    // severely hampers our ability to do arbitrary sized "streaming" compression
    // using the same context. While we can use ZSTD_estimateCStreamSize_usingCParams
    // and set chunk size to 0 (allow anything), the end result is not much better
    // in footprint than just letting the compressor allocate as it prefers.
    // And I can't even get dstream inited to a proper size to work with
    // varying compressed streams as source.
    // So, in the interest of keeping it both simple and working, it seems
    // that ensuring we instead re-use compressor objects seem like a better
    // option to reduce footprint. (See compressor.cc)
    , _compression_level(read_compression_level(opts))
    , _cctx(ZSTD_createCCtx(), &ZSTD_freeCCtx)
    , _dctx(ZSTD_createDCtx(), &ZSTD_freeDCtx)
{

    if (!_cctx) {
        throw std::runtime_error("Unable to initialize ZSTD compression context");
    }
    if (!_dctx) {
        throw std::runtime_error("Unable to initialize ZSTD decompression context");
    }
}

zstd_processor::zstd_processor(const zstd_processor& p, int cl)
    : compressor(COMPRESSOR_NAME)
    , _compression_level(cl)
    , _cctx(p._cctx)
    , _dctx(p._dctx)
{}

/**
 * Implement this, as we do in fact have _one_ relevant parameter, 
 * which should be respected. If compression level differs, we simply
 * create another instance of us sharing the zcontexts.
*/
zstd_processor::ptr_type zstd_processor::replace(const opt_getter& opts) const {
    auto l = read_compression_level(opts);
    if (l != _compression_level) {
        return make_shared<zstd_processor>(*this, l);
    }
    return {};
}

int zstd_processor::read_compression_level(const opt_getter& opts) {
    int compression_level = 3;

    auto level = opts ? opts(COMPRESSION_LEVEL) : std::nullopt;
    if (level) {
        try {
            compression_level = std::stoi(*level);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(
                format("Invalid integer value {} for {}", *level, COMPRESSION_LEVEL));
        }

        auto min_level = ZSTD_minCLevel();
        auto max_level = ZSTD_maxCLevel();
        if (min_level > compression_level || compression_level > max_level) {
            throw exceptions::configuration_exception(
                format("{} must be between {} and {}, got {}", COMPRESSION_LEVEL, min_level, max_level, compression_level));
        }
    }
    return compression_level;
}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = ZSTD_decompressDCtx(_dctx.get(), output, output_len, input, input_len);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD decompression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

size_t zstd_processor::compress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = ZSTD_compressCCtx(_cctx.get(), output, output_len, input, input_len, _compression_level);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD compression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

template<typename Func, typename... Args>
static size_t z_checked(std::string_view what, Func&& f, Args ...args) {
    auto ret = f(args...);
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(fmt::format("ZSTD {} failure: {}", what, ZSTD_getErrorName(ret)));
    }
    return ret;
}

size_t zstd_processor::uncompress(bytes_source& in, bytes_ostream& os) const {
    z_checked("decompression", ZSTD_initDStream, _dctx.get());

    size_t off = os.size_bytes();
    ZSTD_outBuffer output = { nullptr, 0, 0 };

    while (!in.empty()) {
        auto frag = in.next();
        auto size = frag.size();

        ZSTD_inBuffer input = { frag.data(), size, 0 };

        while (input.pos < input.size) {
            if (output.pos == output.size) {
                auto need = ZSTD_DStreamOutSize();
                output.dst = os.write_place_holder(need);
                output.size = need;
                output.pos = 0;
            }

            z_checked("decompression", ZSTD_decompressStream, _dctx.get(), &output, &input);
        }
    }

    os.remove_suffix(output.size - output.pos);
    return os.size_bytes() - off;
}

size_t zstd_processor::compress(bytes_source& in, bytes_ostream& os) const {
    z_checked("compression", ZSTD_initCStream, _cctx.get(), _compression_level);

    size_t off = os.size_bytes();
    ZSTD_outBuffer output = { nullptr, 0, 0 };

    while (!in.empty()) {
        auto frag = in.next();
        auto size = frag.size();

        ZSTD_inBuffer input = { frag.data(), size, 0 };
        ZSTD_EndDirective mode = in.size() == size ? ZSTD_e_end : ZSTD_e_continue;

        for (;;) {
            if (output.pos == output.size) {
                auto need = std::min(ZSTD_CStreamOutSize(), ZSTD_compressBound(input.size - input.pos));
                output.dst = os.write_place_holder(need);
                output.size = need;
                output.pos = 0;
            }

            auto n = z_checked("compression", ZSTD_compressStream2, _cctx.get(), &output, &input, mode);

            // if last chunk, we are finished when compress returns 0
            if (mode == ZSTD_e_end && n == 0) {
                break;
            }
            // else we are done whenever input is consumed
            if (mode == ZSTD_e_continue && input.pos == input.size) {
                break;
            }
        }
    }

    os.remove_suffix(output.size - output.pos);
    return os.size_bytes() - off;
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
