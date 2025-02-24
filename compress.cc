/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "seastar/core/sharded.hh"
#include "sstables/exceptions.hh"
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>
#include <seastar/util/log.hh>
#include "utils/reusable_buffer.hh"
#include "utils/hashers.hh"

#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "sstables/sstable_compressor_factory.hh"
#include "sstables/exceptions.hh"

sstring compressor::make_name(std::string_view short_name) {
    return seastar::format("org.apache.cassandra.io.compress.{}", short_name);
}

// SHA256
using dict_id = std::array<std::byte, 32>;
class sstable_compressor_factory_impl;

class raw_dict : public enable_lw_shared_from_this<raw_dict> {
    mutable sstable_compressor_factory_impl* _owner;
    dict_id _id;
    std::vector<std::byte> _dict;
public:
    raw_dict(sstable_compressor_factory_impl& owner, dict_id key, std::span<const std::byte> dict)
        : _owner(&owner)
        , _id(key)
        , _dict(dict.begin(), dict.end())
    {}
    ~raw_dict();
    const std::span<const std::byte> raw() const {
        return _dict;
    }
    dict_id id() const {
        return _id;
    }
    auto disown() const {
        _owner = nullptr;
    }
};

class zstd_ddict : public enable_lw_shared_from_this<zstd_ddict> {
    mutable sstable_compressor_factory_impl* _owner;
    lw_shared_ptr<const raw_dict> _raw;
    std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> _dict;
public:
    zstd_ddict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw)
        : _owner(&owner)
        , _raw(raw)
        , _dict(ZSTD_createDDict_byReference(_raw->raw().data(), _raw->raw().size()), ZSTD_freeDDict)
    {
        if (!_dict) {
            throw std::bad_alloc();
        }
    }
    ~zstd_ddict();
    auto dict() const {
        return _dict.get();
    }
    auto raw() const {
        return _raw->raw();
    }
    auto disown() const {
        _owner = nullptr;
    }
};

class zstd_cdict : public enable_lw_shared_from_this<zstd_cdict> {
    mutable sstable_compressor_factory_impl* _owner;
    lw_shared_ptr<const raw_dict> _raw;
    int _level;
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> _dict;
public:
    zstd_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw, int level)
        : _owner(&owner)
        , _raw(raw)
        , _level(level)
        , _dict(ZSTD_createCDict_byReference(_raw->raw().data(), _raw->raw().size(), level), ZSTD_freeCDict)
    {
        if (!_dict) {
            throw std::bad_alloc();
        }
    }
    ~zstd_cdict();
    auto dict() const {
        return _dict.get();
    }
    auto raw() const {
        return _raw->raw();
    }
    auto disown() const {
        _owner = nullptr;
    }
};

class lz4_cdict : public enable_lw_shared_from_this<lz4_cdict> {
    mutable sstable_compressor_factory_impl* _owner;
    lw_shared_ptr<const raw_dict> _raw;
    std::unique_ptr<LZ4_stream_t, decltype(&LZ4_freeStream)> _dict;
public:
    lz4_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw)
        : _owner(&owner)
        , _raw(raw)
        , _dict(LZ4_createStream(), LZ4_freeStream)
    {
        if (!_dict) {
            throw std::bad_alloc();
        }
        LZ4_loadDictSlow(_dict.get(), reinterpret_cast<const char*>(_raw->raw().data()), _raw->raw().size());
    }
    ~lz4_cdict();
    auto dict() const {
        return _dict.get();
    }
    auto raw() const {
        return _raw->raw();
    }
    auto disown() const {
        _owner = nullptr;
    }
};

class lz4_processor: public compressor {
public:
    using cdict_ptr = foreign_ptr<lw_shared_ptr<const lz4_cdict>>;
    using ddict_ptr = foreign_ptr<lw_shared_ptr<const raw_dict>>;
private:
    cdict_ptr _cdict;
    ddict_ptr _ddict;
    static LZ4_stream_t* get_cctx() {
        static thread_local auto cctx = std::unique_ptr<LZ4_stream_t, decltype(&LZ4_freeStream)>{LZ4_createStream(), LZ4_freeStream};
        return cctx.get();
    }
public:
    lz4_processor(cdict_ptr = nullptr, ddict_ptr = nullptr);
    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    std::map<sstring, sstring> options() const override;
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

static const sstring COMPRESSION_LEVEL = "compression_level";

class zstd_processor : public compressor {
    int _compression_level = 3;
    size_t _cctx_size;
    using cdict_ptr = foreign_ptr<lw_shared_ptr<const zstd_cdict>>;
    using ddict_ptr = foreign_ptr<lw_shared_ptr<const zstd_ddict>>;
    cdict_ptr _cdict;
    ddict_ptr _ddict;

    static auto with_dctx(std::invocable<ZSTD_DCtx*> auto f) {
        static const size_t DCTX_SIZE = ZSTD_estimateDCtxSize();
        // The decompression context has a fixed size of ~128 KiB,
        // so we don't bother ever resizing it the way we do with
        // the compression context.
        static thread_local std::unique_ptr<char[]> buf = std::invoke([&] {
            auto ptr = std::unique_ptr<char[]>(new char[DCTX_SIZE]);
            auto dctx = ZSTD_initStaticDCtx(ptr.get(), DCTX_SIZE);
            if (!dctx) {
                // Barring a bug, this should never happen.
                throw std::runtime_error("Unable to initialize ZSTD decompression context");
            }
            return ptr;
        });
        return f(reinterpret_cast<ZSTD_DCtx*>(buf.get()));
    }

    static auto with_cctx(size_t cctx_size, std::invocable<ZSTD_CCtx*> auto f) {
        // See the comments to reusable_buffer for a rationale of using it for compression.
        static thread_local utils::reusable_buffer<lowres_clock> buf(std::chrono::seconds(600));
        static thread_local size_t last_seen_reallocs = buf.reallocs();
        auto guard = utils::reusable_buffer_guard(buf);
        // Note that the compression context isn't initialized with a particular
        // compression config, but only with a particular size. As long as
        // it is big enough, we can reuse a context initialized by an
        // unrelated instance of zstd_processor without reinitializing it.
        //
        // If the existing context isn't big enough, the reusable buffer will
        // be resized by the next line, and the following `if` will notice that
        // and reinitialize the context.
        auto view = guard.get_temporary_buffer(cctx_size);
        if (last_seen_reallocs != buf.reallocs()) {
            // Either the buffer just grew because we requested a buffer bigger
            // than its last capacity, or it was shrunk some time ago by a timer.
            // Either way, the resize destroyed the contents of the buffer and
            // we have to initialize the context anew.
            auto cctx = ZSTD_initStaticCCtx(view.data(), buf.size());
            if (!cctx) {
                // Barring a bug, this should never happen.
                throw std::runtime_error("Unable to initialize ZSTD compression context");
            }
            last_seen_reallocs = buf.reallocs();
        }
        return f(reinterpret_cast<ZSTD_CCtx*>(view.data()));
    }

public:
    zstd_processor(const compression_parameters&, cdict_ptr, ddict_ptr);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    std::map<sstring, sstring> options() const override;
};

static sstring zstd_compressor_name() {
    return sstring(compression_parameters::algorithm_names[int(compression_parameters::algorithm::zstd)]);
}

zstd_processor::zstd_processor(const compression_parameters& opts, cdict_ptr cdict, ddict_ptr ddict)
    : compressor(zstd_compressor_name()) {
    _cdict = std::move(cdict);
    _ddict = std::move(ddict);
    if (auto level = opts.zstd_compression_level()) {
        _compression_level = *level;
    }

    auto chunk_len = opts.chunk_length();

    // We assume that the uncompressed input length is always <= chunk_len.
    auto cparams = ZSTD_getCParams(_compression_level, chunk_len, 0);
    _cctx_size = ZSTD_estimateCCtxSize_usingCParams(cparams);

}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = with_dctx([&] (ZSTD_DCtx* dctx) {
        if (_cdict) {
            return ZSTD_decompress_usingDDict(dctx, output, output_len, input, input_len, _ddict->dict());
        } else {
            return ZSTD_decompressDCtx(dctx, output, output_len, input, input_len);
        }
    });
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD decompression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}


size_t zstd_processor::compress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = with_cctx(_cctx_size, [&] (ZSTD_CCtx* cctx) {
        if (_cdict) {
            return ZSTD_compress_usingCDict(cctx, output, output_len, input, input_len, _cdict->dict());
        } else {
            return ZSTD_compressCCtx(cctx, output, output_len, input, input_len, _compression_level);
        }
    });
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD compression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

size_t zstd_processor::compress_max_size(size_t input_len) const {
    return ZSTD_compressBound(input_len);
}

const std::string_view DICTIONARY_OPTION = ".dictionary.";

static std::map<sstring, sstring> dict_as_options(std::span<const std::byte> d) {
    std::map<sstring, sstring> result;
    const size_t max_part_size = std::numeric_limits<uint16_t>::max() - 1;
    while (!d.empty()) {
        auto this_part_size = std::min(max_part_size, d.size());
        auto part_name = fmt::format("{}{:08}", DICTIONARY_OPTION, result.size());
        auto part = d.subspan(0, this_part_size);
        auto part_as_string = std::string_view(reinterpret_cast<const char*>(part.data()), part.size());
        result.emplace(part_name, part_as_string);
        d = d.subspan(this_part_size);
    }
    return result;
}

static std::optional<std::vector<std::byte>> dict_from_options(const sstables::compression& c) {
    std::map<int, bytes_view> parts;
    for (const auto& [k, v] : c.options.elements) {
        auto k_str = sstring(k.value.begin(), k.value.end());
        if (k_str.starts_with(DICTIONARY_OPTION)) {
            try {
                auto i = std::stoi(k_str.substr(DICTIONARY_OPTION.size()));
                parts.emplace(i, v.value);
            } catch (const std::exception& e) {
                throw sstables::malformed_sstable_exception(fmt::format("Corrupted dictionary option: {}", k_str));
            }
        }
        auto v_str = sstring(v.value.begin(), v.value.end());
    }
    std::vector<std::byte> result;
    int i = 0;
    for (const auto& [k, v] : parts) {
        if (k != i) {
            throw sstables::malformed_sstable_exception(fmt::format("Missing dictionary part: expected {}, got {}", i, k));
        }
        ++i;
        auto s = std::as_bytes(std::span(v));
        result.insert(result.end(), s.begin(), s.end());
    }
    return result;
}

std::map<sstring, sstring> zstd_processor::options() const {
    std::map<sstring, sstring> result = {{COMPRESSION_LEVEL, std::to_string(_compression_level)}};
    std::optional<std::span<const std::byte>> dict_blob;
    if (_cdict) {
        dict_blob = _cdict->raw();
    } else if (_ddict) {
        dict_blob = _ddict->raw();
    }
    if (dict_blob) {
        result.merge(dict_as_options(*dict_blob));
    }
    return result;
}

compressor::compressor(sstring name)
    : _name(std::move(name))
{}

std::map<sstring, sstring> compressor::options() const {
    return {};
}

thread_local const shared_ptr<compressor> compressor::lz4 = ::make_shared<lz4_processor>();
thread_local const shared_ptr<compressor> compressor::snappy = ::make_shared<snappy_processor>(make_name("SnappyCompressor"));
thread_local const shared_ptr<compressor> compressor::deflate = ::make_shared<deflate_processor>(make_name("DeflateCompressor"));

const sstring compression_parameters::SSTABLE_COMPRESSION = "sstable_compression";
const sstring compression_parameters::CHUNK_LENGTH_KB = "chunk_length_in_kb";
const sstring compression_parameters::CHUNK_LENGTH_KB_ERR = "chunk_length_kb";
const sstring compression_parameters::CRC_CHECK_CHANCE = "crc_check_chance";

compression_parameters::compression_parameters()
    : compression_parameters(algorithm::lz4)
{}

compression_parameters::~compression_parameters()
{}

compression_parameters::compression_parameters(algorithm alg)
    : compression_parameters::compression_parameters(
        alg == algorithm::none
        ? std::map<sstring, sstring>{}
        : std::map<sstring, sstring>{{sstring(SSTABLE_COMPRESSION), sstring(algorithm_to_name(alg))}}
    )
{}

auto compression_parameters::name_to_algorithm(std::string_view name) -> algorithm {
    if (name.empty()) {
        return algorithm::none;
    }
    auto qn = sstring(qualified_name(compressor::make_name(""), name));
    auto it = std::ranges::find(algorithm_names, qn);
    if (it == std::end(algorithm_names)) {
        throw std::runtime_error(std::format("Unknown sstable compression algorithm: {}", name));
    }
    return algorithm(it - std::begin(algorithm_names));
}

std::string_view compression_parameters::algorithm_to_name(algorithm alg) {
    return algorithm_names.at(static_cast<int>(alg));
}

compression_parameters::compression_parameters(const std::map<sstring, sstring>& options) {
    std::set<sstring> used_options;
    auto get_option = [&options, &used_options] (const sstring& x) -> const sstring* {
        used_options.insert(x);
        if (auto it = options.find(x); it != options.end()) {
            return &it->second;
        }
        return nullptr;
    };

    if (auto v = get_option(SSTABLE_COMPRESSION)) {
        _algorithm = name_to_algorithm(*v);
    } else {
        _algorithm = algorithm::none;
    }

    const sstring* chunk_length = nullptr;
    if (auto v = get_option(CHUNK_LENGTH_KB_ERR)) {
        chunk_length = v;
    }
    if (auto v = get_option(CHUNK_LENGTH_KB)) {
        chunk_length = v;
    }
    if (chunk_length) {
        try {
            _chunk_length = std::stoi(*chunk_length) * 1024;
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid integer value ") + *chunk_length + " for " + CHUNK_LENGTH_KB);
        }
    }

    if (auto v = get_option(CRC_CHECK_CHANCE)) {
        try {
            _crc_check_chance = std::stod(*v);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid double value ") + *v + "for " + CRC_CHECK_CHANCE);
        }
    }

    switch (_algorithm) {
    case algorithm::zstd:
        if (auto v = get_option(COMPRESSION_LEVEL)) {
            try {
                _zstd_compression_level = std::stoi(*v);
            } catch (const std::exception&) {
                throw exceptions::configuration_exception(format("Invalid integer value {} for {}", *v, COMPRESSION_LEVEL));
            }
        }
        break;
    default:
    }

    for (const auto& o : options) {
        if (!used_options.contains(o.first)) {
            throw exceptions::configuration_exception(format("Unknown compression option '{}'.", o.first));
        }
    }
}

void compression_parameters::validate() {
    if (_chunk_length) {
        auto chunk_length = _chunk_length.value();
        if (chunk_length <= 0) {
            throw exceptions::configuration_exception(
                fmt::format("Invalid negative or null for {}/{}", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
        // _chunk_length must be a power of two
        if (chunk_length & (chunk_length - 1)) {
            throw exceptions::configuration_exception(
                fmt::format("{}/{} must be a power of 2.", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
        // Excessive _chunk_length is pointless and can lead to allocation
        // failures (see issue #9933)
        if (chunk_length > 128 * 1024) {
            throw exceptions::configuration_exception(
                fmt::format("{}/{} must be 128 or less.", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
    }
    if (_crc_check_chance && (_crc_check_chance.value() < 0.0 || _crc_check_chance.value() > 1.0)) {
        throw exceptions::configuration_exception(sstring(CRC_CHECK_CHANCE) + " must be between 0.0 and 1.0.");
    }
    if (_zstd_compression_level) {
        if (*_zstd_compression_level != std::clamp<int>(*_zstd_compression_level, ZSTD_minCLevel(), ZSTD_maxCLevel())) {
            throw exceptions::configuration_exception(fmt::format("{} must be between {} and {}, got {}", ZSTD_minCLevel(), ZSTD_maxCLevel(), COMPRESSION_LEVEL, *_zstd_compression_level));
        }
    }
}

std::map<sstring, sstring> compression_parameters::get_options() const {
    auto opts = std::map<sstring, sstring>();
    if (_algorithm != algorithm::none) {
        opts.emplace(compression_parameters::SSTABLE_COMPRESSION, algorithm_to_name(_algorithm));
    }
    if (_zstd_compression_level) {
        opts.emplace(COMPRESSION_LEVEL, std::to_string(_zstd_compression_level.value()));
    }
    if (_chunk_length) {
        opts.emplace(sstring(CHUNK_LENGTH_KB), std::to_string(_chunk_length.value() / 1024));
    }
    if (_crc_check_chance) {
        opts.emplace(sstring(CRC_CHECK_CHANCE), std::to_string(_crc_check_chance.value()));
    }
    return opts;
}

lz4_processor::lz4_processor(cdict_ptr cdict, ddict_ptr ddict)
    : compressor(sstring(compression_parameters::algorithm_to_name(compression_parameters::algorithm::lz4)))
    , _cdict(std::move(cdict))
    , _ddict(std::move(ddict))
{}

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

    int ret;
    if (_cdict) {
        ret = LZ4_decompress_safe_usingDict(input, output, input_len, output_len, reinterpret_cast<const char*>(_ddict->raw().data()), _ddict->raw().size());
    } else {
        ret = LZ4_decompress_safe(input, output, input_len, output_len);
    }
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
    int ret;
    if (_cdict) {
        auto* ctx = get_cctx();
        LZ4_attach_dictionary(ctx, _cdict->dict());
        ret = LZ4_compress_fast_continue(ctx, input, output + 4, input_len, LZ4_compressBound(input_len), 1);
        if (ret == 0) {
            LZ4_initStream(ctx, sizeof(*ctx));
        } else {
            LZ4_resetStream_fast(ctx);
        }
    } else {
        ret = LZ4_compress_default(input, output + 4, input_len, LZ4_compressBound(input_len));
    }
    if (ret == 0) {
        throw std::runtime_error("LZ4 compression failure: LZ4_compress() failed");
    }
    return ret + 4;
}

size_t lz4_processor::compress_max_size(size_t input_len) const {
    return LZ4_COMPRESSBOUND(input_len) + 4;
}

std::map<sstring, sstring> lz4_processor::options() const {
    std::optional<std::span<const std::byte>> dict_blob;
    if (_cdict) {
        dict_blob = _cdict->raw();
    } else if (_ddict) {
        dict_blob = _ddict->raw();
    }
    if (dict_blob) {
        return dict_as_options(*dict_blob);
    } else {
        return {};
    }
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

class sstable_compressor_factory_impl : public sstable_compressor_factory {
    shard_id _owner_shard;
    const gms::feature_service& _feature_service;
    struct zstd_cdict_id {
        dict_id id;
        int level;
        std::strong_ordering operator<=>(const zstd_cdict_id&) const = default;
    };
    std::map<dict_id, const raw_dict*> _raw_dicts;
    std::map<zstd_cdict_id, const zstd_cdict*> _zstd_cdicts;
    std::map<dict_id, const zstd_ddict*> _zstd_ddicts;
    std::map<dict_id, const lz4_cdict*> _lz4_cdicts;
    std::map<table_id, lw_shared_ptr<const raw_dict>> _recommended;

    lw_shared_ptr<const raw_dict> get_canonical_ptr(std::span<const std::byte> dict) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        auto id = get_sha256(dict);
        if (auto it = _raw_dicts.find(id); it != _raw_dicts.end()) {
            return it->second->shared_from_this();
        } else {
            auto p = make_lw_shared<const raw_dict>(*this, id, dict);
            _raw_dicts.emplace(id, p.get());
            return p;
        }
    }
    using zstd_dicts = std::pair<
        foreign_ptr<lw_shared_ptr<const zstd_ddict>>,
        foreign_ptr<lw_shared_ptr<const zstd_cdict>>
    >;
    zstd_dicts get_zstd_dicts(lw_shared_ptr<const raw_dict> raw, int level) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        lw_shared_ptr<const zstd_ddict> ddict;
        lw_shared_ptr<const zstd_cdict> cdict;
        if (auto it = _zstd_ddicts.find(raw->id()); it != _zstd_ddicts.end()) {
            ddict = it->second->shared_from_this();
        } else {
            ddict = make_lw_shared<zstd_ddict>(*this, raw);
            _zstd_ddicts.emplace(raw->id(), ddict.get());
        }
        if (auto it = _zstd_cdicts.find({raw->id(), level}); it != _zstd_cdicts.end()) {
            cdict = it->second->shared_from_this();
        } else {
            cdict = make_lw_shared<zstd_cdict>(*this, raw, level);
            _zstd_cdicts.emplace(zstd_cdict_id{raw->id(), level}, cdict.get());
        }
        return {make_foreign(std::move(ddict)), make_foreign(std::move(cdict))};
    }
    future<zstd_dicts> get_zstd_dicts(std::span<const std::byte> dict, int level) {
        return smp::submit_to(_owner_shard, [this, dict, level] -> zstd_dicts {
            auto raw = get_canonical_ptr(dict);
            return get_zstd_dicts(raw, level);
        });
    }
    future<zstd_dicts> get_zstd_dicts(table_id t, int level) {
        return smp::submit_to(_owner_shard, [this, t, level] -> zstd_dicts {
            if (!_feature_service.sstable_compression_dicts) {
                return {};
            }
            auto rec_it = _recommended.find(t);
            if (rec_it != _recommended.end()) {
                return get_zstd_dicts(rec_it->second, level);
            } else {
                return {};
            }
        });
    }
    using lz4_dicts = std::pair<
        foreign_ptr<lw_shared_ptr<const raw_dict>>,
        foreign_ptr<lw_shared_ptr<const lz4_cdict>>
    >;
    lz4_dicts get_lz4_dicts(lw_shared_ptr<const raw_dict> raw) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        lw_shared_ptr<const lz4_cdict> cdict;
        if (auto it = _lz4_cdicts.find(raw->id()); it != _lz4_cdicts.end()) {
            cdict = it->second->shared_from_this();
        } else {
            cdict = make_lw_shared<lz4_cdict>(*this, raw);
            _lz4_cdicts.emplace(raw->id(), cdict.get());
        }
        return {make_foreign(std::move(raw)), make_foreign(std::move(cdict))};
    }
    future<lz4_dicts> get_lz4_dicts(std::span<const std::byte> dict) {
        return smp::submit_to(_owner_shard, [this, dict] -> lz4_dicts {
            auto raw = get_canonical_ptr(dict);
            return get_lz4_dicts(raw);
        });
    }
    future<lz4_dicts> get_lz4_dicts(table_id t) {
        return smp::submit_to(_owner_shard, [this, t] -> lz4_dicts {
            if (!_feature_service.sstable_compression_dicts) {
                return {};
            }
            auto rec_it = _recommended.find(t);
            if (rec_it != _recommended.end()) {
                return get_lz4_dicts(rec_it->second);
            } else {
                return {};
            }
        });
    }

public:
    sstable_compressor_factory_impl(const gms::feature_service& fs)
        : _owner_shard(this_shard_id())
        , _feature_service(fs)
    {
    }
    sstable_compressor_factory_impl(sstable_compressor_factory_impl&&) = delete;
    ~sstable_compressor_factory_impl() {
        // The factory is only needed for creating compressors, but the compressors are allowed to outlive it.
        // Therefore we need to detach all compressors from the factory.
        for (auto& [k, v] : _raw_dicts) {
            v->disown();
        }
        _raw_dicts.clear();
        for (auto& [k, v] : _zstd_cdicts) {
            v->disown();
        }
        _zstd_cdicts.clear();
        for (auto& [k, v] : _zstd_ddicts) {
            v->disown();
        }
        _zstd_ddicts.clear();
        for (auto& [k, v] : _lz4_cdicts) {
            v->disown();
        }
        _lz4_cdicts.clear();
    }
    void forget_raw_dict(dict_id id) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        _raw_dicts.erase(id);
    }
    void forget_zstd_cdict(dict_id id, int level) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        _zstd_cdicts.erase({id, level});
    }
    void forget_zstd_ddict(dict_id id) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        _zstd_ddicts.erase(id);
    }
    void forget_lz4_cdict(dict_id id) {
        SCYLLA_ASSERT(this_shard_id() == _owner_shard);
        _lz4_cdicts.erase(id);
    }
    future<> set_recommended_dict(table_id t, std::span<const std::byte> dict) override {
        return smp::submit_to(_owner_shard, [this, t, dict] {
            if (dict.size()) {
                auto canonical_ptr = get_canonical_ptr(dict);
                _recommended.emplace(t, canonical_ptr);
            } else {
                if (auto it = _recommended.find(t); it != _recommended.end()) {
                    it->second->disown();
                    _recommended.erase(it);
                }
            }
        });
    }
    future<compressor_ptr> make_compressor_for_writing(schema_ptr) override;
    future<compressor_ptr> make_compressor_for_reading(sstables::compression&) override;
};


future<compressor_ptr> sstable_compressor_factory_impl::make_compressor_for_writing(schema_ptr s) {
    auto params = s->get_compressor_params();
    using algorithm = compression_parameters::algorithm;
    switch (params.get_algorithm()) {
    case algorithm::lz4: {
        auto [ddict, cdict] = co_await get_lz4_dicts(s->id());
        co_return seastar::make_shared<lz4_processor>(std::move(cdict), std::move(ddict));
    }
    case algorithm::deflate:
        co_return compressor::deflate;
    case algorithm::snappy:
        co_return compressor::snappy;
    case algorithm::zstd: {
        auto [ddict, cdict] = co_await get_zstd_dicts(s->id(), params.zstd_compression_level().value_or(ZSTD_defaultCLevel()));
        co_return seastar::make_shared<zstd_processor>(params, std::move(cdict), std::move(ddict));
    }
    case algorithm::none:
        co_return nullptr;
    }
    abort();
}

future<compressor_ptr> sstable_compressor_factory_impl::make_compressor_for_reading(sstables::compression& c) {
    auto params = compression_parameters(sstables::options_from_compression(c));
    auto dict = dict_from_options(c);
    using algorithm = compression_parameters::algorithm;
    std::unique_ptr<compressor> p;
    switch (params.get_algorithm()) {
    case algorithm::lz4: {
        if (dict) {
            auto [ddict, cdict] = co_await get_lz4_dicts(std::as_bytes(std::span(*dict)));
            co_return seastar::make_shared<lz4_processor>(std::move(cdict), std::move(ddict));
        } else {
            co_return seastar::make_shared<lz4_processor>(nullptr, nullptr);
        }
    }
    case algorithm::deflate:
        co_return compressor::deflate;
    case algorithm::snappy:
        co_return compressor::snappy;
    case algorithm::zstd: {
        if (dict) {
            auto level = params.zstd_compression_level().value_or(ZSTD_defaultCLevel());
            auto [ddict, cdict] = co_await get_zstd_dicts(std::as_bytes(std::span(*dict)), level);
            co_return seastar::make_shared<zstd_processor>(params, std::move(cdict), std::move(ddict));
        } else {
            co_return seastar::make_shared<zstd_processor>(params, nullptr, nullptr);
        }
        break;
    }
    case algorithm::none:
        co_return nullptr;
    }
}

raw_dict::~raw_dict() {
    if (_owner) {
        _owner->forget_raw_dict(_id);
    }
}

zstd_cdict::~zstd_cdict() {
    if (_owner) {
        _owner->forget_zstd_cdict(_raw->id(), _level);
    }
}

zstd_ddict::~zstd_ddict() {
    if (_owner) {
        _owner->forget_zstd_ddict(_raw->id());
    }
}

lz4_cdict::~lz4_cdict() {
    if (_owner) {
        _owner->forget_lz4_cdict(_raw->id());
    }
}

std::unique_ptr<sstable_compressor_factory> make_sstable_compressor_factory(const gms::feature_service& fs) {
    return std::make_unique<sstable_compressor_factory_impl>(fs);
}
