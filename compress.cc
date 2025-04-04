/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>
#include <seastar/util/log.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include "utils/reusable_buffer.hh"
#include "sstables/compress.hh"
#include "sstables/exceptions.hh"
#include "utils/hashers.hh"
#include "sstables/sstable_compressor_factory.hh"
#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "gms/feature_service.hh"

// SHA256
using dict_id = std::array<std::byte, 32>;
class sstable_compressor_factory_impl;

static seastar::logger compressor_factory_logger("sstable_compressor_factory");

template <> struct fmt::formatter<compression_parameters::algorithm> : fmt::formatter<string_view> {
    auto format(const compression_parameters::algorithm& alg, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", compression_parameters::algorithm_to_name(alg));
    }
};

// Holds a raw dictionary blob (without algorithm-specific hash tables).
// raw dicts might be used (and kept alive) directly by compressors (in particular, lz4 decompressor)
// or referenced by algorithm-specific dicts.
class raw_dict : public enable_lw_shared_from_this<raw_dict> {
    weak_ptr<sstable_compressor_factory_impl> _owner;
    dict_id _id;
    std::vector<std::byte> _dict;
public:
    raw_dict(sstable_compressor_factory_impl& owner, dict_id key, std::span<const std::byte> dict);
    ~raw_dict();
    const std::span<const std::byte> raw() const { return _dict; }
    dict_id id() const { return _id; }
};

// A custom allocator for zstd, so that we can track its memory usage.
struct zstd_callback_allocator {
    using callback_type = std::function<void(ssize_t)>;
    callback_type _callback;
    using self = zstd_callback_allocator;
    zstd_callback_allocator(callback_type cb) : _callback(std::move(cb)) {}
    zstd_callback_allocator(self&&) = delete;
    ZSTD_customMem as_zstd_custommem() & {
        return ZSTD_customMem{
            .customAlloc = [] (void* opaque, size_t n) -> void* {
                auto addr = malloc(n);
                static_cast<self*>(opaque)->_callback(static_cast<ssize_t>(malloc_usable_size(addr)));
                return addr;
            },
            .customFree = [] (void* opaque, void* addr) {
                static_cast<self*>(opaque)->_callback(-malloc_usable_size(addr));
                free(addr);
                return;
            },
            .opaque = static_cast<void*>(this),
        };
    }
};

// Holds a zstd-specific decompression dictionary
// (which internally holds a pointer to the raw dictionary blob
// and parsed entropy tables).
class zstd_ddict : public enable_lw_shared_from_this<zstd_ddict> {
    weak_ptr<sstable_compressor_factory_impl> _owner;
    lw_shared_ptr<const raw_dict> _raw;
    size_t _used_memory = 0;
    zstd_callback_allocator _alloc;
    std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> _dict;
public:
    zstd_ddict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw);
    ~zstd_ddict();
    auto dict() const { return _dict.get(); }
    auto raw() const { return _raw->raw(); }
    dict_id id() const { return _raw->id(); }
};

// Holds a zstd-specific decompression dictionary
// (which internally holds a pointer to the raw dictionary blob,
// indices over the blob, and entropy tables).
//
// Note that the index stored inside this dict is level-specific,
// so the level of compression is decided at the time of construction
// of this dict.
class zstd_cdict : public enable_lw_shared_from_this<zstd_cdict> {
    weak_ptr<sstable_compressor_factory_impl> _owner;
    lw_shared_ptr<const raw_dict> _raw;
    int _level;
    size_t _used_memory = 0;
    zstd_callback_allocator _alloc;
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> _dict;
public:
    zstd_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw, int level);
    ~zstd_cdict();
    auto dict() const { return _dict.get(); }
    auto raw() const { return _raw->raw(); }
    dict_id id() const {return _raw->id(); }
};

// Holds a lz4-specific compression dictionary
// (which internally holds a pointer to the raw dictionary blob,
// and a hash index over the substrings of the blob).
//
class lz4_cdict : public enable_lw_shared_from_this<lz4_cdict> {
    weak_ptr<sstable_compressor_factory_impl> _owner;
    lw_shared_ptr<const raw_dict> _raw;
    std::unique_ptr<LZ4_stream_t, decltype(&LZ4_freeStream)> _dict;
public:
    lz4_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw);
    ~lz4_cdict();
    auto dict() const { return _dict.get(); }
    auto raw() const { return _raw->raw(); }
    auto id() const { return _raw->id(); }
};

// A lz4 compressor for SSTables.
//
// Compression and decompression dicts can be passed to it via the constructor,
// and they will be used for compression and decompression respectively.
//
// If only the decompression dict is passed, calling `compress()` is illegal.
// If only the compression dict is passed, calling `decompress()` is illegal.
// If both dicts or none are passed, both `compress()` and `decompress()` are legal.
//
// (The reason we want to allow passing only one dict is that we want to discard
// compression dicts after the SSTable is written. They are much bigger then decompression
// dicts, and they won't be useful anymore, so it makes sense to free them.)
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
    // Legal if `_ddict || !_cdict`.
    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    // Legal if `_cdict || !_ddict`.
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    std::map<sstring, sstring> options() const override;
    algorithm get_algorithm() const override;
};

class snappy_processor: public compressor {
public:
    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    algorithm get_algorithm() const override { return algorithm::snappy; }
};

class deflate_processor: public compressor {
public:
    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    algorithm get_algorithm() const override { return algorithm::deflate; }
};

static const sstring COMPRESSION_LEVEL = "compression_level";

// A Zstd compressor for SSTables.
//
// Compression and decompression dicts can be passed to it via the constructor,
// and they will be used for compression and decompression respectively.
//
// If only the decompression dict is passed, calling `compress()` is illegal.
// If only the compression dict is passed, calling `decompress()` is illegal.
// If both dicts or none are passed, both `compress()` and `decompress()` are legal.
//
// (The reason we want to allow passing only one dict is that we want to discard
// compression dicts after the SSTable is written. They are much bigger then decompression
// dicts, and they won't be useful anymore, so it makes sense to free them.)
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

    // Legal if `(_ddict || !_cdict)`.
    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    // Legal if `(_cdict || !_ddict)`.
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    algorithm get_algorithm() const override;
    std::map<sstring, sstring> options() const override;
};

zstd_processor::zstd_processor(const compression_parameters& opts, cdict_ptr cdict, ddict_ptr ddict) {
    _cdict = std::move(cdict);
    _ddict = std::move(ddict);
    if (auto level = opts.zstd_compression_level()) {
        _compression_level = *level;
    }

    // The memory needed by the compression context depends both on the input
    // size and on the dictionary size.
    size_t dict_len = _cdict ? _cdict->raw().size() : 0;
    // We assume that the uncompressed input length is always <= chunk_len.
    auto chunk_len = opts.chunk_length();
    auto cparams = ZSTD_getCParams(_compression_level, chunk_len, dict_len);
    _cctx_size = ZSTD_estimateCCtxSize_usingCParams(cparams);

}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = with_dctx([&] (ZSTD_DCtx* dctx) {
        if (_ddict) {
            return ZSTD_decompress_usingDDict(dctx, output, output_len, input, input_len, _ddict->dict());
        } else {
            SCYLLA_ASSERT(!_cdict && "Write-only compressor used for reading");
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
            SCYLLA_ASSERT(!_ddict && "Read-only compressor used for writing");
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

auto zstd_processor::get_algorithm() const -> algorithm {
    return (_cdict || _ddict) ? algorithm::zstd_with_dicts : algorithm::zstd;
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

std::map<sstring, sstring> compressor::options() const {
    return {};
}

std::string compressor::name() const {
    return compression_parameters::algorithm_to_qualified_name(get_algorithm());
}

bool compressor::is_hidden_option_name(std::string_view sv) {
    return sv.starts_with('.');
}

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
    auto unqualified = sstring(unqualified_name(name_prefix, name));
    for (int i = 0; i < static_cast<int>(algorithm::none); ++i) {
        auto alg = static_cast<algorithm>(i);
        if (std::string_view(unqualified) == algorithm_to_name(alg)) {
            return alg;
        }
    }
    throw std::runtime_error(std::format("Unknown sstable_compression: {}", name));
}

std::string_view compression_parameters::algorithm_to_name(algorithm alg) {
    switch (alg) {
        case algorithm::lz4: return "LZ4Compressor";
        case algorithm::lz4_with_dicts: return "LZ4WithDictsCompressor";
        case algorithm::deflate: return "DeflateCompressor";
        case algorithm::snappy: return "SnappyCompressor";
        case algorithm::zstd: return "ZstdCompressor";
        case algorithm::zstd_with_dicts: return "ZstdWithDictsCompressor";
        case algorithm::none: on_internal_error(compressor_factory_logger, "algorithm_to_name(): called with algorithm::none");
    }
    abort();
}

std::string compression_parameters::algorithm_to_qualified_name(algorithm alg) {
    auto short_name = compression_parameters::algorithm_to_name(alg);
    // For the cassandra-compatible compressors, we return the long name
    // ("org.apache.cassandra.io.compress.LZ4Compressor") for compatibility.
    //
    // For incompatible compressors, we only return the short name.
    // It wouldn't make sense to pretend they are a Java class.
    switch (alg) {
        case algorithm::lz4_with_dicts:
        case algorithm::zstd_with_dicts:
            return std::string(short_name);
        default: {
            auto result = std::string(name_prefix);
            result.append(short_name);
            return result;
        }
    }
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
    case algorithm::zstd_with_dicts:
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

void compression_parameters::validate(const gms::feature_service& fs) {
    if (!fs.sstable_compression_dicts) {
        if (_algorithm == algorithm::zstd_with_dicts || _algorithm == algorithm::lz4_with_dicts) {
            throw std::runtime_error(std::format("sstable_compression {} can't be used before "
                                                 "all nodes are upgraded to a versions which supports it",
                                                 algorithm_to_name(_algorithm)));
        }
    }
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
        opts.emplace(compression_parameters::SSTABLE_COMPRESSION, algorithm_to_qualified_name(_algorithm));
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
    : _cdict(std::move(cdict))
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
    if (_ddict) {
        ret = LZ4_decompress_safe_usingDict(input, output, input_len, output_len, reinterpret_cast<const char*>(_ddict->raw().data()), _ddict->raw().size());
    } else {
        SCYLLA_ASSERT(!_cdict && "Write-only compressor used for reading");
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
        SCYLLA_ASSERT(!_ddict && "Read-only compressor used for writing");
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

auto lz4_processor::get_algorithm() const -> algorithm {
    return (_cdict || _ddict) ? algorithm::lz4_with_dicts : algorithm::lz4;
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

compressor_ptr make_lz4_sstable_compressor_for_tests() {
    return std::make_unique<lz4_processor>();
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

// Holds weak pointers to all live dictionaries
// (so that they can be cheaply shared with new SSTables if an identical dict is requested),
// and shared (lifetime-extending) pointers to the current writer ("recommended")
// dict for each table (so that they can be shared with new SSTables without consulting
// `system.dicts`).
//
// Whenever a dictionary dies (because its refcount reaches 0), its weak pointer
// is removed from the factory.
//
// Tracks the total memory usage of existing dicts.
//
// Has a configurable memory budget for live dicts. If the budget is exceeded,
// will return null dicts to new writers (to avoid making the memory usage even worse)
// and print warnings.
class sstable_compressor_factory_impl : public weakly_referencable<sstable_compressor_factory_impl> {
    mutable logger::rate_limit budget_warning_rate_limit{std::chrono::minutes(10)};
    using config = default_sstable_compressor_factory::config;
    const config& _cfg;
    uint64_t _total_live_dict_memory = 0;
    metrics::metric_groups _metrics;
    struct zstd_cdict_id {
        dict_id id;
        int level;
        std::strong_ordering operator<=>(const zstd_cdict_id&) const = default;
    };
    std::map<dict_id, const raw_dict*> _raw_dicts;
    std::map<zstd_cdict_id, const zstd_cdict*> _zstd_cdicts;
    std::map<dict_id, const zstd_ddict*> _zstd_ddicts;
    std::map<dict_id, const lz4_cdict*> _lz4_cdicts;
    std::map<table_id, foreign_ptr<lw_shared_ptr<const raw_dict>>> _recommended;

    size_t memory_budget() const {
        return _cfg.memory_fraction_starting_at_which_we_stop_writing_dicts() * seastar::memory::stats().total_memory();
    }
    bool memory_budget_exceeded() const {
        return _total_live_dict_memory >= memory_budget();
    }
    void warn_budget_exceeded() const {
        compressor_factory_logger.log(
            log_level::warn,
            budget_warning_rate_limit,
            "Memory usage by live compression dicts ({} bytes) exceeds configured memory budget ({} bytes). Some new SSTables will fall back to compression without dictionaries.",
            _total_live_dict_memory,
            memory_budget()
        );
    }
public:
    lw_shared_ptr<const raw_dict> get_canonical_ptr(std::span<const std::byte> dict) {
        if (dict.empty()) {
            return nullptr;
        }
        auto id = get_sha256(dict);
        if (auto it = _raw_dicts.find(id); it != _raw_dicts.end()) {
            return it->second->shared_from_this();
        } else {
            auto p = make_lw_shared<const raw_dict>(*this, id, dict);
            _raw_dicts.emplace(id, p.get());
            return p;
        }
    }
    using foreign_zstd_ddict = foreign_ptr<lw_shared_ptr<const zstd_ddict>>;
    foreign_zstd_ddict get_zstd_dict_for_reading(lw_shared_ptr<const raw_dict> raw, int level) {
        if (!raw) {
            return nullptr;
        }
        lw_shared_ptr<const zstd_ddict> ddict;
        // Fo reading, we must allocate a new dict, even if memory budget is exceeded. We have no other choice.
        // In any case, if the budget is exceeded after we print a rate-limited warning about it.
        if (auto it = _zstd_ddicts.find(raw->id()); it != _zstd_ddicts.end()) {
            ddict = it->second->shared_from_this();
        } else {
            ddict = make_lw_shared<zstd_ddict>(*this, raw);
            _zstd_ddicts.emplace(raw->id(), ddict.get());
        }
        if (memory_budget_exceeded()) {
            warn_budget_exceeded();
            compressor_factory_logger.debug("make_compressor_for_writing: falling back to no dict");
        }
        return make_foreign(std::move(ddict));
    }
    using foreign_zstd_cdict = foreign_ptr<lw_shared_ptr<const zstd_cdict>>;
    foreign_zstd_cdict get_zstd_dict_for_writing(lw_shared_ptr<const raw_dict> raw, int level) {
        if (!_cfg.enable_writing_dictionaries() || !raw) {
            return nullptr;
        }
        lw_shared_ptr<const zstd_cdict> cdict;
        // If we can share an already-allocated dict, we do that regardless of memory budget.
        // If we would have to allocate a new dict for writing, we only do that if we haven't exceeded
        // the budget yet. Otherwise we return null.
        if (auto it = _zstd_cdicts.find({raw->id(), level}); it != _zstd_cdicts.end()) {
            cdict = it->second->shared_from_this();
        } else if (memory_budget_exceeded()) {
            warn_budget_exceeded();
            compressor_factory_logger.debug("make_compressor_for_writing: falling back to no dict");
        } else {
            cdict = make_lw_shared<zstd_cdict>(*this, raw, level);
            _zstd_cdicts.emplace(zstd_cdict_id{raw->id(), level}, cdict.get());
        }
        return make_foreign(std::move(cdict));
    }
    using lz4_dicts = std::pair<
        foreign_ptr<lw_shared_ptr<const raw_dict>>,
        foreign_ptr<lw_shared_ptr<const lz4_cdict>>
    >;
    using foreign_lz4_ddict = foreign_ptr<lw_shared_ptr<const raw_dict>>;
    using foreign_lz4_cdict = foreign_ptr<lw_shared_ptr<const lz4_cdict>>;
    foreign_lz4_ddict get_lz4_dict_for_reading(lw_shared_ptr<const raw_dict> raw) {
        return make_foreign(std::move(raw));
    }
    foreign_lz4_cdict get_lz4_dict_for_writing(lw_shared_ptr<const raw_dict> raw) {
        if (!_cfg.enable_writing_dictionaries() || !raw) {
            return nullptr;
        }
        lw_shared_ptr<const lz4_cdict> cdict;
        // If we can share an already-allocated dict, we do that regardless of memory budget.
        // If we would have to allocate a new dict for writing, we only do that if we haven't exceeded
        // the budget yet. Otherwise we return null.
        if (auto it = _lz4_cdicts.find(raw->id()); it != _lz4_cdicts.end()) {
            cdict = it->second->shared_from_this();
        } else if (memory_budget_exceeded()) {
            warn_budget_exceeded();
        } else {
            cdict = make_lw_shared<lz4_cdict>(*this, raw);
            _lz4_cdicts.emplace(raw->id(), cdict.get());
        }
        return make_foreign(std::move(cdict));
    }

public:
    sstable_compressor_factory_impl(const config& cfg)
        : _cfg(cfg)
    {
        if (_cfg.register_metrics) {
            namespace sm = seastar::metrics;
            _metrics.add_group("sstable_compression_dicts", {
                sm::make_counter("total_live_memory_bytes", _total_live_dict_memory, sm::description("Total amount of memory consumed by SSTable compression dictionaries in RAM")),
            });
        }
    }
    sstable_compressor_factory_impl(sstable_compressor_factory_impl&&) = delete;
    ~sstable_compressor_factory_impl() {
        // Note: `_recommended` might be the only thing keeping some dicts alive,
        // so clearing it will destroy them.
        //
        // In the destructor, they will call back into us to erase themselves from the `std::map`
        // which map `table_id`s to dicts.
        //
        // Erasing from already-destroyed maps would be illegal, so the `_recommended`
        // must be cleared before the maps are destroyed.
        //
        // We could just rely on the member field destruction order for that,
        // but let's be explicit and clear it manually before any fields are destroyed.
        // (Calling back into the partially-destroyed factory would be iffy anyway, even if it was legal).
        _recommended.clear();
    }
    void forget_raw_dict(dict_id id) {
        _raw_dicts.erase(id);
    }
    void forget_zstd_cdict(dict_id id, int level) {
        _zstd_cdicts.erase({id, level});
    }
    void forget_zstd_ddict(dict_id id) {
        _zstd_ddicts.erase(id);
    }
    void forget_lz4_cdict(dict_id id) {
        _lz4_cdicts.erase(id);
    }
    void set_recommended_dict(table_id t, foreign_ptr<lw_shared_ptr<const raw_dict>> dict) {
        _recommended.erase(t);
        if (dict) {
            compressor_factory_logger.debug("set_recommended_dict: table={} size={} id={}",
                t, dict->raw().size(), fmt_hex(dict->id()));
            _recommended.emplace(t, std::move(dict));
        } else {
            compressor_factory_logger.debug("set_recommended_dict: table={} size=0", t);
        }
    }
    future<foreign_ptr<lw_shared_ptr<const raw_dict>>> get_recommended_dict(table_id t) {
        auto rec_it = _recommended.find(t);
        if (rec_it == _recommended.end()) {
            co_return nullptr;
        }
        co_return co_await rec_it->second.copy();
    }

    void account_memory_delta(ssize_t n) {
        if (static_cast<ssize_t>(_total_live_dict_memory) + n < 0) {
            compressor_factory_logger.error(
                "Error in dictionary memory accounting: delta {} brings live memory {} below 0",
                n, _total_live_dict_memory);
        }
        _total_live_dict_memory += n;
    }
};

default_sstable_compressor_factory::default_sstable_compressor_factory(config cfg)
    : _cfg(std::move(cfg))
    , _impl(std::make_unique<sstable_compressor_factory_impl>(_cfg))
{
    for (shard_id i = 0; i < smp::count; ++i) {
        auto numa_id = _cfg.numa_config[i];
        _numa_groups.resize(std::max<size_t>(_numa_groups.size(), numa_id + 1));
        _numa_groups[numa_id].push_back(i);
    }
}

default_sstable_compressor_factory::~default_sstable_compressor_factory() {
}

std::vector<unsigned> default_sstable_compressor_factory_config::get_default_shard_to_numa_node_mapping() {
    auto sp = local_engine->smp().shard_to_numa_node_mapping();
    return std::vector<unsigned>(sp.begin(), sp.end());
}

unsigned default_sstable_compressor_factory::local_numa_id() {
    return _cfg.numa_config[this_shard_id()];
}

shard_id default_sstable_compressor_factory::get_dict_owner(unsigned numa_id, const sha256_type& sha) {
    auto hash = read_unaligned<uint64_t>(sha.data());
    const auto& group = _numa_groups[numa_id];
    if (group.empty()) {
        on_internal_error(compressor_factory_logger, "get_dict_owner called on an empty NUMA group");
    }
    return group[hash % group.size()];
}

future<> default_sstable_compressor_factory::set_recommended_dict_local(table_id t, std::span<const std::byte> dict) {
    if (_leader_shard != this_shard_id()) {
        on_internal_error(compressor_factory_logger, fmt::format("set_recommended_dict_local called on wrong shard. Expected: {}, got {}", _leader_shard, this_shard_id()));
    }
    auto units = co_await get_units(_recommendation_setting_sem, 1);
    auto sha = get_sha256(dict);
    for (unsigned numa_id = 0; numa_id < _numa_groups.size(); ++numa_id) {
        const auto& group = _numa_groups[numa_id];
        if (group.empty()) {
            continue;
        }
        auto r = get_dict_owner(numa_id, sha);
        auto d = co_await container().invoke_on(r, [dict](self& local) {
            return make_foreign(local._impl->get_canonical_ptr(dict));
        });
        auto local_coordinator = group[0];
        co_await container().invoke_on(local_coordinator, coroutine::lambda([t, d = std::move(d)](self& local) mutable {
            local._impl->set_recommended_dict(t, std::move(d));
        }));
    }
}

future<> default_sstable_compressor_factory::set_recommended_dict(table_id t, std::span<const std::byte> dict) {
    return container().invoke_on(_leader_shard, &self::set_recommended_dict_local, t, dict);
}

future<foreign_ptr<lw_shared_ptr<const raw_dict>>> default_sstable_compressor_factory::get_recommended_dict(table_id t) {
    const auto local_coordinator = _numa_groups[local_numa_id()][0];
    return container().invoke_on(local_coordinator, [t](self& local) {
        return local._impl->get_recommended_dict(t);
    });
}

future<compressor_ptr> default_sstable_compressor_factory::make_compressor_for_writing(schema_ptr s) {
    const auto params = s->get_compressor_params();
    using algorithm = compression_parameters::algorithm;
    const auto algo = params.get_algorithm();
    compressor_factory_logger.debug("make_compressor_for_writing: table={} algo={}", s->id(), algo);
    switch (algo) {
    case algorithm::lz4:
        co_return std::make_unique<lz4_processor>(nullptr, nullptr);
    case algorithm::lz4_with_dicts: {
        impl::foreign_lz4_cdict cdict;
        if (auto recommended = co_await get_recommended_dict(s->id())) {
            cdict = co_await container().invoke_on(recommended.get_owner_shard(), [recommended = std::move(recommended)] (self& local) mutable {
                return local._impl->get_lz4_dict_for_writing(recommended.release());
            });
        }
        if (cdict) {
            compressor_factory_logger.debug("make_compressor_for_writing: using dict id={}", fmt_hex(cdict->id()));
        }
        co_return std::make_unique<lz4_processor>(std::move(cdict), nullptr);
    }
    case algorithm::deflate:
        co_return std::make_unique<deflate_processor>();
    case algorithm::snappy:
        co_return std::make_unique<snappy_processor>();
    case algorithm::zstd:
        co_return std::make_unique<zstd_processor>(params, nullptr, nullptr);
    case algorithm::zstd_with_dicts: {
        impl::foreign_zstd_cdict cdict;
        if (auto recommended = co_await get_recommended_dict(s->id())) {
            auto level = params.zstd_compression_level().value_or(ZSTD_defaultCLevel());
            cdict = co_await container().invoke_on(recommended.get_owner_shard(), [level, recommended = std::move(recommended)] (self& local) mutable {
                return local._impl->get_zstd_dict_for_writing(recommended.release(), level);
            });
        }
        if (cdict) {
            compressor_factory_logger.debug("make_compressor_for_writing: using dict id={}", fmt_hex(cdict->id()));
        }
        co_return std::make_unique<zstd_processor>(params, std::move(cdict), nullptr);
    }
    case algorithm::none:
        co_return nullptr;
    }
    abort();
}

future<compressor_ptr> default_sstable_compressor_factory::make_compressor_for_reading(sstables::compression& c) {
    const auto params = compression_parameters(sstables::options_from_compression(c));
    using algorithm = compression_parameters::algorithm;
    const auto algo = params.get_algorithm();
    compressor_factory_logger.debug("make_compressor_for_reading: compression={} algo={}", fmt::ptr(&c), algo);
    switch (algo) {
    case algorithm::lz4:
        co_return std::make_unique<lz4_processor>(nullptr, nullptr);
    case algorithm::lz4_with_dicts: {
        auto dict = dict_from_options(c);
        auto dict_span = std::as_bytes(std::span(*dict));
        auto sha = get_sha256(dict_span);
        auto dict_owner = get_dict_owner(local_numa_id(), sha);
        auto ddict = co_await container().invoke_on(dict_owner, [dict_span] (self& local) mutable {
            auto d = local._impl->get_canonical_ptr(dict_span);
            return local._impl->get_lz4_dict_for_reading(std::move(d));
        });
        if (ddict) {
            compressor_factory_logger.debug("make_compressor_for_reading: using dict id={}", fmt_hex(ddict->id()));
        }
        co_return std::make_unique<lz4_processor>(nullptr, std::move(ddict));
    }
    case algorithm::deflate:
        co_return std::make_unique<deflate_processor>();
    case algorithm::snappy:
        co_return std::make_unique<snappy_processor>();
    case algorithm::zstd: {
        co_return std::make_unique<zstd_processor>(params, nullptr, nullptr);
    }
    case algorithm::zstd_with_dicts: {
        auto level = params.zstd_compression_level().value_or(ZSTD_defaultCLevel());
        auto dict = dict_from_options(c);
        auto dict_span = std::as_bytes(std::span(*dict));
        auto sha = get_sha256(dict_span);
        auto dict_owner = get_dict_owner(local_numa_id(), sha);
        auto ddict = co_await container().invoke_on(dict_owner, [level, dict_span] (self& local) mutable {
            auto d = local._impl->get_canonical_ptr(dict_span);
            return local._impl->get_zstd_dict_for_reading(std::move(d), level);
        });
        if (ddict) {
            compressor_factory_logger.debug("make_compressor_for_reading: using dict id={}", fmt_hex(ddict->id()));
        }
        co_return std::make_unique<zstd_processor>(params, nullptr, std::move(ddict));
    }
    case algorithm::none:
        co_return nullptr;
    }
    abort();
}

raw_dict::raw_dict(sstable_compressor_factory_impl& owner, dict_id key, std::span<const std::byte> dict)
    : _owner(owner.weak_from_this())
    , _id(key)
    , _dict(dict.begin(), dict.end())
{
    _owner->account_memory_delta(malloc_usable_size(const_cast<std::byte*>(_dict.data())));
}

raw_dict::~raw_dict() {
    if (_owner) {
        _owner->forget_raw_dict(_id);
        _owner->account_memory_delta(-malloc_usable_size(const_cast<std::byte*>(_dict.data())));
    }
}

zstd_cdict::zstd_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw, int level)
    : _owner(owner.weak_from_this())
    , _raw(raw)
    , _level(level)
    , _alloc([this] (ssize_t n) {
        _used_memory += n;
        if (_owner) {
            _owner->account_memory_delta(n);
        }})
    , _dict(
        ZSTD_createCDict_advanced(
            _raw->raw().data(),
            _raw->raw().size(),
            ZSTD_dlm_byRef,
            ZSTD_dct_auto,
            ZSTD_getCParams(level, 4096, _raw->raw().size()),
            _alloc.as_zstd_custommem()),
        ZSTD_freeCDict)
{
    if (!_dict) {
        throw std::bad_alloc();
    }
}


zstd_cdict::~zstd_cdict() {
    if (_owner) {
        _owner->forget_zstd_cdict(_raw->id(), _level);
        // Note: memory delta will be accounted by the destruction of the allocated dict.
    }
}

zstd_ddict::zstd_ddict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw)
    : _owner(owner.weak_from_this())
    , _raw(raw)
    , _alloc([this] (ssize_t n) {
        _used_memory += n;
        if (_owner) {
            _owner->account_memory_delta(n);
        }})
    , _dict(
        ZSTD_createDDict_advanced(
            _raw->raw().data(),
            _raw->raw().size(),
            ZSTD_dlm_byRef,
            ZSTD_dct_auto,
            _alloc.as_zstd_custommem()),
        ZSTD_freeDDict)
{
    if (!_dict) {
        throw std::bad_alloc();
    }
}

zstd_ddict::~zstd_ddict() {
    if (_owner) {
        _owner->forget_zstd_ddict(_raw->id());
        // Note: memory delta will be accounted by the destruction of the allocated dict.
    }
}

lz4_cdict::lz4_cdict(sstable_compressor_factory_impl& owner, lw_shared_ptr<const raw_dict> raw)
    : _owner(owner.weak_from_this())
    , _raw(raw)
    , _dict(LZ4_createStream(), LZ4_freeStream)
{
    if (!_dict) {
        throw std::bad_alloc();
    }
    LZ4_loadDictSlow(_dict.get(), reinterpret_cast<const char*>(_raw->raw().data()), _raw->raw().size());
    _owner->account_memory_delta(malloc_usable_size(_dict.get()));
}

lz4_cdict::~lz4_cdict() {
    if (_owner) {
        _owner->account_memory_delta(-malloc_usable_size(_dict.get()));
        _owner->forget_lz4_cdict(_raw->id());
    }
}

std::unique_ptr<sstable_compressor_factory> make_sstable_compressor_factory_for_tests_in_thread() {
    SCYLLA_ASSERT(thread::running_in_thread());
    struct wrapper : sstable_compressor_factory {
        using impl = default_sstable_compressor_factory;
        sharded<impl> _impl;
        future<compressor_ptr> make_compressor_for_writing(schema_ptr s) override {
            return _impl.local().make_compressor_for_writing(s);
        }
        future<compressor_ptr> make_compressor_for_reading(sstables::compression& c) override {
            return _impl.local().make_compressor_for_reading(c);
        }
        future<> set_recommended_dict(table_id t, std::span<const std::byte> d) override {
            return _impl.local().set_recommended_dict(t, d);
        };
        wrapper(wrapper&&) = delete;
        wrapper() {
            _impl.start().get();
        }
        ~wrapper() {
            _impl.stop().get();
        }
    };
    return std::make_unique<wrapper>();
}

