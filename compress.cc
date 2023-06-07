/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>
#include <snappy.h>
#include <snappy-sinksource.h>

#include <seastar/util/defer.hh>

#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"
#include "utils/fragmented_temporary_buffer.hh"

const sstring compressor::namespace_prefix = "org.apache.cassandra.io.compress.";

class lz4_processor: public compressor {
public:
    lz4_processor(sstring name)
        : compressor(std::move(name), true)
    {}

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

    size_t uncompress(bytes_source& in, bytes_ostream&) const override;

    size_t compress(bytes_source& in, bytes_ostream&) const override;
};

class deflate_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    size_t uncompress(bytes_source& in, bytes_ostream&) const override;

    size_t compress(bytes_source& in, bytes_ostream&) const override;
};

compressor::compressor(sstring name, bool writes_uncompressed_size)
    : _name(std::move(name))
    /**
     * Whether the compressors (linear) compress method already
     * writes the uncompressed size. If true, must adhere/use the
     * write_uncompressed_size version thereof.
     * 
     * This _only_ exists because lz4 compressor for historical reasons
     * (cassandra) does this, and we want to avoid redundancy 
     * in forthcoming patches.
    */
    , _writes_uncompressed_size(writes_uncompressed_size)
{
    (void)_writes_uncompressed_size;
}

std::set<sstring> compressor::option_names() const {
    return {};
}

std::map<sstring, sstring> compressor::options() const {
    return {};
}

/**
 * This method allows a compressor to do (shallow) object replace
 * of a cached compressor object, typically to be able to share 
 * expensive payloads, such as the compression library objects,
 * buffers, whatnot, yet maintain shallow holders for differing
 * parameters/options set by compressor parameters.
 **/
compressor::ptr_type compressor::replace(const opt_getter&) const {
    return nullptr;
}

compressor::ptr_type compressor::create(const sstring& name, const opt_getter& opts) {
    if (name.empty()) {
        return {};
    }

    qualified_name qn(namespace_prefix, name);

    for (auto& c : { lz4, snappy, deflate }) {
        if (c->name() == static_cast<const sstring&>(qn)) {
            return c;
        }
    }

    static thread_local std::unordered_map<sstring, ptr_type> named_compressors;

    ptr_type res = nullptr;

    auto i = named_compressors.find(name);
    if (i != named_compressors.end()) {
        res = i->second;
        /**
         * If we have a cached compressor, we need to potentially
         * (shallow) replace it to ensure we respect the compressor
         * parameters set by "opts" (or lack thereof).
         * Implementations can handle this in "replace", and either
         * make an expensive replacement, or, more likely, a very cheap one...
         * (zstd).
        */
        auto rep = res->replace(opts);
        if (rep) {
            res = std::move(rep);
        }
    }

    if (!res) {
        res = compressor_registry::create(qn, opts);
        if (res) {
            named_compressors.emplace(name, res);
        }
    }

    return res;
}

shared_ptr<compressor> compressor::create(const std::map<sstring, sstring>& options, const sstring& class_opt) {
    auto i = options.find(class_opt);
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

static constexpr auto uncompressed_size_marker_size = 4u;

/*
 * This is OCD level stuff, but because we for historical reasons (i.e. cassandra sstable compatibility)
 * write uncompressed size in some compressors (hello lz4), I cannot in good concience allow a user (me)
 * to duplicate this info for other. So we keep track per compressor type etc whether and how to add
 * this to a packet.
 * 
 * Note: for the fragmented compressor methods above, for some compressors at least we should not need
 * having this info at all. Esp. zstd/deflate can just stream compressed data until exhausted. LZ4
 * interface for this is yukky, but workable etc. So room for better fruits here....
 * 
 * TLDR: TODO: refactor sstable/other compression to 
 *      a.) do always fragmented operation
 *      b.) only use frame markers for lz4 in sstable and interop reasons.
 */ 
static size_t write_uncompressed_size(int8_t* out, size_t size, size_t input_len) {
    if (size < uncompressed_size_marker_size) {
        throw std::invalid_argument("Buffer underflow");
    }
    static_assert(sizeof(uint32_t) == uncompressed_size_marker_size);
    if (input_len > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("Compressed size overflow");
    }
    out[0] = input_len & 0xFF;
    out[1] = (input_len >> 8) & 0xFF;
    out[2] = (input_len >> 16) & 0xFF;
    out[3] = (input_len >> 24) & 0xFF;

    return uncompressed_size_marker_size;
}

static size_t read_uncompressed_size(const int8_t* in, size_t size) {
    if (size < uncompressed_size_marker_size) {
        throw std::invalid_argument("Buffer underflow");
    }
    size_t res = (in[3] << 24 & 0xff000000)
        | (in[2] << 16 & 0x00ff0000)
        | (in[1] << 8 & 0x0000ff00)
        | (in[0] << 0 & 0x000000ff)
        ;
    return res;
}


bytes_source::bytes_source(bool lin)
    : _linearized(lin)
{}

class buffer_bytes_source : public bytes_source {
    bytes _buf;
    bool _done = false;
public:
    buffer_bytes_source(bytes_source& in)
        : bytes_source(true)
        , _buf(bytes::initialized_later(), in.size())        
    {
        auto out = _buf.begin();
        for (;;) {
            auto v = in.next();
            if (v.empty()) {
                break;
            }
            out = std::copy(v.begin(), v.end(), out);
        }
    }
    bytes_view next() override {
        if (std::exchange(_done, true)) {
            _buf = {};
        }
        return _buf;
    }
    size_t size() const override {
        return _buf.size();
    }
};

class managed_bytes_source : public bytes_source {
    managed_bytes_view _view;
    bool _first = true;
public:
    managed_bytes_source(managed_bytes_view in)
        : bytes_source(in.is_linearized())
        , _view(std::move(in))
    {}
    bytes_view next() override {
        if (!std::exchange(_first, false)) {
            _view.remove_current();
        }
        return _view.current_fragment();
    }
    size_t size() const override {
        return _view.size();
    }
};

class fragmented_bytes_source : public bytes_source {
    fragmented_temporary_buffer::view _view;
    bool _first = true;
public:
    fragmented_bytes_source(fragmented_temporary_buffer::view in)
        : bytes_source(in.size_bytes() == in.current_fragment().size())
        , _view(std::move(in))
    {}
    bytes_view next() override {
        if (!std::exchange(_first, false)) {
            _view.remove_current();
        }
        return _view.current_fragment();
    }
    size_t size() const override {
        return _view.size_bytes();
    }
};

size_t compressor::uncompress(managed_bytes_view in, bytes_ostream& os) const {
    managed_bytes_source tmp(in);
    return uncompress(tmp, os);
}

size_t compressor::uncompress(const fragmented_temporary_buffer& in, bytes_ostream& os) const {
    fragmented_bytes_source tmp{fragmented_temporary_buffer::view(in)};
    return uncompress(tmp, os);
}

size_t compressor::uncompress(bytes_source& in, bytes_ostream& os) const {
    // default. fallback to linearized.
    if (!in.is_linearized()) {
        buffer_bytes_source tmp(in);
        return uncompress(tmp, os);
    }

    auto view = in.next();
    auto uncompressed_size = read_uncompressed_size(view.data(), view.size());

    if (!_writes_uncompressed_size) {
        view.remove_prefix(uncompressed_size_marker_size);
    }

    auto out = os.write_place_holder(uncompressed_size);
    auto res = uncompress(reinterpret_cast<const char*>(view.data()), view.size(), reinterpret_cast<char*>(out), uncompressed_size);

    assert(res == uncompressed_size);
    return res;
}


size_t compressor::compress(managed_bytes_view in, bytes_ostream& os) const {
    managed_bytes_source tmp(in);
    return compress(tmp, os);
}

size_t compressor::compress(const fragmented_temporary_buffer& in, bytes_ostream& os) const {
    fragmented_bytes_source tmp{fragmented_temporary_buffer::view(in)};
    return compress(tmp, os);
}

size_t compressor::compress(bytes_source& in, bytes_ostream& os) const {
    if (!in.is_linearized()) {
        buffer_bytes_source tmp(in);
        return compress(tmp, os);
    }
    auto view = in.next();
    auto input_len = view.size();
    if (view.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::invalid_argument("Data overflow");
    }

    auto size = compress_max_size(input_len);
    if (!_writes_uncompressed_size) {
        size += uncompressed_size_marker_size;
    }
    auto out = os.write_place_holder(size);
    if (!_writes_uncompressed_size) {
        auto n = write_uncompressed_size(out, size, input_len);
        out += n;
        size -= n;
    }
    auto actual = compress(reinterpret_cast<const char*>(view.data()), input_len, reinterpret_cast<char*>(out), size);
    assert(actual <= size);
    os.remove_suffix(size - actual);
    return actual;
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
    _compressor = compressor::create(options, SSTABLE_COMPRESSION);

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
    (void)&read_uncompressed_size;
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
    write_uncompressed_size(reinterpret_cast<int8_t*>(output), output_len, input_len);
    auto ret = LZ4_compress_default(input, output + 4, input_len, LZ4_compressBound(input_len));
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

size_t deflate_processor::uncompress(bytes_source& in, bytes_ostream& os) const {
    z_stream zs = { 0, };

    if (inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("deflate uncompression init failure");
    }

    auto cleanup = defer([&zs] {
        inflateEnd(&zs);
    });

    auto pos = os.size();

    for (;;) {
        auto frag = in.next();
        if (frag.empty()) {
            break;
        }

        auto size = frag.size();
        auto flush = in.size() == size ? Z_FINISH : Z_NO_FLUSH;
        auto ret = Z_OK;

        zs.next_in = reinterpret_cast<Bytef*>(const_cast<int8_t*>(frag.data()));
        zs.avail_in = size;

        do {
            if (zs.avail_out == 0) {
                auto osize = std::max(64u, 2 * zs.avail_in);;
                auto out = os.write_place_holder(osize);
                zs.next_out = reinterpret_cast<Bytef*>(out);
                zs.avail_out = osize;
            }

            ret = inflate(&zs, flush);

            if (ret == Z_STREAM_ERROR) {
                throw std::runtime_error("deflate uncompression failure");
            }
        } while (zs.avail_out == 0);
    }

    os.remove_suffix(zs.avail_out);

    return os.size() - pos;
}

size_t deflate_processor::compress(bytes_source& in, bytes_ostream& os) const {
    z_stream zs = { 0, };

    if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
        throw std::runtime_error("deflate compression init failure");
    }

    auto cleanup = defer([&zs] {
        deflateEnd(&zs);
    });

    auto pos = os.size();

    for (;;) {
        auto frag = in.next();
        if (frag.empty()) {
            break;
        }

        auto size = frag.size();
        auto flush = in.size() == size ? Z_FINISH : Z_NO_FLUSH;
        auto ret = Z_OK;

        zs.next_in = reinterpret_cast<Bytef*>(const_cast<int8_t*>(frag.data()));
        zs.avail_in = size;

        do {
            if (zs.avail_out == 0) {
                auto osize = std::max(64u, 2 * zs.avail_in);
                auto out = os.write_place_holder(osize);
                zs.next_out = reinterpret_cast<Bytef*>(out);
                zs.avail_out = osize;
            }

            ret = ::deflate(&zs, flush);

            if (ret == Z_STREAM_ERROR) {
                throw std::runtime_error("deflate compression failure");
            }
        } while (zs.avail_out == 0);
    }

    os.remove_suffix(zs.avail_out);

    return os.size() - pos;
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

class BSSource : public snappy::Source {
    bytes_source& _src;
    bytes_view _view;
public:
    BSSource(bytes_source& in)
        : _src(in)
        , _view(in.next())
    {}
    size_t Available() const override {
        return _src.size(); // includes size of current view (_view)
    }
    const char* Peek(size_t* len) override {
        *len = _view.size();
        return reinterpret_cast<const char*>(_view.data());
    }
    void Skip(size_t n) override {
        assert(n <= _view.size());
        _view.remove_prefix(n);
        if (_view.empty()) {
            _view = _src.next();
        }
    }
};

class BOSSink : public snappy::Sink {
    bytes_ostream& _os;
    bytes_mutable_view _view;
    size_t _written = 0;
public:
    BOSSink(bytes_ostream& os)
        : _os(os)
    {}
    ~BOSSink() {
        _os.remove_suffix(_view.size());
    }
    void Append(const char* bytes, size_t n) override {
        if (reinterpret_cast<const int8_t*>(bytes) != _view.data()) {
            _os.write(bytes_view(reinterpret_cast<const int8_t*>(bytes), n));
        } else {
            _view.remove_prefix(n);
        }
        _written += n;
    }
    char* GetAppendBuffer(size_t length, char* scratch) override {
        return GetAppendBufferVariable(length, length, scratch, length, &length);
    }
    char* GetAppendBufferVariable(size_t min_size, size_t desired_size_hint, char* scratch, size_t scratch_size, size_t* allocated_size) override {
        if (_view.size() < min_size) {
            _os.remove_suffix(_view.size());
            _view = bytes_mutable_view(_os.write_place_holder(desired_size_hint), desired_size_hint);
        }
        *allocated_size = _view.size();
        return reinterpret_cast<char*>(_view.data());
    }
    size_t written() const {
        return _written;
    }
};

size_t snappy_processor::uncompress(bytes_source& in, bytes_ostream& os) const {
    BSSource src(in);
    BOSSink sink(os);

    if (!snappy::Uncompress(&src, &sink)) {
        throw std::runtime_error("snappy uncompression failure");
    }

    return sink.written();
}

size_t snappy_processor::compress(bytes_source& in, bytes_ostream& os) const {
    BSSource src(in);
    BOSSink sink(os);

    if (!snappy::Compress(&src, &sink)) {
        throw std::runtime_error("snappy compression failure");
    }

    return sink.written();
}
