/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/compression.hh"

#include <limits>
#include <span>
#include <stdexcept>
#include <unordered_map>

#include <lz4.h>
#include <snappy.h>
#include <snappy-sinksource.h>
#include <zlib.h>
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/log.hh"

namespace sstables::parquet {

static logging::logger compression_logger("parquet_compression");

// The size of the contiguous pieces of page buffers.
static constexpr size_t piece_size = fragmented_temporary_buffer::default_fragment_size;
static_assert(sync_compress_threshold <= piece_size && sync_decompress_threshold <= piece_size);

// The work of one step of a large call: at most this much input of a
// compression, or output of a decompression, plus the bounded work of the
// codec around it.
static constexpr size_t step_size = 64 * 1024;

// Page sizes are 32-bit signed numbers in the page header.
static constexpr size_t max_page_size = std::numeric_limits<int32_t>::max();

// LZ4 compresses a page in chunks of this size, which is its window.
static constexpr size_t lz4_chunk_size = 64 * 1024;

// SNAPPY compresses a page in blocks of this size. The library allocates
// its working memory on every call: a hash table of two bytes per input
// byte, rounded up to a power of two, plus an input and an output buffer of
// about the block size. That is about 70 KiB for 16 KiB, and more than
// 128 KiB for 32 KiB.
static constexpr size_t snappy_block_size = 16 * 1024;

[[noreturn]] static void malformed(compression_codec codec, std::string_view what) {
    on_malformed_data(seastar::format("compressed page (codec {}): {}", int(codec), what));
}

// Per-shard scratch buffers. A step uses them and is done with them before
// it returns, so they are never held across a preemption point.
//
// The input buffer holds the LZ4 input of a small call, and the chunks of
// LZ4 and SNAPPY that cross contiguous pieces of the input. It has exactly
// 128 KiB.
static std::span<char> scratch_in() {
    static thread_local std::unique_ptr<char[]> b;
    if (!b) {
        b = std::make_unique_for_overwrite<char[]>(piece_size);
    }
    return {b.get(), piece_size};
}

// The output buffer holds a compressed LZ4 chunk or SNAPPY block, and the
// output of a zstd or zlib compression step before it is copied to the
// output stream.
static std::span<char> scratch_out() {
    // snappy::MaxCompressedLength() is not constexpr. It is 32 + n + n / 6.
    static constexpr size_t size = std::max({size_t(LZ4_COMPRESSBOUND(lz4_chunk_size)), step_size,
            32 + snappy_block_size + snappy_block_size / 6});
    static_assert(size <= piece_size);
    static thread_local std::unique_ptr<char[]> b;
    if (!b) {
        b = std::make_unique_for_overwrite<char[]>(size);
    }
    return {b.get(), size};
}

// The state of LZ4 compression, about 16 KiB.
static void* lz4_state() {
    static thread_local std::unique_ptr<LZ4_stream_t> s;
    if (!s) {
        s = std::make_unique<LZ4_stream_t>();
    }
    return s.get();
}

// Collects decompressed output into contiguous pieces of at most
// `piece_size` bytes. Pieces are allocated as output arrives, so a header
// that declares a huge size costs nothing until the data really
// decompresses to that size.
// Once `expected` bytes have been produced, further output goes to a
// one-byte overflow area, which shows that the page is too large.
//
// Every piece but the last has `piece_size` bytes, so a position in the
// output maps to a piece by division. The decoders of SNAPPY and LZ4_RAW
// copy matches from earlier output with copy_match().
class output_pieces {
    size_t _expected;
    size_t _allocated = 0;
    size_t _produced = 0;
    std::vector<temporary_buffer<char>> _pieces;
    // The free part of the last piece starts at _pieces.back().size() - _free.
    size_t _free = 0;
    char _overflow;
    bool _overflowed = false;
public:
    explicit output_pieces(size_t expected) : _expected(expected) {
        // `expected` comes from an untrusted header, so the vector of pieces
        // grows with the output instead of being reserved for it.
        _pieces.reserve(std::min<size_t>((expected + piece_size - 1) / piece_size, 8));
    }

    // Returns writable space, at least one byte.
    std::span<char> space() {
        if (_free == 0) {
            if (_allocated == _expected) {
                return {&_overflow, 1};
            }
            const size_t n = std::min(piece_size, _expected - _allocated);
            _pieces.emplace_back(n);
            _allocated += n;
            _free = n;
        }
        auto& last = _pieces.back();
        return {last.get_write() + last.size() - _free, _free};
    }

    // Records that `n` bytes of the last space() were written.
    void advance(size_t n) {
        if (n == 0) {
            return;
        }
        if (_free == 0) {
            _overflowed = true;
            return;
        }
        _free -= n;
        _produced += n;
    }

    bool overflowed() const noexcept { return _overflowed; }
    size_t produced() const noexcept { return _produced; }
    // The bytes that may still be produced.
    size_t left() const noexcept { return _expected - _produced; }

    // Copies `n` bytes of input. The caller checks that n <= left().
    void copy_from(byte_reader& in, size_t n) {
        while (n) {
            auto s = space();
            const size_t k = std::min(n, s.size());
            in.read(s.data(), k);
            advance(k);
            n -= k;
        }
    }

    // Appends `n` bytes that repeat the output from `offset` bytes back.
    // The caller checks that 0 < offset <= produced() and n <= left().
    void copy_match(size_t offset, size_t n) {
        // The match repeats with a period of `offset`. So once a part of it
        // is written, the copy may take its source from any multiple of
        // `offset` back that does not reach before the source's start. The
        // distance doubles as the match grows, so that a long match with a
        // short period takes few copies. A copy never overlaps its source.
        const size_t source_start = _produced - offset;
        size_t distance = offset;
        while (n) {
            auto dst = space();
            const size_t src = _produced - distance;
            const auto& piece = _pieces[src / piece_size];
            const size_t in_piece = src % piece_size;
            const size_t k = std::min({n, distance, dst.size(), piece.size() - in_piece});
            std::memcpy(dst.data(), piece.get() + in_piece, k);
            advance(k);
            n -= k;
            while (2 * distance <= _produced - source_start) {
                distance *= 2;
            }
        }
    }

    fragmented_temporary_buffer finish(compression_codec codec) && {
        if (_produced != _expected) {
            malformed(codec, seastar::format("decompressed to {} bytes, the page header declares {}", _produced, _expected));
        }
        return fragmented_temporary_buffer(std::move(_pieces), _expected);
    }
};

// Reads the input of a compression.
class input_reader {
    bytes_ostream::fragment_iterator _it;
    bytes_view _current;
    size_t _left;

    void fill() {
        while (_current.empty() && _left) {
            _current = *_it++;
        }
    }
public:
    explicit input_reader(const bytes_ostream& in) : _it(in.begin()), _left(in.size()) {}

    size_t remaining() const noexcept { return _left; }

    // Returns up to `n` bytes in place, at most the rest of a contiguous piece.
    bytes_view next(size_t n) {
        fill();
        auto r = _current.substr(0, n);
        _current.remove_prefix(r.size());
        _left -= r.size();
        return r;
    }

    // Returns the next min(n, remaining()) bytes, in place if they lie in one
    // contiguous piece, and copied to `scratch` otherwise.
    bytes_view take(size_t n, std::span<char> scratch) {
        n = std::min(n, _left);
        fill();
        if (_current.size() >= n) {
            return next(n);
        }
        for (size_t pos = 0; pos < n;) {
            auto v = next(n - pos);
            std::memcpy(scratch.data() + pos, v.data(), v.size());
            pos += v.size();
        }
        return bytes_view(reinterpret_cast<const bytes_view::value_type*>(scratch.data()), n);
    }

    void copy_to(bytes_ostream& out, size_t n) {
        while (n) {
            auto v = next(n);
            out.write(v);
            n -= v.size();
        }
    }

    void skip(size_t n) {
        while (n) {
            n -= next(n).size();
        }
    }
};

static void append_to(bytes_ostream& out, std::span<const char> s) {
    out.write(s.data(), s.size());
}

// Checks the sizes before anything is allocated for the output.
static void check_page_sizes(compression_codec codec, byte_array_view in, size_t uncompressed_size) {
    if (uncompressed_size > max_page_size || in.size_bytes() > max_page_size) {
        malformed(codec, seastar::format("page of {} bytes, {} uncompressed, is too large", in.size_bytes(), uncompressed_size));
    }
}

// Runs a job: an object whose step() does a bounded amount of work and
// returns true when the job is done.
template <typename Job>
static void run_sync(Job& job) {
    while (!job.step()) {
    }
}

template <typename Job>
static future<> run_async(Job& job) {
    while (!job.step()) {
        co_await coroutine::maybe_yield();
    }
}

// The base of the codecs. It runs a small call synchronously and a large
// one asynchronously (see the top of compression.hh).
class codec_base : public page_compressor {
protected:
    virtual void compress_sync(const bytes_ostream& in, bytes_ostream& out) = 0;
    virtual future<> compress_async(const bytes_ostream& in, bytes_ostream& out) = 0;
    virtual fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) = 0;
    virtual future<fragmented_temporary_buffer> decompress_async(byte_array_view in, size_t uncompressed_size) = 0;
public:
    future<> compress(size_t, const bytes_ostream& in, bytes_ostream& out) final {
        try {
            if (in.size() > max_page_size) {
                on_internal_error(compression_logger, seastar::format("a page of {} bytes is too large", in.size()));
            }
            if (in.size() <= sync_compress_threshold) {
                compress_sync(in, out);
                return make_ready_future<>();
            }
        } catch (...) {
            return current_exception_as_future();
        }
        return compress_async(in, out);
    }

    future<fragmented_temporary_buffer> decompress(size_t, byte_array_view in, size_t uncompressed_size) final {
        try {
            check_page_sizes(codec(), in, uncompressed_size);
            if (in.size_bytes() <= sync_decompress_threshold && uncompressed_size <= sync_decompress_threshold) {
                return make_ready_future<fragmented_temporary_buffer>(decompress_sync(in, uncompressed_size));
            }
        } catch (...) {
            return current_exception_as_future<fragmented_temporary_buffer>();
        }
        return decompress_async(in, uncompressed_size);
    }
};

// A codec whose small and large calls run the same jobs, without a context
// that outlives a step.
template <typename CompressJob, typename DecompressJob>
class stateless_codec : public codec_base {
protected:
    void compress_sync(const bytes_ostream& in, bytes_ostream& out) override {
        CompressJob job(in, out);
        run_sync(job);
    }
    future<> compress_async(const bytes_ostream& in, bytes_ostream& out) override {
        CompressJob job(in, out);
        co_await run_async(job);
    }
    future<fragmented_temporary_buffer> decompress_async(byte_array_view in, size_t uncompressed_size) override {
        DecompressJob job(codec(), in, uncompressed_size);
        co_await run_async(job);
        co_return std::move(job).finish();
    }
};

// Contexts of zstd and zlib.
//
// Each is created on first use and kept until shutdown. Their workspaces
// are allocated once (or, for the zstd compression context, again only
// when a higher level needs more), so they are the exception that allows a
// contiguous allocation above 128 KiB. The guards silence the warning for
// these allocations only. A shard has one set of contexts for small calls,
// and one for each scheduling group that makes large calls. A set takes up
// to about 1 MiB for decompression, and 2 MiB for compression at the
// default levels.

// A zstd compression context in a workspace of the size that zstd
// estimates (ZSTD_initStaticCStream()), as zstd_processor in
// sstables/compressor.cc and message/stream_compressor.cc do. A static
// context never allocates. The compression parameters are those that zstd
// picks for an input of one window at the level, with the window of 2^17
// (zstd_cparams()). So a large page does not make zstd pick larger tables.
// The workspace is about 0.9 MiB at level 1, 1.3 MiB at level 3, and
// 3.3 MiB from level 15 up. It is replaced by a larger one when a call uses
// a higher level than the context has used before.
class zstd_cstream {
    std::unique_ptr<char[]> _workspace;
    size_t _size = 0;
    ZSTD_CStream* _ctx = nullptr;
public:
    ZSTD_CStream* get(const ZSTD_compressionParameters& cp) {
        const size_t needed = ZSTD_estimateCStreamSize_usingCParams(cp);
        if (needed > _size) {
            _ctx = nullptr;
            _workspace.reset();
            _size = 0;
            {
                // See above: a context lives until shutdown.
                const memory::scoped_large_allocation_warning_disable guard;
                _workspace = std::make_unique_for_overwrite<char[]>(needed);
            }
            _ctx = ZSTD_initStaticCStream(_workspace.get(), needed);
            if (!_ctx) {
                on_internal_error(compression_logger, "ZSTD_initStaticCStream() failed");
            }
            _size = needed;
        }
        return _ctx;
    }
};

// The window of the frames that we write and read.
static constexpr int zstd_window_log = 17;

// A zstd decompression context in a workspace that fits a window of 2^17,
// about 0.6 MiB.
class zstd_dstream {
    std::unique_ptr<char[]> _workspace;
    ZSTD_DStream* _ctx = nullptr;
public:
    ZSTD_DStream* get() {
        if (!_ctx) {
            const size_t needed = ZSTD_estimateDStreamSize(size_t(1) << zstd_window_log);
            {
                // See above: a context lives until shutdown.
                const memory::scoped_large_allocation_warning_disable guard;
                _workspace = std::make_unique_for_overwrite<char[]>(needed);
            }
            _ctx = ZSTD_initStaticDStream(_workspace.get(), needed);
            if (!_ctx) {
                on_internal_error(compression_logger, "ZSTD_initStaticDStream() failed");
            }
        }
        return _ctx;
    }
};

// windowBits of 15 plus 16 selects the gzip format.
static constexpr int gzip_window_bits = 15 + 16;

// A zlib deflate stream. zlib allocates its state in deflateInit2(), and
// zlib-ng allocates it in one block of about 350 KiB.
class gzip_deflater {
    struct state {
        z_stream zs{};
        int level = 0;
        ~state() { deflateEnd(&zs); }
    };
    std::unique_ptr<state> _s;
public:
    z_stream& get(int level) {
        if (!_s) {
            auto s = std::make_unique<state>();
            int ret;
            {
                // See above: a context lives until shutdown.
                const memory::scoped_large_allocation_warning_disable guard;
                ret = deflateInit2(&s->zs, level, Z_DEFLATED, gzip_window_bits, 8, Z_DEFAULT_STRATEGY);
            }
            if (ret != Z_OK) {
                throw std::runtime_error("zlib deflateInit2() failed");
            }
            s->level = level;
            _s = std::move(s);
        } else {
            deflateReset(&_s->zs);
            if (_s->level != level) {
                if (deflateParams(&_s->zs, level, Z_DEFAULT_STRATEGY) != Z_OK) {
                    throw std::runtime_error("zlib deflateParams() failed");
                }
                _s->level = level;
            }
        }
        return _s->zs;
    }
};

// A zlib inflate stream. Its state and window take about 40 KiB.
class gzip_inflater {
    struct state {
        z_stream zs{};
        ~state() { inflateEnd(&zs); }
    };
    std::unique_ptr<state> _s;
public:
    z_stream& get() {
        if (!_s) {
            auto s = std::make_unique<state>();
            const int ret = inflateInit2(&s->zs, gzip_window_bits);
            if (ret != Z_OK) {
                throw std::runtime_error("zlib inflateInit2() failed");
            }
            _s = std::move(s);
        } else {
            inflateReset(&_s->zs);
        }
        return _s->zs;
    }
};

struct codec_contexts {
    zstd_cstream zstd_compress;
    zstd_dstream zstd_decompress;
    gzip_deflater gzip_compress;
    gzip_inflater gzip_decompress;
};

// The contexts of small calls on this shard. A small call does not yield,
// so it needs no lock.
static codec_contexts& shard_contexts() {
    static thread_local codec_contexts c;
    return c;
}

// A context that large calls hold across preemption points, with the lock
// that serializes them.
template <typename Context>
struct locked {
    semaphore lock{1};
    Context ctx;
};

struct group_contexts {
    locked<zstd_cstream> zstd_compress;
    locked<zstd_dstream> zstd_decompress;
    locked<gzip_deflater> gzip_compress;
    locked<gzip_inflater> gzip_decompress;
};

// The contexts of large calls in the current scheduling group on this
// shard. A large call holds a context until it ends. If the groups shared
// one, a call of a group with a small share would make calls of other
// groups wait, which is a priority inversion. There are at most
// max_scheduling_groups() groups, and an entry is small until a codec of
// its group is used.
static group_contexts& current_group_contexts() {
    static thread_local std::unordered_map<scheduling_group, std::unique_ptr<group_contexts>> m;
    auto& c = m[current_scheduling_group()];
    if (!c) {
        c = std::make_unique<group_contexts>();
    }
    return *c;
}

// UNCOMPRESSED

class uncompressed_compress_job {
    input_reader _in;
    bytes_ostream& _out;
public:
    uncompressed_compress_job(const bytes_ostream& in, bytes_ostream& out) : _in(in), _out(out) {}
    bool step() {
        _in.copy_to(_out, std::min(step_size, _in.remaining()));
        return _in.remaining() == 0;
    }
};

class uncompressed_decompress_job {
    compression_codec _codec;
    byte_reader _in;
    output_pieces _out;
public:
    uncompressed_decompress_job(compression_codec codec, byte_array_view in, size_t uncompressed_size)
            : _codec(codec), _in(in), _out(uncompressed_size) {
        if (in.size_bytes() != uncompressed_size) {
            malformed(codec, seastar::format("the page has {} bytes, the page header declares {}", in.size_bytes(), uncompressed_size));
        }
    }
    bool step() {
        _out.copy_from(_in, std::min<size_t>(step_size, _in.remaining()));
        return _in.remaining() == 0;
    }
    fragmented_temporary_buffer finish() && { return std::move(_out).finish(_codec); }
};

class uncompressed_compressor final : public stateless_codec<uncompressed_compress_job, uncompressed_decompress_job> {
public:
    compression_codec codec() const noexcept override { return compression_codec::uncompressed; }
protected:
    fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) override {
        uncompressed_decompress_job job(codec(), in, uncompressed_size);
        run_sync(job);
        return std::move(job).finish();
    }
};

// ZSTD

// See zstd_cstream.
static ZSTD_compressionParameters zstd_cparams(int level) noexcept {
    auto cp = ZSTD_getCParams(level, size_t(1) << zstd_window_log, 0);
    cp.windowLog = zstd_window_log;
    return cp;
}

static size_t zstd_check(size_t ret) {
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(seastar::format("zstd compression failed: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

// A step consumes at most step_size bytes of input. zstd compresses a block
// of 128 KiB once it has the whole block, so a step compresses at most one
// block.
//
// The strategies slower than dfast (the strategy of the default level 3)
// take several milliseconds for a block of 128 KiB, and tens of
// milliseconds at the highest levels. With them, a step consumes at most
// zstd_slow_step bytes and ends the block with ZSTD_e_flush. The frame
// keeps its window, so later blocks still match earlier data. Smaller
// blocks cost about 1% of the compression ratio.
static constexpr size_t zstd_slow_step = 16 * 1024;

class zstd_compress_job {
    ZSTD_CCtx* _c;
    input_reader _in;
    bytes_ostream& _out;
    size_t _step;
    ZSTD_EndDirective _mode;
public:
    zstd_compress_job(ZSTD_CCtx* c, int level, const ZSTD_compressionParameters& cp, const bytes_ostream& in, bytes_ostream& out)
            : _c(c), _in(in), _out(out)
            , _step(cp.strategy > ZSTD_dfast ? zstd_slow_step : step_size)
            , _mode(cp.strategy > ZSTD_dfast ? ZSTD_e_flush : ZSTD_e_continue) {
        zstd_check(ZSTD_CCtx_reset(c, ZSTD_reset_session_and_parameters));
        zstd_check(ZSTD_CCtx_setParameter(c, ZSTD_c_compressionLevel, level));
        // The parameters override the level's, see zstd_cparams(). zstd
        // lowers the window further for a page smaller than the window.
        zstd_check(ZSTD_CCtx_setCParams(c, cp));
        // The page header has a checksum, so the frame needs none.
        zstd_check(ZSTD_CCtx_setParameter(c, ZSTD_c_checksumFlag, 0));
        zstd_check(ZSTD_CCtx_setPledgedSrcSize(c, in.size()));
    }

    bool step() {
        auto buf = scratch_out().first(step_size);
        if (_in.remaining()) {
            auto v = _in.next(_step);
            ZSTD_inBuffer ib{v.data(), v.size(), 0};
            size_t left;
            do {
                ZSTD_outBuffer ob{buf.data(), buf.size(), 0};
                left = zstd_check(ZSTD_compressStream2(_c, &ob, &ib, _mode));
                append_to(_out, buf.first(ob.pos));
            } while (ib.pos < ib.size || (_mode == ZSTD_e_flush && left));
            return false;
        }
        ZSTD_inBuffer ib{nullptr, 0, 0};
        ZSTD_outBuffer ob{buf.data(), buf.size(), 0};
        const size_t left = zstd_check(ZSTD_compressStream2(_c, &ob, &ib, ZSTD_e_end));
        append_to(_out, buf.first(ob.pos));
        return left == 0;
    }
};

// A step writes at most step_size bytes of output.
class zstd_decompress_job {
    compression_codec _codec;
    ZSTD_DCtx* _d;
    byte_array_view _in;
    ZSTD_inBuffer _ib{nullptr, 0, 0};
    // Zero once a frame is complete and flushed.
    size_t _hint = 1;
    output_pieces _out;
public:
    zstd_decompress_job(compression_codec codec, ZSTD_DCtx* d, byte_array_view in, size_t uncompressed_size)
            : _codec(codec), _d(d), _in(in), _out(uncompressed_size) {
        ZSTD_DCtx_reset(d, ZSTD_reset_session_and_parameters);
        ZSTD_DCtx_setParameter(d, ZSTD_d_windowLogMax, zstd_window_log);
    }

    bool step() {
        while (_ib.pos == _ib.size && !_in.empty()) {
            auto frag = _in.current_fragment();
            _in.remove_current();
            _ib = ZSTD_inBuffer{frag.data(), frag.size(), 0};
        }
        if (_ib.pos == _ib.size && _hint == 0) {
            return true;
        }
        auto space = _out.space();
        space = space.first(std::min(step_size, space.size()));
        ZSTD_outBuffer ob{space.data(), space.size(), 0};
        const size_t in_before = _ib.pos;
        _hint = ZSTD_decompressStream(_d, &ob, &_ib);
        if (ZSTD_isError(_hint)) {
            if (ZSTD_getErrorCode(_hint) == ZSTD_error_frameParameter_windowTooLarge) {
                malformed(_codec, seastar::format("a frame has a window above 2^{} bytes", zstd_window_log));
            }
            malformed(_codec, ZSTD_getErrorName(_hint));
        }
        _out.advance(ob.pos);
        if (_out.overflowed()) {
            malformed(_codec, "decompresses to more than the page header declares");
        }
        // With input left and room for output, zstd always makes progress.
        // No progress therefore means that the input ended inside a frame.
        if (ob.pos == 0 && _ib.pos == in_before) {
            malformed(_codec, "truncated frame");
        }
        return false;
    }

    fragmented_temporary_buffer finish() && { return std::move(_out).finish(_codec); }
};

class zstd_compressor final : public codec_base {
    int _level;
    ZSTD_compressionParameters _cparams;
public:
    explicit zstd_compressor(int level) : _level(level), _cparams(zstd_cparams(level)) {}

    compression_codec codec() const noexcept override { return compression_codec::zstd; }
protected:
    void compress_sync(const bytes_ostream& in, bytes_ostream& out) override {
        zstd_compress_job job(shard_contexts().zstd_compress.get(_cparams), _level, _cparams, in, out);
        run_sync(job);
    }

    future<> compress_async(const bytes_ostream& in, bytes_ostream& out) override {
        auto& c = current_group_contexts().zstd_compress;
        const auto units = co_await get_units(c.lock, 1);
        zstd_compress_job job(c.ctx.get(_cparams), _level, _cparams, in, out);
        co_await run_async(job);
    }

    fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) override {
        zstd_decompress_job job(codec(), shard_contexts().zstd_decompress.get(), in, uncompressed_size);
        run_sync(job);
        return std::move(job).finish();
    }

    future<fragmented_temporary_buffer> decompress_async(byte_array_view in, size_t uncompressed_size) override {
        auto& c = current_group_contexts().zstd_decompress;
        const auto units = co_await get_units(c.lock, 1);
        zstd_decompress_job job(codec(), c.ctx.get(), in, uncompressed_size);
        co_await run_async(job);
        co_return std::move(job).finish();
    }
};

// GZIP

// zlib compresses several times slower than the other codecs, so a step
// consumes at most this much input.
static constexpr size_t gzip_compress_step = 16 * 1024;

class gzip_compress_job {
    z_stream& _zs;
    input_reader _in;
    bytes_ostream& _out;

    // Runs deflate() on `input` until it has consumed it, or with Z_FINISH,
    // until the stream ends or the output buffer is full.
    int run(bytes_view input, int flush) {
        auto buf = scratch_out().first(step_size);
        _zs.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input.data()));
        _zs.avail_in = input.size();
        int ret;
        do {
            _zs.next_out = reinterpret_cast<Bytef*>(buf.data());
            _zs.avail_out = buf.size();
            ret = deflate(&_zs, flush);
            if (ret == Z_STREAM_ERROR) {
                throw std::runtime_error("zlib deflate() failed");
            }
            append_to(_out, buf.first(buf.size() - _zs.avail_out));
        } while (flush != Z_FINISH && _zs.avail_in > 0);
        return ret;
    }
public:
    gzip_compress_job(z_stream& zs, const bytes_ostream& in, bytes_ostream& out) : _zs(zs), _in(in), _out(out) {}

    bool step() {
        if (_in.remaining()) {
            run(_in.next(gzip_compress_step), Z_NO_FLUSH);
            return false;
        }
        return run(bytes_view(), Z_FINISH) == Z_STREAM_END;
    }
};

// A step writes at most step_size bytes of output.
class gzip_decompress_job {
    compression_codec _codec;
    z_stream& _zs;
    byte_array_view _in;
    output_pieces _out;
    // Set at the end of a gzip member. The specification asks readers to
    // accept pages that hold several members.
    bool _member_end = false;
    bool _any_member = false;
public:
    gzip_decompress_job(compression_codec codec, z_stream& zs, byte_array_view in, size_t uncompressed_size)
            : _codec(codec), _zs(zs), _in(in), _out(uncompressed_size) {
        _zs.next_in = nullptr;
        _zs.avail_in = 0;
    }

    bool step() {
        while (_zs.avail_in == 0 && !_in.empty()) {
            auto frag = _in.current_fragment();
            _in.remove_current();
            _zs.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(frag.data()));
            _zs.avail_in = frag.size();
        }
        if (_zs.avail_in == 0 && _member_end) {
            return true;
        }
        if (_member_end) {
            inflateReset(&_zs);
            _member_end = false;
        }
        auto space = _out.space();
        space = space.first(std::min(step_size, space.size()));
        _zs.next_out = reinterpret_cast<Bytef*>(space.data());
        _zs.avail_out = space.size();
        const auto in_before = _zs.avail_in;
        const int ret = inflate(&_zs, Z_NO_FLUSH);
        const size_t written = space.size() - _zs.avail_out;
        _out.advance(written);
        if (_out.overflowed()) {
            malformed(_codec, "decompresses to more than the page header declares");
        }
        if (ret == Z_STREAM_END) {
            _member_end = true;
            _any_member = true;
        } else if (ret == Z_BUF_ERROR || (ret == Z_OK && written == 0 && _zs.avail_in == in_before)) {
            // No progress is possible: the input ended inside a member.
            malformed(_codec, "truncated stream");
        } else if (ret != Z_OK) {
            malformed(_codec, _zs.msg ? _zs.msg : "inflate() failed");
        }
        return false;
    }

    fragmented_temporary_buffer finish() && {
        if (!_any_member) {
            malformed(_codec, "empty input");
        }
        return std::move(_out).finish(_codec);
    }
};

class gzip_compressor final : public codec_base {
    int _level;
public:
    explicit gzip_compressor(int level) : _level(level) {}

    compression_codec codec() const noexcept override { return compression_codec::gzip; }
protected:
    void compress_sync(const bytes_ostream& in, bytes_ostream& out) override {
        gzip_compress_job job(shard_contexts().gzip_compress.get(_level), in, out);
        run_sync(job);
    }

    future<> compress_async(const bytes_ostream& in, bytes_ostream& out) override {
        auto& c = current_group_contexts().gzip_compress;
        const auto units = co_await get_units(c.lock, 1);
        gzip_compress_job job(c.ctx.get(_level), in, out);
        co_await run_async(job);
    }

    fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) override {
        gzip_decompress_job job(codec(), shard_contexts().gzip_decompress.get(), in, uncompressed_size);
        run_sync(job);
        return std::move(job).finish();
    }

    future<fragmented_temporary_buffer> decompress_async(byte_array_view in, size_t uncompressed_size) override {
        auto& c = current_group_contexts().gzip_decompress;
        const auto units = co_await get_units(c.lock, 1);
        gzip_decompress_job job(codec(), c.ctx.get(), in, uncompressed_size);
        co_await run_async(job);
        co_return std::move(job).finish();
    }
};

// SNAPPY
//
// A stream is the uncompressed length (ULEB128) and a series of elements:
// literals and copies of earlier output. Elements do not depend on how the
// compressor split its input, so streams of separately compressed blocks
// join into one stream by dropping their own lengths.

// A step compresses one block.
class snappy_compress_job {
    input_reader _in;
    bytes_ostream& _out;
public:
    snappy_compress_job(const bytes_ostream& in, bytes_ostream& out) : _in(in), _out(out) {
        write_uleb128(out, in.size());
    }

    bool step() {
        if (!_in.remaining()) {
            return true;
        }
        auto block = _in.take(snappy_block_size, scratch_in());
        auto buf = scratch_out();
        if (snappy::MaxCompressedLength(block.size()) > buf.size()) {
            on_internal_error(compression_logger, "the snappy output buffer is too small");
        }
        size_t n;
        snappy::RawCompress(reinterpret_cast<const char*>(block.data()), block.size(), buf.data(), &n);
        // Drop the block's own length.
        const size_t length_size = uleb128_size(block.size());
        append_to(_out, buf.subspan(length_size, n - length_size));
        return false;
    }
};

// Decodes a stream from untrusted input, for a page that is not small.
// A step writes about step_size bytes of output. Valid data never expands
// more than 64 times (a copy element of 3 bytes produces at most 64 bytes),
// so a step also reads at most a few times step_size bytes of input.
class snappy_decompress_job {
    compression_codec _codec;
    byte_reader _in;
    output_pieces _out;
    // The bytes of the current literal that are still to be copied.
    size_t _literal = 0;

    // Returns `n`, after checking that the output can take it.
    size_t check_length(uint64_t n) {
        if (n > _out.left()) {
            malformed(_codec, "decompresses to more than the page header declares");
        }
        return n;
    }
public:
    snappy_decompress_job(compression_codec codec, byte_array_view in, size_t uncompressed_size)
            : _codec(codec), _in(in), _out(uncompressed_size) {
        const uint64_t declared = _in.read_uleb128();
        if (declared != uncompressed_size) {
            malformed(codec, seastar::format("the stream declares {} bytes, the page header {}", declared, uncompressed_size));
        }
    }

    bool step() {
        size_t budget = step_size;
        while (budget) {
            if (_literal) {
                const size_t n = std::min(_literal, budget);
                _out.copy_from(_in, n);
                _literal -= n;
                budget -= n;
                continue;
            }
            if (!_in.remaining()) {
                return true;
            }
            const uint8_t tag = _in.read_byte();
            size_t length;
            size_t offset;
            switch (tag & 3) {
            case 0:
                length = tag >> 2;
                if (length >= 60) {
                    // The length minus one follows in 1 to 4 bytes.
                    const size_t bytes = length - 59;
                    uint32_t v = 0;
                    for (size_t i = 0; i < bytes; ++i) {
                        v |= uint32_t(_in.read_byte()) << (8 * i);
                    }
                    length = v;
                }
                _literal = check_length(uint64_t(length) + 1);
                _in.need(_literal);
                continue;
            case 1:
                length = ((tag >> 2) & 7) + 4;
                offset = (size_t(tag >> 5) << 8) | _in.read_byte();
                break;
            case 2:
                length = (tag >> 2) + 1;
                offset = _in.read_le<uint16_t>();
                break;
            default:
                length = (tag >> 2) + 1;
                offset = _in.read_le<uint32_t>();
                break;
            }
            if (offset == 0 || offset > _out.produced()) {
                malformed(_codec, seastar::format("a copy at {} reaches {} bytes back", _out.produced(), offset));
            }
            _out.copy_match(offset, check_length(length));
            budget -= std::min(budget, length);
        }
        return false;
    }

    fragmented_temporary_buffer finish() && { return std::move(_out).finish(_codec); }
};

// A snappy::Source over a byte_array_view.
class view_source final : public snappy::Source {
    byte_array_view _v;
public:
    explicit view_source(byte_array_view v) : _v(v) {}
    size_t Available() const override { return _v.size_bytes(); }
    const char* Peek(size_t* len) override {
        auto frag = _v.current_fragment();
        *len = frag.size();
        return reinterpret_cast<const char*>(frag.data());
    }
    void Skip(size_t n) override { _v.remove_prefix(n); }
};

class snappy_compressor final : public stateless_codec<snappy_compress_job, snappy_decompress_job> {
    // A snappy copy element of 3 bytes produces at most 64 bytes, and a
    // literal produces fewer bytes than it takes. Valid data therefore never
    // expands more than 64 times.
    static constexpr size_t max_ratio = 64;
public:
    compression_codec codec() const noexcept override { return compression_codec::snappy; }
protected:
    fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) override {
        // The stream starts with the uncompressed length. Snappy writes at
        // most that many bytes, so it must match the page header.
        byte_reader header(in);
        const uint64_t declared = header.read_uleb128();
        if (declared != uncompressed_size) {
            malformed(codec(), seastar::format("the stream declares {} bytes, the page header {}", declared, uncompressed_size));
        }
        if (uncompressed_size > in.size_bytes() * max_ratio + 64) {
            malformed(codec(), seastar::format("{} bytes cannot decompress to {}", in.size_bytes(), uncompressed_size));
        }
        if (uncompressed_size == 0) {
            if (header.remaining()) {
                malformed(codec(), "data after an empty stream");
            }
            return {};
        }
        temporary_buffer<char> piece(uncompressed_size);
        view_source source(in);
        if (!snappy::RawUncompress(&source, piece.get_write())) {
            malformed(codec(), "corrupt stream");
        }
        std::vector<temporary_buffer<char>> pieces;
        pieces.push_back(std::move(piece));
        return fragmented_temporary_buffer(std::move(pieces), uncompressed_size);
    }
};

// LZ4_RAW
//
// A block is a series of sequences. A sequence is a token, whose high
// nibble is the literal length and whose low nibble is the match length
// minus 4, the literals, a 2-byte offset, and the match. A nibble of 15
// continues in bytes that are added to it, up to one below 255. The last
// sequence has literals only, and ends the block.

// Writes the extra bytes of a length whose nibble is 15. `n` is the length
// minus 15.
static void write_lz4_length(bytes_ostream& out, uint64_t n) {
    static const std::array<char, 256> all_ones = [] {
        std::array<char, 256> a;
        a.fill(char(255));
        return a;
    }();
    while (n >= 255) {
        const uint64_t k = std::min<uint64_t>(n / 255, all_ones.size());
        out.write(all_ones.data(), k);
        n -= k * 255;
    }
    const char last = char(n);
    out.write(&last, 1);
}

static void write_lz4_token(bytes_ostream& out, uint64_t literals, uint8_t match_nibble) {
    const char token = char((std::min<uint64_t>(literals, 15) << 4) | match_nibble);
    out.write(&token, 1);
    if (literals >= 15) {
        write_lz4_length(out, literals - 15);
    }
}

// Compresses a page in chunks of lz4_chunk_size bytes with the library, and
// joins the chunks' blocks into one block. The last sequence of a chunk's
// block has literals only. Its literals become the start of the literals of
// the next chunk's first sequence, which gets a new token. All other
// sequences are copied as they are. Their offsets stay valid, because they
// point into the same chunk. The end of the block follows the rules of the
// format because the last chunk's block does.
//
// The literals that wait for the next chunk are read from the input again,
// so they need no copy. They can be long when the input does not compress.
// A step copies at most step_size bytes of them. If it cannot copy them all,
// it saves the rest of the chunk's block in a buffer of its own, because
// the scratch buffer is not kept across steps.
class lz4_compress_job {
    bytes_ostream& _out;
    // The input of the next chunk.
    input_reader _src;
    uint64_t _src_pos = 0;
    // The input from the start of the literals that wait, at _literals_pos.
    input_reader _literals;
    uint64_t _literals_pos = 0;
    // The length of the literals that wait for a sequence.
    uint64_t _pending = 0;
    // A copy of literals in progress: the bytes left, the output that
    // follows it, and where the next literals start.
    uint64_t _copy = 0;
    std::unique_ptr<char[]> _rest;
    size_t _rest_size = 0;
    uint64_t _next_literals_pos = 0;
    bool _ended = false;

    // Copies up to step_size bytes of the literals in progress, then the
    // output that follows them, from `rest` or from _rest.
    void copy_literals(std::span<const char> rest) {
        const size_t n = std::min<uint64_t>(_copy, step_size);
        _literals.copy_to(_out, n);
        _literals_pos += n;
        _copy -= n;
        if (_copy) {
            if (!rest.empty()) {
                if (!_rest) {
                    _rest = std::make_unique_for_overwrite<char[]>(LZ4_COMPRESSBOUND(lz4_chunk_size));
                }
                std::memcpy(_rest.get(), rest.data(), rest.size());
                _rest_size = rest.size();
            }
            return;
        }
        append_to(_out, rest.empty() ? std::span<const char>(_rest.get(), _rest_size) : rest);
        _rest_size = 0;
        _literals.skip(_next_literals_pos - _literals_pos);
        _literals_pos = _next_literals_pos;
    }

    void compress_chunk() {
        const uint64_t chunk_pos = _src_pos;
        auto chunk = _src.take(lz4_chunk_size, scratch_in());
        _src_pos += chunk.size();
        auto buf = scratch_out();
        const int size = LZ4_compress_fast_extState(lz4_state(), reinterpret_cast<const char*>(chunk.data()), buf.data(),
                chunk.size(), buf.size(), 1);
        if (size <= 0) {
            throw std::runtime_error("LZ4 compression failed");
        }
        const auto* c = reinterpret_cast<const uint8_t*>(buf.data());
        size_t pos = 0;
        auto read_length = [&] (uint64_t nibble) {
            if (nibble == 15) {
                uint8_t b;
                do {
                    b = c[pos++];
                    nibble += b;
                } while (b == 255);
            }
            return nibble;
        };
        // The first sequence.
        uint8_t token = c[pos++];
        const uint64_t first_literals = read_length(token >> 4);
        pos += first_literals;
        if (pos == size_t(size)) {
            // The chunk has no match. Its literals wait for the next chunk.
            _pending += first_literals;
            return;
        }
        const uint8_t first_match_nibble = token & 15;
        const size_t rest_start = pos;
        pos += 2;
        read_length(token & 15);
        // Find the last sequence.
        uint64_t last_literals;
        size_t last;
        for (;;) {
            last = pos;
            token = c[pos++];
            last_literals = read_length(token >> 4);
            pos += last_literals;
            if (pos >= size_t(size)) {
                break;
            }
            pos += 2;
            read_length(token & 15);
        }
        if (pos != size_t(size)) {
            on_internal_error(compression_logger, "LZ4 produced a block that does not end with literals");
        }
        // The first sequence with the waiting literals, then the rest of
        // the chunk's block up to its last sequence, whose literals wait.
        write_lz4_token(_out, _pending + first_literals, first_match_nibble);
        _copy = _pending + first_literals;
        _pending = last_literals;
        _next_literals_pos = chunk_pos + chunk.size() - last_literals;
        copy_literals(std::span<const char>(buf.data() + rest_start, last - rest_start));
    }
public:
    lz4_compress_job(const bytes_ostream& in, bytes_ostream& out) : _out(out), _src(in), _literals(in) {
        if (in.size() > LZ4_MAX_INPUT_SIZE) {
            on_internal_error(compression_logger, seastar::format("LZ4 page of {} bytes is too large", in.size()));
        }
    }

    bool step() {
        if (_copy) {
            copy_literals({});
            return false;
        }
        if (_ended) {
            return true;
        }
        if (_src.remaining()) {
            compress_chunk();
            return false;
        }
        // The last sequence: the waiting literals.
        _ended = true;
        write_lz4_token(_out, _pending, 0);
        _copy = _pending;
        _next_literals_pos = _literals_pos + _pending;
        copy_literals({});
        return _copy == 0;
    }
};

// Decodes a block from untrusted input, for a page that is not small.
// A step writes about step_size bytes of output, and reads at most a few
// times step_size bytes of input.
class lz4_decompress_job {
    compression_codec _codec;
    byte_reader _in;
    output_pieces _out;
    // The parts of the current sequence that are still to be copied.
    size_t _literals = 0;
    size_t _match = 0;
    size_t _offset = 0;
    // Whether the current sequence's offset is still to be read: its
    // literals are copied, and the block did not end after them.
    bool _offset_next = false;
    uint8_t _match_nibble = 0;

    // Reads the extra bytes of a length whose nibble is 15, and checks that
    // the output can take it.
    size_t read_length(uint64_t nibble) {
        uint64_t n = nibble;
        if (nibble == 15) {
            uint8_t b;
            do {
                b = _in.read_byte();
                n += b;
                check_length(n);
            } while (b == 255);
        }
        return check_length(n);
    }

    size_t check_length(uint64_t n) {
        if (n > _out.left()) {
            malformed(_codec, "decompresses to more than the page header declares");
        }
        return n;
    }
public:
    lz4_decompress_job(compression_codec codec, byte_array_view in, size_t uncompressed_size)
            : _codec(codec), _in(in), _out(uncompressed_size) {
        if (in.empty()) {
            malformed(codec, "empty input");
        }
    }

    bool step() {
        size_t budget = step_size;
        while (budget) {
            if (_literals) {
                const size_t n = std::min(_literals, budget);
                _out.copy_from(_in, n);
                _literals -= n;
                budget -= n;
            } else if (_match) {
                const size_t n = std::min(_match, budget);
                _out.copy_match(_offset, n);
                _match -= n;
                budget -= n;
            } else if (_offset_next) {
                if (!_in.remaining()) {
                    // The block ends with the literals of its last sequence.
                    return true;
                }
                _offset_next = false;
                _offset = _in.read_le<uint16_t>();
                if (_offset == 0 || _offset > _out.produced()) {
                    malformed(_codec, seastar::format("a match at {} reaches {} bytes back", _out.produced(), _offset));
                }
                _match = read_length(_match_nibble);
                _match = check_length(uint64_t(_match) + 4);
            } else {
                // A new sequence. A block cannot end with a match.
                if (!_in.remaining()) {
                    malformed(_codec, "the block ends with a match");
                }
                const uint8_t token = _in.read_byte();
                _literals = read_length(token >> 4);
                _in.need(_literals);
                _match_nibble = token & 15;
                _offset_next = true;
                --budget;
            }
        }
        return false;
    }

    fragmented_temporary_buffer finish() && { return std::move(_out).finish(_codec); }
};

class lz4_raw_compressor final : public stateless_codec<lz4_compress_job, lz4_decompress_job> {
    // A sequence of n + 1 bytes (a token and length bytes of 255) decodes
    // to at most 255 * n + 19 bytes, so valid data expands less than 256
    // times. The bound is checked before the output is allocated.
    static constexpr size_t max_ratio = 256;
public:
    compression_codec codec() const noexcept override { return compression_codec::lz4_raw; }
protected:
    // The library decodes a page of up to one contiguous piece from a
    // contiguous input into the page's only piece.
    fragmented_temporary_buffer decompress_sync(byte_array_view in, size_t uncompressed_size) override {
        if (uncompressed_size > in.size_bytes() * max_ratio + 64) {
            malformed(codec(), seastar::format("{} bytes cannot decompress to {}", in.size_bytes(), uncompressed_size));
        }
        const char* src = reinterpret_cast<const char*>(in.current_fragment().data());
        if (in.current_fragment().size() != in.size_bytes()) {
            auto buf = scratch_in();
            byte_reader(in).read(buf.data(), in.size_bytes());
            src = buf.data();
        }
        temporary_buffer<char> piece(uncompressed_size);
        const int n = LZ4_decompress_safe(src, piece.get_write(), in.size_bytes(), uncompressed_size);
        if (n < 0 || size_t(n) != uncompressed_size) {
            malformed(codec(), n < 0 ? "corrupt block" : "block size differs from the page header");
        }
        std::vector<temporary_buffer<char>> pieces;
        if (uncompressed_size) {
            pieces.push_back(std::move(piece));
        }
        return fragmented_temporary_buffer(std::move(pieces), uncompressed_size);
    }
};

std::string_view codec_name(compression_codec codec) noexcept {
    switch (codec) {
    case compression_codec::uncompressed: return "UNCOMPRESSED";
    case compression_codec::snappy: return "SNAPPY";
    case compression_codec::gzip: return "GZIP";
    case compression_codec::lzo: return "LZO";
    case compression_codec::brotli: return "BROTLI";
    case compression_codec::lz4: return "LZ4";
    case compression_codec::zstd: return "ZSTD";
    case compression_codec::lz4_raw: return "LZ4_RAW";
    }
    return "UNKNOWN";
}

bool is_supported(compression_codec codec) noexcept {
    switch (codec) {
    case compression_codec::uncompressed:
    case compression_codec::snappy:
    case compression_codec::gzip:
    case compression_codec::lz4_raw:
    case compression_codec::zstd:
        return true;
    default:
        return false;
    }
}

std::unique_ptr<page_compressor> make_page_compressor(compression_codec codec, std::optional<int> level) {
    switch (codec) {
    case compression_codec::uncompressed:
        return std::make_unique<uncompressed_compressor>();
    case compression_codec::snappy:
        return std::make_unique<snappy_compressor>();
    case compression_codec::gzip:
        return std::make_unique<gzip_compressor>(level.value_or(Z_DEFAULT_COMPRESSION));
    case compression_codec::lz4_raw:
        return std::make_unique<lz4_raw_compressor>();
    case compression_codec::zstd:
        return std::make_unique<zstd_compressor>(level.value_or(ZSTD_defaultCLevel()));
    default:
        on_malformed_data(seastar::format("unsupported compression codec {}", int(codec)));
    }
}

} // namespace sstables::parquet
