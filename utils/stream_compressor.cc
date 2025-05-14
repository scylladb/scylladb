/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/stream_compressor.hh"

#include <array>
#include <memory>
#include <bit>
#include <seastar/core/byteorder.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/core/memory.hh>
#include "utils/small_vector.hh"
#include "seastarx.hh"
#include "utils/crc.hh"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

namespace utils {

namespace {

size_t varint_length(uint8_t first_byte) {
    return 1 + std::countr_zero(first_byte);
}

uint32_t varint_decode(uint64_t all_bytes) {
    size_t n_bytes = 1 + std::countr_zero(all_bytes);
    size_t ignored_msb = (64 - 8 * n_bytes);
    return ((all_bytes << ignored_msb) >> ignored_msb) >> n_bytes;
}

std::pair<size_t, uint64_t> varint_encode(uint32_t x) {
    size_t n_bytes = (std::max(std::bit_width(x), 1) + 6) / 7;
    return {n_bytes, ((x << 1) | 1) << (n_bytes - 1)};
}

} // namespace

size_t raw_stream::copy(ZSTD_outBuffer* out, ZSTD_inBuffer* in) {
    size_t n = std::min(out->size - out->pos, in->size - in->pos);
    memcpy((char*)out->dst + out->pos, (const char*)in->src + in->pos, n);
    in->pos += n;
    out->pos += n;
    return 0;
}

void raw_stream::decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool) {
    copy(out, in);
}

size_t raw_stream::compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) {
    return copy(out, in);
}

void raw_stream::reset() noexcept {
}

// Throw if ret is an ZSTD error code.
static void check_zstd(size_t ret, const char* text) {
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(fmt::format("{} error: {}", text, ZSTD_getErrorName(ret)));
    }
}

// IMPORTANT: with this constant, we are hardcoding a max window size in the decoder.
//
// We do this to make the allocator usage by ZSTD more predictable.
// (If you let zstd alloc and free memory on its own,
// it has some "helpful" heuristics which sometimes reallocate the buffers
// even if the context is reused, and we want to avoid that.
// See https://github.com/facebook/zstd/commit/3d523c741be041f17c28e43b89ab6dfcaee281d2)
//
// But the hardcoding means that the decoder won't be able to handle frames with a window
// size bigger than this. (The decoding will return an error that there's not enough memory).
//
// That means that the window size becomes a part of the protocol:
// the compressor mustn't send messages with a greater window size.
//
// If you wish to enlarge the window size for whatever reason,
// you have to ensure that the new version of the compressor isn't talking
// to a decompressor with a window size hardcoded to something too small.
//
// (In case of RPC compression, you can do this by bumping the COMPRESSOR_NAME
// in advanced_rpc_compressor.cc when releasing the new Scylla version).
constexpr size_t ZSTD_WINDOW_SIZE_LOG = 17;

zstd_dstream::zstd_dstream() {
    // IMPORTANT: window size set in the decompressor can't be smaller
    // must be bigger than the window size of the compressor
    // talking to us.
    const auto window_size = 1 << ZSTD_WINDOW_SIZE_LOG;
    const size_t workspace_size = ZSTD_estimateDStreamSize(window_size);
    {
        // zstd needs a large contiguous allocation, it's unavoidable.
        const memory::scoped_large_allocation_warning_threshold slawt{1024*1024+1};
        _ctx.reset(static_cast<ZSTD_DStream*>(malloc(workspace_size)));
    }
    if (!_ctx) {
        throw std::bad_alloc();
    }
    if (!ZSTD_initStaticDStream(_ctx.get(), workspace_size)) {
        throw std::runtime_error("ZSTD_initStaticCStream() failed");
    }

    // IMPORTANT: this must match the compressor.
    check_zstd(ZSTD_DCtx_setParameter(_ctx.get(), ZSTD_d_format, ZSTD_f_zstd1_magicless), "ZSTD_CCtx_setParameter(.., ZSTD_c_format, ZSTD_f_zstd1_magicless)");
}

void zstd_dstream::reset() noexcept {
    ZSTD_DCtx_reset(_ctx.get(), ZSTD_reset_session_only);
    _in_progress = false;
}

void zstd_dstream::decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool end_of_frame) {
    if (!_in_progress && in->pos == in->size) {
        // Without this early return, we would start the decompression of the next frame.
        // ZSTD_decompressStream() would return something positive, and our truncation
        // checking logic could wrongly claim truncation.
        return;
    }
    size_t ret = ZSTD_decompressStream(_ctx.get(), out, in);
    check_zstd(ret, "ZSTD_decompressStream");
    _in_progress = bool(ret);
    if (in->pos == in->size && end_of_frame && _in_progress) {
        // Should never happen in practice, barring a bug/corruption.
        // It's not the compressor's job to verify data integrity,
        // so we could as well not check for this.
        //
        // But we do it as a good practice.
        // To an extent, it keeps the effects of a corruption localized to the corrupted message.
        // This could make some possible debugging easier.
        // (If we didn't check for truncation, a bad message could leave leftovers in the compressor,
        // which would cause weirdness when decompressing the next -- perhaps correct -- message).
        throw std::runtime_error("truncated ZSTD frame");
    }
}
void zstd_dstream::set_dict(const ZSTD_DDict* dict) {
    _dict = dict;
    ZSTD_DCtx_refDDict(_ctx.get(), _dict);
}

zstd_cstream::zstd_cstream() {
    struct params_deleter {
        void operator()(ZSTD_CCtx_params* params) const noexcept {
            ZSTD_freeCCtxParams(params);
        }
    };

    std::unique_ptr<ZSTD_CCtx_params, params_deleter> params(ZSTD_createCCtxParams());
    // For now, we hardcode a 128 kiB window and the lowest compression level here.
    // We don't need more for RPC compression (or rather: we value lower CPU
    // usage over mildly stronger compression).
    auto compression_level = 1;
    check_zstd(ZSTD_CCtxParams_init(params.get(), compression_level), "ZSTD_Cctx_params_init(.., 1)");
    // IMPORTANT: this must match the decompressor.
    check_zstd(ZSTD_CCtxParams_setParameter(params.get(), ZSTD_c_format, ZSTD_f_zstd1_magicless), "ZSTD_CCTxParams_setParameter(.., ZSTD_c_format, ZSTD_f_zstd1_magicless)");
    check_zstd(ZSTD_CCtxParams_setParameter(params.get(), ZSTD_c_contentSizeFlag, 0), "ZSTD_CCTxParams_setParameter(.., ZSTD_c_contentSizeFlag, 0)");
    check_zstd(ZSTD_CCtxParams_setParameter(params.get(), ZSTD_c_checksumFlag, 0), "ZSTD_CCTxParams_setParameter(.., ZSTD_c_checksumFlag, 0)");
    check_zstd(ZSTD_CCtxParams_setParameter(params.get(), ZSTD_c_dictIDFlag, 0), "ZSTD_CCTxParams_setParameter(.., ZSTD_c_dictIDFlag, 0)");
    // IMPORTANT: window size in compressor mustn't be greater than
    // the max window size handlable by the decompressor.
    check_zstd(ZSTD_CCtxParams_setParameter(params.get(), ZSTD_c_windowLog, ZSTD_WINDOW_SIZE_LOG), "ZSTD_CCtx_setParameter(.., ZSTD_c_windowLog, 17)");

    const size_t workspace_size = ZSTD_estimateCStreamSize_usingCCtxParams(params.get());
    {
        // zstd needs a large contiguous allocation, it's unavoidable.
        const memory::scoped_large_allocation_warning_threshold slawt{1024*1024+1};
        _ctx.reset(static_cast<ZSTD_CStream*>(malloc(workspace_size)));
    }
    if (!_ctx) {
        throw std::bad_alloc();
    }
    if (!ZSTD_initStaticCStream(_ctx.get(), workspace_size)) {
        throw std::runtime_error("ZSTD_initStaticCStream() failed");
    }

    check_zstd(ZSTD_CCtx_setParametersUsingCCtxParams(_ctx.get(), params.get()), "ZSTD_CCtx_setParametersUsingCCtxParams(..)");
}

size_t zstd_cstream::compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) {
    size_t ret = ZSTD_compressStream2(_ctx.get(), out, in, end);
    check_zstd(ret, "ZSTD_compressStream2");
    return ret;
}

void zstd_cstream::reset() noexcept {
    ZSTD_CCtx_reset(_ctx.get(), ZSTD_reset_session_only);
}
void zstd_cstream::set_dict(const ZSTD_CDict* dict) {
    _dict = dict;
    ZSTD_CCtx_refCDict(_ctx.get(), _dict);
}

// Used as a intermediary buffer by lz4_cstream and lz4_dstream;
// Since this buffer is shared, lz4_cstream and lz4_dstream must be used synchronously.
// That is, when the processing of a frame (the string of data between `reset()`s) starts,
// it has to be finished before another compressor instance attempts to use it.
//
// If there ever is a need to process multiple frames concurrently, the dependency on this
// buffer has to be eliminated, and each concurrent stream must be given a separate buffer.
//
// This makes for a bad/dangerous API, but it's only used in this file, so it's okay for now.
static thread_local std::array<char, LZ4_COMPRESSBOUND(max_lz4_window_size) + sizeof(uint64_t)> _lz4_scratch;

lz4_cstream::lz4_cstream(size_t window_size)
    : _buf(window_size)
{
    reset();
}

void lz4_cstream::reset() noexcept {
    _scratch_beg = _scratch_end = _buf_pos = 0;
    LZ4_initStream(&_ctx, sizeof(_ctx));
    LZ4_attach_dictionary(&_ctx, _dict);
}

void lz4_cstream::resetFast() noexcept {
    _scratch_beg = _scratch_end = _buf_pos = 0;
    LZ4_resetStream_fast(&_ctx);
    LZ4_attach_dictionary(&_ctx, _dict);
}

// When new data arrives in `in`, we copy an arbitrary amount of it to `_buf`,
// (the amount is arbitrary, but it has to fit contiguously in `_buf`),
// compress the new block from `_buf` to `_lz4_scratch`,
// then we copy everything from `_lz4_scratch` to `out`.
// Repeat until `in` is empty.
size_t lz4_cstream::compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) {
    if (_scratch_end == _scratch_beg && in->size != in->pos) {
        // If we already copied everything we compressed to `out`,
        // and there is still some data in `in`,
        // we are going to compress a new block from `in` to `_lz4_scratch`.

        if (_buf_pos == _buf.size()) {
            // The ring buffer wraps around here.
            _buf_pos = 0;
        }
        // We will compress the biggest prefix of `in` that fits contiguously inside `buf`.
        // In principle, this is slightly suboptimal -- ideally, if `in` is smaller than the contiguous space in `buf`,
        // we should only copy `in` to `buf` and wait with the compressor call until future `in`s
        // fill the contiguous space entirely, or `end` is `ZSTD_e_flush` or `ZSTD_e_end`.
        // But for streaming LZ4 it doesn't really make a difference.
        size_t n = std::min(_buf.size() - _buf_pos, in->size - in->pos);
        // The compressed block mustn't fill the entire ring buffer.
        // It must be at least 1 byte shorter. It's a dumb quirk of lz4. Perhaps it should be called a bug.
        n = std::min(max_lz4_window_size - 1, n);
        std::memcpy(_buf.data() + _buf_pos, static_cast<const char*>(in->src) + in->pos, n);
        in->pos += n;
        // The first header_size bytes contain the length of the compressed block, so we compress to _lz4_scratch.data() + sizeof(uint64_t).
        int x = LZ4_compress_fast_continue(&_ctx, _buf.data() + _buf_pos, _lz4_scratch.data() + sizeof(uint64_t), n, _lz4_scratch.size(), 1);
        if (x < 0) {
            throw std::runtime_error(fmt::format(
                "LZ4_compress_fast_continue failed with negative return value {}. "
                "LZ4 shouldn't fail. This indicates a bug or a corruption.",
                x));
        }
        auto [header_size, header_value] = varint_encode(x);
        _scratch_end = sizeof(uint64_t) + x;
        _scratch_beg = sizeof(uint64_t) - header_size;
        _buf_pos += n;
        // We prepend to every block its compressed length.
        // This is necessary, because LZ4 needs to know it, but it doesn't store it by itself.
        seastar::write_le<uint64_t>(_lz4_scratch.data(), header_value << (8 * (sizeof(uint64_t) - header_size)));
    }
    if (_scratch_end > _scratch_beg) {
        // If we compressed a block to _lz4_scratch, but we still haven't copied it all to `out`,
        // we copy as much as possible to `out`.
        size_t n = std::min(_scratch_end - _scratch_beg, out->size - out->pos);
        std::memcpy(static_cast<char*>(out->dst) + out->pos, _lz4_scratch.data() + _scratch_beg, n);
        _scratch_beg += n;
        out->pos += n;
    }
    if (_scratch_beg == _scratch_end && in->size == in->pos && end == ZSTD_e_end) {
        // If we have no more data in `_lz4_scratch`, there is no more data in `in`, and `end`
        // says that this is the last block in the stream, we reset the stream state.
        resetFast();
        // Note that the we currently don't emit anything to mark end of stream.
        // We rely on the outer layer to notify the decompressor about it manually.
        // This is inconsistent with what zstd's API does. We could fix that for elegance points. 
    };
    // Return a non-zero value if there is still some data lingering in the compressor's
    // internal buffers (`_lz4_scratch`), which has to be copied out.
    // This will prompt the caller to call us again, even if `in` is empty. 
    return _scratch_beg != _scratch_end;
}
// The passed dict must live until it is unset by another set_dict(). (Or until the compressor is destroyed).
// The pointer can be null, this will unset the current dict.
void lz4_cstream::set_dict(const LZ4_stream_t* dict) {
    _dict = dict;
    resetFast();
}

lz4_dstream::lz4_dstream(size_t window_size)
    : _buf(window_size)
{}

void lz4_dstream::reset() noexcept {
    _scratch_pos = _buf_end = _buf_beg = 0;
    LZ4_setStreamDecode(&_ctx, reinterpret_cast<const char*>(_dict.size() ? _dict.data() : nullptr), _dict.size());
}

void lz4_dstream::decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool end_of_frame) {
    if (_buf_beg == _buf_end) {
        // If we have no decompressed data that wasn't yet output,
        // we will decompress a new block.

        if (_buf_end == _buf.size()) {
            // The ring buffer wraps around here.
            _buf_end = _buf_beg = 0;
        }
        // First, we have to ingest the first few bytes of input data, which contain
        // the post-compression size of the following block.
        size_t header_size = 1;
        if (_scratch_pos < header_size) {
            size_t n = std::min(header_size - _scratch_pos, in->size - in->pos);
            std::memcpy(_lz4_scratch.data() + _scratch_pos, static_cast<const char*>(in->src) + in->pos, n);
            _scratch_pos += n;
            in->pos += n;
        }
        if (_scratch_pos >= header_size) {
            header_size = varint_length(static_cast<uint8_t>(_lz4_scratch[0]));
        }
        if (_scratch_pos < header_size) {
            size_t n = std::min(header_size - _scratch_pos, in->size - in->pos);
            std::memcpy(_lz4_scratch.data() + _scratch_pos, static_cast<const char*>(in->src) + in->pos, n);
            _scratch_pos += n;
            in->pos += n;
        }
        // If we know the first header_size bytes, we can read the compressed size and ingest that many bytes,
        // so we have a full compressed block in contiguous memory.
        if (_scratch_pos >= header_size) {
            auto x = varint_decode(seastar::read_le<uint32_t>(_lz4_scratch.data()));
            if (x + header_size > _lz4_scratch.size()) {
                throw std::runtime_error(fmt::format("Oversized LZ4 block: {} bytes.", x + header_size));
            }
            size_t n = std::min(x + header_size - _scratch_pos, in->size - in->pos);
            std::memcpy(_lz4_scratch.data() + _scratch_pos, static_cast<const char*>(in->src) + in->pos, n);
            _scratch_pos += n;
            in->pos += n;
            if (_scratch_pos == x + header_size) {
                // Now the full compressed block is in contiguous memory.
                // We can decompress it to `_buf` and clear `_lz4_scratch`.
                size_t n_buf = _buf.size() - _buf_end;
                int ret = LZ4_decompress_safe_continue(&_ctx, _lz4_scratch.data() + header_size, _buf.data() + _buf_end, x, n_buf);
                if (ret < 0) {
                    throw std::runtime_error(fmt::format("LZ4_decompress_safe_continue failed with negative return value {}. This indicates a corruption of the RPC connection.", ret));
                }
                _buf_end += ret;
                _scratch_pos = 0;
            }
        }
    }
    if (_buf_end > _buf_beg) {
        // If we have some decompressed data that wasn't yet output,
        // we output as much as possible.
        size_t n = std::min(_buf_end - _buf_beg, out->size - out->pos);
        std::memcpy(static_cast<char*>(out->dst) + out->pos, _buf.data() + _buf_beg, n);
        out->pos += n;
        _buf_beg += n;
    }
    if (end_of_frame && _buf_beg == _buf_end && in->size == in->pos) {
        // If there is no data in internal buffers to be output, there is no more data in `in`,
        // and `in` was the last block in the stream, we reset the stream state.
        if (_scratch_pos != 0) {
            throw std::runtime_error(fmt::format("Truncated LZ4 frame."));
        }
        reset();
    }
    // Sanity check.
    assert(_buf_end >= _buf_beg);
}
// The passed dict must live until it is unset by another set_dict(). (Or until the decompressor is destroyed).
// The span can be empty, this will unset the current dict.
void lz4_dstream::set_dict(std::span<const std::byte> dict) {
    _dict = dict;
    reset();
}

rpc::snd_buf compress_impl(size_t head_space, const rpc::snd_buf& data, stream_compressor& compressor, bool end_of_frame, size_t chunk_size) try {
    const auto size = data.size;

    auto src = std::get_if<temporary_buffer<char>>(&data.bufs);
    if (!src) {
        src = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    size_t size_left = size;
    size_t size_compressed = 0;

    ZSTD_inBuffer inbuf = {};
    ZSTD_outBuffer outbuf = {};

    small_vector<temporary_buffer<char>, 16> dst_buffers;

    // Note: we always allocate chunk_size here, then we resize it to fit at the end.
    // Maybe that's a waste of cycles, and we should allocate a buffer that's about as big
    // as the input, and not shrink at the end?
    dst_buffers.emplace_back(std::max<size_t>(head_space, chunk_size));
    outbuf.dst = dst_buffers.back().get_write();
    outbuf.size = dst_buffers.back().size();
    outbuf.pos = head_space;

    // Stream compressors can handle frames of size 0,
    // but when a Zstd compressor is called multiple times (around 10, I think?)
    // with an empty input, it tries to be helpful and emits a "no progress" error.
    //
    // To avoid that, we handle empty frames we special-case empty frames with the `if`
    // below, and we just return an empty buffer as the result,
    // without putting the empty message through the compressor.
    if (size > 0) {
        while (true) {
            if (size_left && inbuf.pos == inbuf.size) {
                size_left -= src->size();
                inbuf.src = src->get();
                inbuf.size = src->size();
                inbuf.pos = 0;
                ++src;
                continue;
            }
            if (outbuf.pos == outbuf.size) {
                size_compressed += outbuf.pos;
                dst_buffers.emplace_back(chunk_size);
                outbuf.dst = dst_buffers.back().get_write();
                outbuf.size = dst_buffers.back().size();
                outbuf.pos = 0;
                continue;
            }
            size_t ret = compressor.compress(&outbuf, &inbuf, size_left ? ZSTD_e_continue : (end_of_frame ? ZSTD_e_end : ZSTD_e_flush));
            if (!size_left // No more input chunks.
                && inbuf.pos == inbuf.size // No more data in the last chunk. 
                && ret == 0 // No data remaining in compressor's internal buffers. 
            ) {
                break;
            }
        }
    }
    size_compressed += outbuf.pos;
    dst_buffers.back().trim(outbuf.pos);

    // In this routine, we always allocate fragments of size chunk_size, even
    // if the message is tiny.
    // Hence, at this point, dst_buffers contains up to (128 kiB) unused memory.
    // When there are many concurrent RPC messages, this waste might add up to
    // a considerable overhead.
    // Let's pay some cycles to shrink the underlying allocation to fit, to
    // avoid any problems with that unseen memory usage.
    dst_buffers.back() = dst_buffers.back().clone();

    if (dst_buffers.size() == 1) {
        return rpc::snd_buf(std::move(dst_buffers.front()));
    }
    return rpc::snd_buf({std::make_move_iterator(dst_buffers.begin()), std::make_move_iterator(dst_buffers.end())}, size_compressed);
} catch (...) {
    compressor.reset();
    throw;
}

template <typename T>
requires std::same_as<T, rpc::rcv_buf> || std::same_as<T, rpc::snd_buf>
uint32_t crc_impl_generic(const T& data) noexcept {
    auto size = data.size;

    auto it = std::get_if<temporary_buffer<char>>(&data.bufs);
    if (!it) {
        it = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    utils::crc32 crc;
    while (size > 0) {
        crc.process(reinterpret_cast<const uint8_t*>(it->get()), it->size());
        size -= it->size();
        ++it;
    }
    return crc.get();
}

uint32_t crc_impl(const rpc::snd_buf& data) noexcept {
    return crc_impl_generic(data);
}

uint32_t crc_impl(const rpc::rcv_buf& data) noexcept {
    return crc_impl_generic(data);
}

rpc::rcv_buf decompress_impl(const seastar::rpc::rcv_buf& data, stream_decompressor& decompressor, bool end_of_frame, size_t chunk_size) try {
    // As a special-case, if we take empty input, we immediately return an empty output,
    // without going through the actual compressors.
    //
    // This is due to a quirk of Zstd, see the matching comment in `decompress_impl`
    // for details.
    if (data.size == 0) {
        return rpc::rcv_buf();
    }

    size_t size_decompressed = 0;

    size_t input_remaining = data.size;
    auto it = std::get_if<temporary_buffer<char>>(&data.bufs);
    if (!it) {
        it = std::get<std::vector<temporary_buffer<char>>>(data.bufs).data();
    }

    small_vector<temporary_buffer<char>, 16> dst_buffers;

    ZSTD_inBuffer inbuf = {};
    ZSTD_outBuffer outbuf = {};
    while (true) {
        if (input_remaining && inbuf.pos == inbuf.size) {
            inbuf.src = it->get();
            inbuf.size = it->size();
            input_remaining -= it->size();
            ++it;
            inbuf.pos = 0;
            continue;
        }
        if (outbuf.pos == outbuf.size) {
            size_decompressed += outbuf.pos;
            dst_buffers.emplace_back(chunk_size);
            outbuf.dst = dst_buffers.back().get_write();
            outbuf.size = dst_buffers.back().size();
            outbuf.pos = 0;
            continue;
        }
        decompressor.decompress(&outbuf, &inbuf, end_of_frame && !input_remaining);
        if (!input_remaining // No more input chunks.
            && inbuf.pos == inbuf.size // No more data in the last input chunk.
            && outbuf.pos < outbuf.size // No more data in decompressor's internal buffers.
        ) {
            break;
        }
    }

    size_decompressed += outbuf.pos;
    if (!dst_buffers.empty()) {
        dst_buffers.back().trim(outbuf.pos);
        // In this routine, we always allocate fragments of size chunk_size, even
        // if the message is tiny.
        // Hence, at this point, dst_buffers contains up to (128 kiB) unused memory.
        // When there are many concurrent RPC messages, this waste might add up to
        // a considerable overhead.
        // Let's pay some cycles to shrink the underlying allocation to fit, to
        // avoid any problems with that unseen memory usage. 

        // We could avoid the CPU cost if we prepended the decompressed size to
        // the message (thus growing the messages by 1-2 bytes) and used that to
        // allocate exactly the right buffer.
        // But I don't know if it's worth the 2 bytes.
        dst_buffers.back() = dst_buffers.back().clone();
    }
    if (dst_buffers.size() == 1) {
        return rpc::rcv_buf(std::move(dst_buffers.front()));
    }
    return rpc::rcv_buf({std::make_move_iterator(dst_buffers.begin()), std::make_move_iterator(dst_buffers.end())}, size_decompressed);
} catch (...) {
    decompressor.reset();
    throw;
}

} // namespace utils
