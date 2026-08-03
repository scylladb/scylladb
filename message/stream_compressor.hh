/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/rpc/rpc_types.hh>
#include <vector>
#include <memory>
#include <span>

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#define LZ4_STATIC_LINKING_ONLY
#include <lz4.h>

namespace netw {

// The pairs (zstd_dstream, zstd_cstream) and (raw_stream, raw_stream)
// implement a common compressor interface, with similar semantics as the streaming interface of zstd.
// (That's why zstd_cstream and zstd_dstream are only thin wrappers, and why all take ZSTD_inBuffer/ZSTD_outBuffer args).
// The main difference with zstd's interface is that we communicate errors by exception rather than
// by error code.

struct stream_compressor {
    virtual size_t compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) = 0;
    // After compress() throws, the compressor is left in an undefined state.
    // In this state, it mustn't be used for compression.
    // However, reset() can be called to reset the internal state and recycle the compressor.
    virtual void reset() noexcept = 0;
    virtual ~stream_compressor() {}
};

struct stream_decompressor {
    virtual void decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool end_of_frame) = 0;
    // After decompress() throws, the decompressor is left in an undefined state.
    // In this state, it mustn't be used for decompression.
    // However, reset() can be called to reset the internal state and recycle the decompressor.
    virtual void reset() noexcept = 0;
    virtual ~stream_decompressor() {}
};

// Implements a streaming compression interface similar to ZSTD_CStream/ZSTD_DStream,
// but the "compression" is just memcpy.
struct raw_stream final : public stream_compressor, public stream_decompressor {
    static size_t copy(ZSTD_outBuffer* out, ZSTD_inBuffer* in);
    void decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool) override;
    size_t compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) override;
    void reset() noexcept override;
};

// Thin wrapper over ZSTD_DStream.
class zstd_dstream final : public stream_decompressor {
    struct ctx_deleter {
        void operator()(ZSTD_DStream* stream) const noexcept {
            free(stream);
        }
    };
    std::unique_ptr<ZSTD_DStream, ctx_deleter> _ctx;
    const ZSTD_DDict* _dict;
    bool _in_progress = false;
public:
    zstd_dstream();
    void reset() noexcept override;
    void decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool end_of_frame) override;
    // The passed dict must live until it is unset by another set_dict(). (Or until the compressor is destroyed).
    // The pointer can be null, this will unset the current dict.
    void set_dict(const ZSTD_DDict* dict);
};

// Thin wrapper over ZSTD_CStream.
class zstd_cstream final : public stream_compressor {
    struct ctx_deleter {
        void operator()(ZSTD_CStream* stream) const noexcept {
            free(stream);
        }
    };
    std::unique_ptr<ZSTD_CStream, ctx_deleter> _ctx;
    const ZSTD_CDict* _dict;
public:
    zstd_cstream();
    size_t compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) override;
    void reset() noexcept override;
    // The passed dict must live until it is unset by another set_dict(). (Or until the compressor is destroyed).
    // The pointer can be null, this will unset the current dict.
    void set_dict(const ZSTD_CDict* dict);
};

seastar::rpc::snd_buf compress_impl(size_t head_space, const seastar::rpc::snd_buf& data, stream_compressor& compressor, bool end_of_frame, size_t chunk_size);
seastar::rpc::rcv_buf decompress_impl(const seastar::rpc::rcv_buf& data, stream_decompressor& decompressor, bool end_of_frame, size_t chunk_size);
uint32_t crc_impl(const seastar::rpc::snd_buf& data) noexcept;
uint32_t crc_impl(const seastar::rpc::rcv_buf& data) noexcept;

// Size of the history buffer maintained by both sides of the compressed connection.
// Governs the memory usage and effectiveness of streaming LZ4 compression.
// (Note that the decompressor's history buffer is twice this size. See lz4_dstream::_buf).
//
// There is no value in making it greater than 64 kiB, because LZ4 doesn't support
// greater history sizes, at least in the official releases of LZ4.
// But it might be set smaller to reduce memory usage at the cost of lowered
// compression strength.
//
// If LZ4 streaming compression turns out effective, we should make this live-updatable
// and check the effectiveness of various sizes in practice.
// 
// Must be equal on both sides of the connection. Currently this is achieved by making it
// a constant. If we want to make it live-updatable, changes in window size will have to
// be made a part of lz4_cstream's/lz4_dstream's internal protocol.
constexpr size_t max_lz4_window_size = 64 * 1024;

// Implements a streaming compression interface similar to ZSTD_CStream.
class lz4_cstream final : public stream_compressor {
    // A ring buffer with recent stream history.
    //    
    // To implement streaming compression, LZ4 doesn't copy the history to its own buffer.
    // Instead, we maintain our own history buffer, and the LZ4 compressor only stores a view
    // of the most recent contiguous 64 kiB chunk from that buffer.
    // 
    // Thus the "contiguity" during decompression has to match the "contiguity" during compression.
    // That is, for every block, the contiguous sum (up to 64 kiB) of most recent views passed to
    // LZ4_decompress_safe_continue must be at least as long as the contiguous sum (up to 64 kiB)
    // of most recent views passed to LZ4_compress_fast_continue for that same block.
    // 
    // Thus there are some rules/schemes which have to be obeyed when maintaining the history buffer.
    //
    // The legal schemes are listed in the comment of LZ4_decompress_safe_continue() in lz4.h.
    // We use the third one: the decompressor's ring buffer is bigger than this one by at least
    // the maximum block size, which frees the two sides from having to be synchronized.
    // (See the comment on lz4_dstream::_buf for why we don't use the "synchronized" scheme).
    std::vector<char> _buf;
    // The current position in the ringbuffer _buf. New input will be appended at this position.
    size_t _buf_pos = 0;
    // This pair describes the compressed data in `_lz4_scratch`, which is pending output.
    // We have to copy it out before we can compress new data to the scratch buffer.
    size_t _scratch_beg = 0;
    size_t _scratch_end = 0;

    LZ4_stream_t _ctx;
    const LZ4_stream_t* _dict = nullptr;

public:
    lz4_cstream(size_t window_size = max_lz4_window_size);
    void reset() noexcept override;
    void resetFast() noexcept;
    // When new data arrives in `in`, we copy an arbitrary amount of it to `_buf`,
    // (the amount is arbitrary, but it has to fit contiguously in `_buf`),
    // compress the new block from `_buf` to `_lz4_scratch`,
    // then we copy everything from `_lz4_scratch` to `out`.
    // Repeat until `in` is empty.
    size_t compress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, ZSTD_EndDirective end) override;
    // The passed dict must live until it is unset by another set_dict(). (Or until the compressor is destroyed).
    // The pointer can be null, this will unset the current dict.
    void set_dict(const LZ4_stream_t* dict);
};

// Implements a streaming compression interface similar to ZSTD_DStream.
class lz4_dstream final : public stream_decompressor {
    // The decompression-side counterpart of lz4_cstream::_buf. See its comment for the basics.
    //
    // Note that this is *not* a mirror image of the compressor's ring buffer:
    // it's twice as big and block boundaries fall at different offsets within it.
    //
    // The alternative would LZ4's "synchronized" scheme, where the two ring buffers have
    // exactly the same size and are updated in lockstep. But that scheme additionally requires
    // that LZ4_decompress_safe_continue() is told the *exact* decompressed size of each block,
    // and our protocol doesn't carry it. (It only carries compressed sizes).
    //
    // It was a painful lesson that an upper bound isn't good enough.
    // If the bound isn't exact, LZ4 might internally overwrite some bytes which
    // are backreferenced by the next sequence in the stream, which causes corrupt results.
    //
    // So we use the third scheme from the LZ4_decompress_safe_continue() comment instead:
    // this buffer is at least `max block size` bytes bigger than the compressor's,
    // which allows the two sides to be unsynchronized,
    // and makes an upper bound on the block size good enough.
    // The price is a doubled history buffer on the decompressor's side.
    // (Which doesn't matter because we only have one decompressor per shard.
    // It would matter if we e.g. had a decompressor per RPC connection, which we probably
    // aren't going to do).
    std::vector<char> _buf;
    // An upper bound on the decompressed size of a single block, which is what the third scheme
    // above is parameterized by. The compressor never emits a block bigger than its own ring
    // buffer, so its window size is a valid bound.
    size_t _max_block_size;
    // The write position in `_buf`. New input will be decompressed to this offset.
    size_t _buf_end = 0;
    // The read position in `_buf`. The chunk between `_buf_beg` and `_buf_end` is the data
    // that was decompressed, but hasn't been copied to caller's `out` yet.
    // We have to copy it out before we can overwrite it with new decompressed data.
    size_t _buf_beg = 0;
    // The amount of data accumulated in `_lz4_scratch`. Data accumulates in `_lz4_scratch` until
    // a full LZ4 block (with prepended length) is available — only then we can decompress it to `_buf. 
    size_t _scratch_pos = 0;

    LZ4_streamDecode_t _ctx;
    std::span<const std::byte> _dict;

public:
    lz4_dstream(size_t window_size = max_lz4_window_size);
    void reset() noexcept override;
    void decompress(ZSTD_outBuffer* out, ZSTD_inBuffer* in, bool has_more_fragments) override;
    // The passed dict must live until it is unset by another set_dict(). (Or until the decompressor is destroyed).
    // The span can be empty, this will unset the current dict.
    void set_dict(std::span<const std::byte> dict);
};

} // namespace netw
