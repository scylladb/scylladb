/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Page compression of the Parquet codec (Compression.md of the
// parquet-format repository).
//
// `sstables::compressor` cannot be reused: its LZ4 output starts with a
// 4-byte length and its Deflate output has zlib framing, while Parquet
// needs a raw LZ4 block and gzip framing.
//
// Page buffers are chains of contiguous pieces of at most 128 KiB. No call
// allocates a contiguous buffer above 128 KiB. The only larger allocations
// are the zstd and zlib contexts, which are allocated once and kept until
// shutdown.
//
// Preemption: a small call runs without a preemption point and returns a
// ready future. A compression is small up to sync_compress_threshold bytes
// of input. A decompression is small up to sync_decompress_threshold bytes
// of input and of output. Either takes at most a few hundred microseconds
// at the default levels. A larger call works in steps of at most 64 KiB of
// input or output, with a preemption point between steps. So a page that
// holds a large value does not stall the reactor.
//
// Contexts: zstd and zlib keep state across the steps of a call. Small calls
// use the contexts of the shard, which they never hold across a preemption
// point. Large calls use the contexts of the current scheduling group, on
// the current shard. Each of those has a lock, which a call holds until it
// ends. A large call therefore waits only for large calls of its own
// scheduling group, never for another group or another shard. The calls do
// no I/O and do not wait for reader memory while they hold a lock: the
// caller reserves the memory of the output before the call.
//
// How each compression codec works on chains of contiguous pieces:
//
// - UNCOMPRESSED copies piece by piece.
// - ZSTD and GZIP use the streaming interfaces of libzstd and zlib.
//   ZSTD frames have a window of at most 2^17 bytes. The writer caps the
//   window, and the reader rejects a frame with a larger one. So the
//   decoder's workspace has a fixed size.
// - SNAPPY compresses blocks of 16 KiB with the library, which then needs
//   less than 128 KiB of working memory, and joins the blocks under one
//   length preamble. It decompresses a small page with the library, and a
//   larger one with its own decoder.
// - LZ4_RAW: a page is a single LZ4 block, which the library processes
//   only between contiguous buffers, in one call. Compression runs the
//   library on chunks of 64 KiB and joins their sequences into one block.
//   Decompression uses the library for a small page, with a contiguous
//   scratch buffer of 128 KiB per shard for the input. A larger page goes
//   through its own decoder.

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

#include <seastar/core/future.hh>

#include "bytes_ostream.hh"
#include "sstables/parquet/bit_util.hh"
#include "sstables/parquet/metadata.hh"
#include "utils/fragmented_temporary_buffer.hh"

namespace sstables::parquet {

/// The largest input of a compression that runs synchronously. See the top
/// of this file.
inline constexpr size_t sync_compress_threshold = 32 * 1024;

/// The largest input and output of a decompression that runs
/// synchronously. Compression is several times slower than decompression.
inline constexpr size_t sync_decompress_threshold = 128 * 1024;

/// Compresses and decompresses the pages of a file.
///
/// The methods take the index of the leaf column that the page belongs to.
/// The compression codecs ignore it for now. It is the hook for a later
/// choice of compression codec or of a trained dictionary per column.
class page_compressor {
public:
    virtual ~page_compressor() = default;

    virtual compression_codec codec() const noexcept = 0;

    /// Compresses a page of `column` and appends the result to `out`.
    /// The output may be larger than the input. The column writer then
    /// stores the page uncompressed, as Data Page V2 allows.
    ///
    /// A page of up to sync_compress_threshold bytes is compressed
    /// synchronously, and
    /// the future is ready. A larger one may yield. `in` and `out` must stay
    /// valid and unchanged until the future resolves.
    virtual future<> compress(size_t column, const bytes_ostream& in, bytes_ostream& out) = 0;

    /// Decompresses a page of `column` from untrusted input.
    ///
    /// `uncompressed_size` comes from the page header. The output must have
    /// exactly this size. Larger output is never produced, and any mismatch
    /// or corruption raises the malformed sstable error. The output is a
    /// chain of contiguous pieces of at most 128 KiB.
    ///
    /// If `in` and `uncompressed_size` are both at most
    /// sync_decompress_threshold bytes, the page is decompressed synchronously, and the future is
    /// ready. Otherwise the call may yield. `in` must stay valid until the
    /// future resolves.
    virtual future<fragmented_temporary_buffer> decompress(size_t column, byte_array_view in, size_t uncompressed_size) = 0;
};

/// Whether the reader and the writer support pages compressed with `codec`.
bool is_supported(compression_codec codec) noexcept;

/// The name of a compression codec in the Parquet specification, for
/// example "LZ4_RAW".
std::string_view codec_name(compression_codec codec) noexcept;

/// Creates the compressor of `codec`, one of UNCOMPRESSED, SNAPPY, GZIP,
/// LZ4_RAW, and ZSTD. `level` applies to ZSTD and GZIP, and the library's
/// default is used if it is not set. Raises the malformed sstable error for
/// any other codec, because the codec comes from the file on the read path.
std::unique_ptr<page_compressor> make_page_compressor(compression_codec codec, std::optional<int> level = std::nullopt);

} // namespace sstables::parquet
