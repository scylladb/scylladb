/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

#include <seastar/core/byteorder.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "sstables/checksum_utils.hh"
#include "sstables/version.hh"

class compressor;

namespace sstables {

// The size of the largest header or footer of a framed chunk. A frame is an
// unsigned vint, and the largest valid compressed length (that of a 128 KiB
// chunk) takes 3 bytes.
constexpr uint32_t max_chunk_frame_len = 3;

// A decoded header or footer.
struct chunk_frame {
    // The length of the compressed data of the chunk.
    uint64_t compressed_len;
    // The size of the frame, in bytes.
    uint32_t len;
};

// The parts of one compressed chunk of Data.db. The view of the compressed data
// points into a buffer that the caller owns. The frames are copied into the
// struct.
//
// Unframed chunk (every format except `mu`):
//
//     [ compressed data ][ 4-byte checksum ]
//
// Framed chunk (`mu`):
//
//     [ header ][ compressed data ][ 4-byte checksum ][ footer ]
//
// See framed_chunk_codec for the encoding of the header and the footer. The
// per-chunk checksum covers the header and the compressed data. The
// whole-file digest covers every byte of the file.
struct framed_chunk {
    // The header and the footer, as stored in Data.db. Only their first
    // frame_len bytes are used.
    std::array<char, max_chunk_frame_len> header_bytes{};
    std::array<char, max_chunk_frame_len> footer_bytes{};
    // 0 for an unframed chunk.
    uint32_t frame_len = 0;
    std::string_view compressed;
    uint32_t stored_checksum;

    // Views of the frames. They point into this struct.
    std::string_view header() const noexcept {
        return std::string_view(header_bytes.data(), frame_len);
    }
    std::string_view footer() const noexcept {
        return std::string_view(footer_bytes.data(), frame_len);
    }

    // The size of the chunk in Data.db.
    uint64_t extent() const noexcept {
        return 2 * frame_len + compressed.size() + sizeof(uint32_t);
    }

    template <ChecksumUtils Checksum>
    uint32_t compute_checksum() const {
        if (!frame_len) {
            return Checksum::checksum(compressed.data(), compressed.size());
        }
        auto c = Checksum::checksum(header_bytes.data(), frame_len);
        return Checksum::checksum(c, compressed.data(), compressed.size());
    }

    // Returns the checksum of the chunk. Throws malformed_sstable_exception if
    // it differs from the stored checksum. `file_offset` is the position of the
    // chunk in Data.db; it is used only in the error message.
    template <ChecksumUtils Checksum>
    uint32_t verify_checksum(uint64_t file_offset) const {
        auto actual = compute_checksum<Checksum>();
        if (actual != stored_checksum) {
            throw_checksum_mismatch(file_offset, actual);
        }
        return actual;
    }

    // Adds the chunk to the whole-file digest `digest`, in on-disk order.
    // `checksum` is the chunk's checksum, as returned by compute_checksum().
    // `include_checksum` tells whether the digest covers the per-chunk
    // checksums. It is true for `mc` and later formats.
    template <ChecksumUtils Checksum>
    uint32_t fold_into_digest(uint32_t digest, uint32_t checksum, bool include_checksum) const {
        if constexpr (Checksum::prefer_combine()) {
            digest = Checksum::checksum_combine(digest, checksum, frame_len + compressed.size());
        } else {
            // zlib's adler32() returns its initial value for a null buffer, so
            // never pass it the empty header.
            if (frame_len) {
                digest = Checksum::checksum(digest, header_bytes.data(), frame_len);
            }
            digest = Checksum::checksum(digest, compressed.data(), compressed.size());
        }
        if (include_checksum) {
            uint32_t be_checksum = cpu_to_be(checksum);
            digest = Checksum::checksum(digest, reinterpret_cast<const char*>(&be_checksum), sizeof(be_checksum));
        }
        if (frame_len) {
            digest = Checksum::checksum(digest, footer_bytes.data(), frame_len);
        }
        return digest;
    }

private:
    [[noreturn]] void throw_checksum_mismatch(uint64_t file_offset, uint32_t actual) const;
};

// Encodes and decodes the compressed chunks of one Data.db. It knows whether the
// chunks are framed, and how the frames are encoded. It is the only place that
// knows the chunk layout; see framed_chunk.
//
// The header of a framed chunk is the length of its compressed data, as an
// unsigned vint in its shortest encoding. The vint's first byte gives its size.
// The footer holds the bytes of the header in reverse order, so its last byte
// gives its size. Thus, a reader can find the size of a frame from the frame's
// boundary with the start or the end of the chunk. A decoder rejects an encoding
// that isn't the shortest one, so the header is a function of the length. This
// lets a parser that has read only the footer rebuild the header, and the other
// way round.
class framed_chunk_codec {
    uint32_t _uncompressed_chunk_length;
    // The size of the largest frame. 0 for unframed chunks.
    uint32_t _max_frame_len;

    framed_chunk_codec(uint32_t uncompressed_chunk_length, bool framed);
public:
    // The codec of the chunks of an sstable of version `v`.
    framed_chunk_codec(sstable_version_types v, uint32_t uncompressed_chunk_length);

    // The codec of framed chunks, whatever the format version. For tests only.
    // Code that reads an sstable uses sstable::get_chunk_codec(), which follows
    // the version.
    static framed_chunk_codec for_framed_chunks(uint32_t uncompressed_chunk_length);

    bool framed() const noexcept {
        return _max_frame_len != 0;
    }

    // The size of the frames of the chunk with the longest valid compressed
    // data. 0 for unframed chunks.
    uint32_t max_frame_len() const noexcept {
        return _max_frame_len;
    }

    uint32_t uncompressed_chunk_length() const noexcept {
        return _uncompressed_chunk_length;
    }

    // The size of the header and of the footer of a chunk with
    // `compressed_len` bytes of compressed data. 0 for unframed chunks.
    uint32_t frame_len(uint64_t compressed_len) const noexcept;

    // The size in Data.db of a chunk with `compressed_len` bytes of compressed
    // data.
    uint64_t extent(uint64_t compressed_len) const noexcept {
        return 2 * frame_len(compressed_len) + compressed_len + sizeof(uint32_t);
    }

    // Framed chunks only. The size of a header, given its first byte, or of a
    // footer, given its last byte. Throws malformed_sstable_exception if the
    // size is greater than max_frame_len().
    uint32_t frame_len_from_boundary_byte(char b) const;

    // Framed chunks only. Decodes the header at the start of `bytes`. `bytes`
    // can extend past the header. Throws malformed_sstable_exception if `bytes`
    // is shorter than the header, if the length is too large, or if its
    // encoding isn't the shortest one.
    chunk_frame decode_header(std::string_view bytes) const;

    // Framed chunks only. Like decode_header(), but decodes the footer at the
    // end of `bytes`. `bytes` can start before the footer.
    chunk_frame decode_footer(std::string_view bytes) const;

    // Splits a chunk. `chunk` holds exactly one chunk. Throws
    // malformed_sstable_exception if `chunk` is too short, or if a framed
    // chunk's header doesn't match the size of `chunk` or its footer.
    framed_chunk parse(std::string_view chunk) const;

    // Framed chunks only. Splits a chunk whose header holds `compressed_len`,
    // and whose other bytes are at the start of `rest`. `rest` can extend
    // past the end of the chunk. Throws malformed_sstable_exception if `rest`
    // is too short, or if the footer doesn't hold `compressed_len`.
    framed_chunk parse_after_header(uint64_t compressed_len, std::string_view rest) const;

    // Framed chunks only. Splits a chunk whose footer holds `compressed_len`,
    // and whose other bytes are `front`. Throws malformed_sstable_exception if
    // `front` doesn't have the size that `compressed_len` implies, or if the
    // header doesn't hold `compressed_len`.
    framed_chunk parse_before_footer(std::string_view front, uint64_t compressed_len) const;

    // Decompresses the chunk. Doesn't verify its checksum.
    temporary_buffer<char> decompress(const compressor& c, const framed_chunk& chunk) const;

    // Framed chunks only. Writes the header of a chunk with `compressed_len`
    // bytes of compressed data. Writes frame_len(compressed_len) bytes at
    // `dst`.
    void write_header(char* dst, uint64_t compressed_len) const;

    // Framed chunks only. Writes the header and the footer of a chunk that
    // starts at `chunk` and has `compressed_len` bytes of compressed data.
    void write_frames(char* chunk, uint64_t compressed_len) const;
};

// A chunk header that framed_chunk_stream_reader::read_header() read.
struct streamed_chunk_header {
    chunk_frame frame;
    // Whether read_header() consumed the header from the stream. If it
    // didn't, the header is still the next part of the stream.
    bool consumed;
};

// Reads framed chunks from an input_stream: first the header, then the whole
// chunk.
//
// The fast paths are not coroutines. They look at the buffer of the stream
// without consuming it, through input_stream::consume(). consume() calls its
// consumer at once if the stream has buffered data. Otherwise, it reads first,
// and calls the consumer when the read completes, after the fast path has
// returned. So the consumer writes what it sees to this object, not to a local
// variable. Thus, the object must stay alive until each future that it returns
// resolves.
class framed_chunk_stream_reader {
    framed_chunk_codec _codec;
    // What the consumer of the last peek() saw. peek() clears `_peek_seen`
    // before it calls consume().
    bool _peek_seen = false;
    std::string_view _peeked;

    // Looks at the buffer of `in` without consuming it. If the stream has
    // buffered data, sets _peeked and _peek_seen before it returns.
    future<> peek(input_stream<char>& in);
    // The header that the last peek() saw, if it saw the largest possible
    // header. Must be called right after peek(), before the stream changes.
    std::optional<chunk_frame> decode_peeked_header() const;
    future<streamed_chunk_header> read_header_slow(input_stream<char>& in, uint64_t file_offset, future<> peeked);
    future<temporary_buffer<char>> read_chunk_slow(input_stream<char>& in, streamed_chunk_header header);
public:
    explicit framed_chunk_stream_reader(framed_chunk_codec codec) noexcept
        : _codec(codec)
    {}

    // Framed chunks only. Reads the header of the chunk at the current
    // position of `in`.
    //
    // If `in` has buffered at least max_frame_len() bytes, the header is
    // decoded from the buffer, and stays in the stream. Otherwise, the header
    // can cross the end of the buffer. Then the function reads the first byte
    // of the header, which gives its size, and then the rest of it.
    //
    // Throws malformed_sstable_exception if the stream ends before the end of
    // the header, or if the header is invalid. `file_offset` is the position
    // of the chunk in Data.db; it is used only in error messages.
    future<streamed_chunk_header> read_header(input_stream<char>& in, uint64_t file_offset);

    // Framed chunks only. Reads the chunk whose header read_header() returned
    // as `header`. Returns the whole chunk, with its header. Returns a shorter
    // buffer if the stream ends before the end of the chunk.
    future<temporary_buffer<char>> read_chunk(input_stream<char>& in, const streamed_chunk_header& header);
};

} // namespace sstables
