/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

// This is an implementation of a random-access compressed file compatible
// with Cassandra's org.apache.cassandra.io.compress compressed files.
//
// To allow reasonably-efficient seeking in the compressed file, the file
// is not compressed as a whole, but rather divided into chunks of a known
// size (by default, 64 KB), where each chunk is compressed individually.
// The compressed size of each chunk is different, so for allowing seeking
// to a particular position in the uncompressed data, we need to also know
// the position of each chunk. This offset vector is supplied externally as
// a "compression_metadata" object (in Cassandra, this is read from a
// separate "compression info" file).
//
// Cassandra supports three different compression algorithms for the chunks,
// LZ4, Snappy, and Deflate - the default (and therefore most important) is
// LZ4. Each compressor is an implementation of the "compressor" class.
//
// Each compressed chunk is followed by a 4-byte checksum of the compressed
// data, using the Adler32 algorithm. In Cassandra, there is a parameter
// "crc_check_chance" (defaulting to 1.0) which determines the probability
// of us verifying the checksum of each chunk we read.
//
// This implementation does not cache the compressed disk blocks (which
// are read using O_DIRECT), nor uncompressed data. We intend to cache high-
// level Cassandra rows, not disk blocks.

#include <vector>
#include <cstdint>

#include "core/file.hh"
#include "core/reactor.hh"
#include "core/shared_ptr.hh"

#include "sstables/types.hh"

// An "uncompress_func" is a function which uncompresses the given compressed
// input chunk, and writes the uncompressed data into the given output buffer.
// An exception is thrown if the output buffer is not big enough, but that
// is not expected to happen - in the chunked compression scheme used here,
// we know that the uncompressed data will be exactly chunk_size bytes (or
// smaller for the last chunk).
typedef size_t uncompress_func(const char* input, size_t input_len,
        char* output, size_t output_len);

uncompress_func uncompress_lz4;
uncompress_func uncompress_snappy;
uncompress_func uncompress_deflate;

uint32_t checksum_adler32(const char* input, size_t input_len);

class compression_metadata {
private:
    uint64_t _compressed_file_length;
    uint64_t _data_length;
    // FIXME: There is no real reason why _chunk_offsets needs to be a
    // contiguous vector. It could also be something non-contiguous, like
    // std::dequeue. It also doesn't strictly need to be read up front.
    std::vector<uint64_t> _chunk_offsets;
    uncompress_func *_uncompress = uncompress_lz4;
    unsigned _chunk_length = 65536; // constant size of uncompressed chunk
public:
    // locate() locates in the compressed file the given byte position of
    // the uncompressed data:
    //   1. The byte range containing the appropriate compressed chunk, and
    //   2. the offset into the uncompressed chunk.
    // Note that the last 4 bytes of the returned chunk are not the actual
    // compressed data, but rather the checksum of the compressed data.
    // locate() throws an out-of-range exception if the position is beyond
    // the last chunk.
    struct chunk_and_offset {
        uint64_t chunk_start;
        uint64_t chunk_len; // variable size of compressed chunk
        unsigned offset; // offset into chunk after uncompressing it
    };
    chunk_and_offset locate(uint64_t position) const;

    unsigned uncompressed_chunk_length() const noexcept {
        return _chunk_length;
    }
    size_t uncompress(
            const char* input, size_t input_len,
            char* output, size_t output_len) const {
        return _uncompress(input, input_len, output, output_len);
    }
    // Move constructor from "struct compression". TODO: think if instead of
    // such constructor, we should somehow unify these two types.
    compression_metadata(sstables::compression &&c, uint64_t compressed_file_length);
};

input_stream<char> make_compressed_file_input_stream(
        lw_shared_ptr<file> f, lw_shared_ptr<compression_metadata> cm,
        uint64_t offset = 0);
