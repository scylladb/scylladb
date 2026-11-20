/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
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
// a "compression_metadata" object, which also contains additional information
// needed from decompression - such as the chunk size and compressor type.
//
// Cassandra supports four different compression algorithms for the chunks,
// LZ4, Snappy, Deflate, and Zstd - the default (and therefore most important) is
// LZ4. Each compressor is an implementation of the "compressor" class.
//
// Each compressed chunk is followed by a 4-byte checksum of the compressed
// data, using the Adler32 or CRC32 algorithm. In Cassandra, there is a parameter
// "crc_check_chance" (defaulting to 1.0) which determines the probability
// of us verifying the checksum of each chunk we read.
//
// This implementation does not cache the compressed disk blocks (which
// are read using O_DIRECT), nor uncompressed data. We intend to cache high-
// level Cassandra rows, not disk blocks.

#include "utils/assert.hh"
#include "utils/div_ceil.hh"
#include <vector>
#include <cstdint>
#include <iterator>
#include <deque>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/fstream.hh>

#include "types/types.hh"
#include "sstables/types.hh"
#include "sstables/version.hh"
#include "sstables/sstable_position.hh"
#include "checksum_utils.hh"

class reader_permit;

class compression_parameters;
class compressor;

namespace sstables {

struct compression {
    // To reduce the memory footpring of compression-info, n offsets are grouped
    // together into segments, where each segment stores a base absolute offset
    // into the file, the other offsets in the segments being relative offsets
    // (and thus of reduced size). Also offsets are allocated only just enough
    // bits to store their maximum value. The offsets are thus packed in a
    // buffer like so:
    //      arrrarrrarrr...
    // where n is 4, a is an absolute offset and r are offsets relative to a.
    // Segments are stored in buckets, where each bucket has its own base offset.
    // Segments in a buckets are optimized to address as large of a chunk of the
    // data as possible for a given chunk size and bucket size.
    //
    // This is not a general purpose container. There are limitations:
    // * Can't be used before init() is called.
    // * at() is best called incrementally, although random lookups are
    // perfectly valid as well.
    // * The iterator and at() can't provide references to the elements.
    // * No point insert is available.
    class segmented_offsets {
    public:
        class state {
            std::size_t _current_index{0};
            std::size_t _current_bucket_index{0};
            uint64_t _current_bucket_segment_index{0};
            uint64_t _current_segment_relative_index{0};
            uint64_t _current_segment_offset_bits{0};

            void update_position_trackers(std::size_t index, uint16_t segment_size_bits,
                uint32_t segments_per_bucket, uint8_t grouped_offsets);

            friend class segmented_offsets;
        };

        class accessor {
            const segmented_offsets& _offsets;
            mutable state _state;
        public:
            accessor(const segmented_offsets& offsets) : _offsets(offsets) { }

            uint64_t at(std::size_t i) const {
                return _offsets.at(i, _state);
            }
        };

        class writer {
            segmented_offsets& _offsets;
            state _state;
        public:
            writer(segmented_offsets& offsets) : _offsets(offsets) { }

            void push_back(uint64_t offset) {
                return _offsets.push_back(offset, _state);
            }
        };

        accessor get_accessor() const {
            return accessor(*this);
        }

        writer get_writer() {
            return writer(*this);
        }
    private:
        // A bucket's storage is accounted for in the shard-wide
        // sstables_stats::compression_offsets_memory counter. The subtraction is
        // done by the deleter, so that it happens on every release path
        // (destruction, assignment over a non-empty container).
        struct bucket_storage_deleter {
            void operator()(char* p) const noexcept;
        };
        using bucket_storage = std::unique_ptr<char[], bucket_storage_deleter>;

        struct bucket {
            uint64_t base_offset;
            bucket_storage storage;
        };

        uint32_t _chunk_size{0};
        uint8_t _segment_base_offset_size_bits{0};
        uint8_t _segmented_offset_size_bits{0};
        uint16_t _segment_size_bits{0};
        uint32_t _segments_per_bucket{0};
        uint8_t _grouped_offsets{0};

        uint64_t _last_written_offset{0};

        std::size_t _size{0};
        std::deque<bucket> _storage;

        uint64_t read(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits) const;
        void write(uint64_t bucket_index, uint64_t offset_bits, uint64_t size_bits, uint64_t value);

        uint64_t at(std::size_t i, state& s) const;
        void push_back(uint64_t offset, state& s);
    public:
        class const_iterator {
        public:
            using iterator_category = std::random_access_iterator_tag;
            using value_type = uint64_t;
            using difference_type = std::ptrdiff_t;
            using pointer = const uint64_t*;
            using reference = const uint64_t&;
        private:
            friend class segmented_offsets;
            struct end_tag {};

            segmented_offsets::accessor _offsets;
            std::size_t _index;

            const_iterator(const segmented_offsets& offsets)
                : _offsets(offsets.get_accessor())
                , _index(0) {
            }

            const_iterator(const segmented_offsets& offsets, end_tag)
                : _offsets(offsets.get_accessor())
                , _index(offsets.size()) {
            }

        public:
            const_iterator(const const_iterator& other) = default;

            const_iterator& operator=(const const_iterator& other) {
                SCYLLA_ASSERT(&_offsets == &other._offsets);
                _index = other._index;
                return *this;
            }

            const_iterator operator++(int) {
                const_iterator it{*this};
                return ++it;
            }

            const_iterator& operator++() {
                *this += 1;
                return *this;
            }

            const_iterator operator+(ssize_t i) const {
                const_iterator it{*this};
                it += i;
                return it;
            }

            const_iterator& operator+=(ssize_t i) {
                _index += i;

                return *this;
            }

            const_iterator operator--(int) {
                const_iterator it{*this};
                return --it;
            }

            const_iterator& operator--() {
                *this -= 1;
                return *this;
            }

            const_iterator operator-(ssize_t i) const {
                const_iterator it{*this};
                it -= i;
                return it;
            }

            const_iterator& operator-=(ssize_t i) {
                _index -= i;
                return *this;
            }

            value_type operator*() const {
                return _offsets.at(_index);
            }

            value_type operator[](ssize_t i) const {
                return _offsets.at(_index + i);
            }

            bool operator==(const const_iterator& other) const {
                return _index == other._index;
            }

            bool operator<(const const_iterator& other) const {
                return _index < other._index;
            }

            bool operator<=(const const_iterator& other) const {

                return _index <= other._index;
            }

            bool operator>(const const_iterator& other) const {
                return _index > other._index;
            }

            bool operator>=(const const_iterator& other) const {
                return _index >= other._index;
            }
        };

        segmented_offsets() = default;

        segmented_offsets(const segmented_offsets&) = delete;
        segmented_offsets& operator=(const segmented_offsets&) = delete;

        segmented_offsets(segmented_offsets&&) = default;
        segmented_offsets& operator=(segmented_offsets&&) = default;

        // Has to be called before using the class. Doing otherwise
        // results in undefined behaviour! Don't call more than once!
        // TODO: fold into constructor, once the parse() et. al. code
        // allows it.
        void init(uint32_t chunk_size);

        uint32_t chunk_size() const noexcept {
            return _chunk_size;
        }

        std::size_t size() const noexcept {
            return _size;
        }

        const_iterator begin() const {
            return const_iterator(*this);
        }

        const_iterator end() const {
            return const_iterator(*this, const_iterator::end_tag{});
        }

        const_iterator cbegin() const {
            return const_iterator(*this);
        }

        const_iterator cend() const {
            return const_iterator(*this, const_iterator::end_tag{});
        }
    };

    disk_string<uint16_t> name;
    disk_array<uint32_t, option> options;
    uint64_t data_len = 0;
    // The offset of each chunk in the compressed data file. Empty for sstables
    // that use physical positions (`mu`): their CompressionInfo.db stores only a
    // sample of the offsets (see sampled_chunk), and their readers don't need
    // the offsets.
    segmented_offsets offsets;

private:
    // Variables *not* found in the "Compression Info" file (added by update()):
    uint64_t _compressed_file_length = 0;
    // chunk_len and _full_checksum are grouped here so the two uint32_t pack
    // into a single 8-byte slot (avoids padding holes). chunk_len is only
    // accessed via uncompressed_chunk_length()/set_uncompressed_chunk_length().
    uint32_t chunk_len = 0;
    uint32_t _full_checksum = 0;
    compressor_ptr _compressor;
    // `mu` only. CompressionInfo.db holds a random sample of the chunk offsets.
    // The writer keeps each chunk with probability 1 / _sampling_denominator,
    // independently of the other chunks. When it writes CompressionInfo.db, it
    // leaves out the chunks near the end of the data (see
    // sampled_chunk_min_data).
    struct sampled_chunk {
        // The offset of the chunk in the compressed data file.
        uint64_t offset;
        // The position of the chunk in the uncompressed data.
        uint64_t uncompressed_start;
    };
    // The sampled chunks, in increasing order. Filled during a write, and
    // freed when CompressionInfo.db is written.
    std::vector<sampled_chunk> _sampled_chunks;
    uint32_t _sampling_denominator = 0;
    // Where the sample starts inside CompressionInfo.db, and how many offsets
    // it has. They let a reader fetch the sample from the file.
    uint64_t _offsets_start_pos = 0;
    uint32_t _sample_size = 0;
public:
    // Set the compressor algorithm, please check the definition of enum compressor.
    void set_compressor(compressor_ptr c);
    compressor& get_compressor() const;
    void discard_hidden_options();
    // After changing _compression, update() must be called to update
    // additional variables depending on it.    
    void update(uint64_t compressed_file_length);
    operator bool() const {
        return !name.value.empty();
    }
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
    chunk_and_offset locate(uint64_t position, const compression::segmented_offsets::accessor& accessor) const;

    unsigned uncompressed_chunk_length() const noexcept {
        return chunk_len;
    }

    void set_uncompressed_chunk_length(uint32_t cl) {
        chunk_len = cl;

        offsets.init(chunk_len);
    }

    uint64_t uncompressed_file_length() const noexcept {
        return data_len;
    }

    void set_uncompressed_file_length(uint64_t fl) {
        data_len = fl;
    }

    uint64_t compressed_file_length() const {
        return _compressed_file_length;
    }
    void set_compressed_file_length(uint64_t compressed_file_length) {
        _compressed_file_length = compressed_file_length;
    }

    uint32_t get_full_checksum() const {
        return _full_checksum;
    }

    void set_full_checksum(uint32_t checksum) {
        _full_checksum = checksum;
    }

    // The number of chunks in the compressed data file. Every chunk except the
    // last holds uncompressed_chunk_length() bytes of uncompressed data.
    uint64_t chunk_count() const noexcept {
        return chunk_len ? div_ceil(data_len, chunk_len) : 0;
    }

    // `mu` only. Adds a chunk to the sample. Chunks must be added in
    // increasing order.
    void add_sampled_chunk(uint64_t offset, uint64_t uncompressed_start) {
        _sampled_chunks.push_back({offset, uncompressed_start});
    }

    // `mu` only. Returns the offsets of the sampled chunks, and frees the
    // sample. Leaves out the chunks that start less than
    // max(sampled_chunk_min_data, uncompressed_chunk_length()) bytes before
    // the end of the uncompressed data. Call it after all the data is written.
    std::vector<uint64_t> take_sampled_offsets();

    // `mu` only. The writer keeps a chunk in the sample with probability
    // 1 / sampling_denominator().
    uint32_t sampling_denominator() const noexcept {
        return _sampling_denominator;
    }

    void set_sampling_denominator(uint32_t d) noexcept {
        _sampling_denominator = d;
    }

    // `mu` only. The position of the sample in CompressionInfo.db.
    uint64_t offsets_start_pos() const noexcept {
        return _offsets_start_pos;
    }

    // `mu` only. The number of offsets in the sample.
    uint32_t sample_size() const noexcept {
        return _sample_size;
    }

    // `mu` only. Records where the sample is in CompressionInfo.db.
    void set_sample_location(uint64_t start_pos, uint32_t size) noexcept {
        _offsets_start_pos = start_pos;
        _sample_size = size;
    }

    friend class sstable;
};

// The maximum length, in bytes, of the compressed data of a chunk whose
// uncompressed length is `uncompressed_chunk_length`. The limit doesn't include
// the checksum, or the header and the footer of a framed chunk.
//
// Rationale: there are some data structures which store compressed chunk lengths
// (in particular: the header of compressed chunk in Data.db
// and the payload of an sstable index entry, for sstables which use physical positions).
// Space is valuable there, so we want to pack this length into as few bits as possible.
//
// Imposing a limit on the post-compression size for a given sstable
// allows us to use a more efficient encoding.
inline uint64_t compressed_chunk_length_limit(uint64_t uncompressed_chunk_length) {
    // This assumption should be met (with a large margin) by any sane compressor and chunk size.
    return uncompressed_chunk_length + uncompressed_chunk_length / 2;
}

// In a physically indexed (`mu`) Data.db, each compressed chunk starts with a
// header and ends with a footer. Both hold the length of the chunk's compressed
// data, as a variable-size integer. See framed_chunk_codec for the encoding.
// On-disk chunk layout:
//
//     [ header ][ compressed data ][ 4-byte checksum ][ footer ]
//
// A reader at the start of a chunk uses the header to find the end of the chunk.
// It uses the footer of the previous chunk (the bytes just before its position)
// to find the start of the previous chunk. Thus, a reader can move in both
// directions without the chunk offsets from CompressionInfo.db.
//
// The 4-byte checksum covers the header and the compressed data, but not the
// footer. Instead, a reader checks that the footer holds the same length as the
// header. A backward reader uses the footer before it can check it. If the
// footer is corrupt, the reader computes a wrong start for the previous chunk,
// and the header it finds there doesn't match the footer, or the data fails the
// checksum. Thus, a reader must not trust a length until it has verified the
// chunk. The decoder also rejects lengths that are too large, so a reader never
// sizes a read from an obviously corrupt length.

using stream_creator_fn = std::function<future<input_stream<char>>(uint64_t, uint64_t, file_input_stream_options)>;

// Note: compression_metadata is passed by reference; The caller is
// responsible for keeping the compression_metadata alive as long as there
// are open streams on it. This should happen naturally on a higher level -
// as long as we have *sstables* work in progress, we need to keep the whole
// sstable alive, and the compression metadata is only a part of it.
input_stream<char> make_compressed_file_k_l_format_input_stream(stream_creator_fn stream_creator,
                sstables::compression* cm, sstable_version_types version, disk_read_range range,
                class file_input_stream_options options, reader_permit permit,
                std::optional<uint32_t> digest);

input_stream<char> make_compressed_file_m_format_input_stream(stream_creator_fn stream_creator,
                sstables::compression* cm, sstable_version_types version, disk_read_range range,
                class file_input_stream_options options, reader_permit permit,
                std::optional<uint32_t> digest);

// Raw compressed data stream function that return compressed chunks without decompression
// while still calculating digests and verifying checksums. Compatible with SSTables version 3.x and later.
input_stream<char> make_compressed_raw_file_input_stream(sstables::stream_creator_fn stream_creator, sstables::compression *cm,
        sstable_version_types version, file_input_stream_options options, reader_permit permit, std::optional<uint32_t> digest);

// Observer invoked after each compressed chunk is written, with the chunk's
// post-compression position, its pre-compression position (the sum of the
// lengths of all prior buffers), its post-compression size and its
// pre-compression (uncompressed) size.
using compressed_chunk_observer = std::function<void(uint64_t post_compression_pos, uint64_t pre_compression_pos, uint64_t size, uint64_t uncompressed_size)>;

// The probability that the writer of a `mu` sstable keeps a chunk offset in the
// sample of CompressionInfo.db is 1 / default_offset_sampling_denominator.
constexpr uint32_t default_offset_sampling_denominator = 64;

// The writer of a `mu` sstable keeps a chunk in the sample of CompressionInfo.db
// only if at least max(sampled_chunk_min_data, chunk length) bytes of
// uncompressed data start at the chunk. Then a sample of up to that many bytes,
// read from the start of a sampled chunk, doesn't run past the end of the data.
// This covers the samples that dictionary training reads: 4 KiB, 16 KiB, or one
// chunk. A reader can't rely on this: the value is a choice of the writer, and
// isn't stored in the file.
constexpr uint64_t sampled_chunk_min_data = 16 * 1024;

// `sampling_seed` seeds the choice of the sampled chunk offsets of a `mu`
// sstable (see compression::add_sampled_chunk()).
output_stream<char> make_compressed_file_m_format_output_stream(output_stream<char> out,
                sstables::compression* cm,
                sstable_version_types version,
                const compression_parameters& cp,
                compressor_ptr,
                uint64_t sampling_seed,
                compressed_chunk_observer observer = {});


std::map<sstring, sstring> options_from_compression(const compression& c);

}
