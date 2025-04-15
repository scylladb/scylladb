/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdint>

#include <seastar/core/bitops.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include "types.hh"
#include "exceptions.hh"
#include "checksum_utils.hh"
#include "data_source_types.hh"
#include "checksummed_data_source.hh"

namespace sstables {

extern logging::logger sstlog;

// File data source implementation for SSTables with attached checksum
// data and no compression
template <ChecksumUtils ChecksumType, bool check_digest>
class checksummed_file_data_source_impl : public data_source_impl {
    std::optional<input_stream<char>> _input_stream;
    const checksum& _checksum;
    [[no_unique_address]] digest_members<check_digest> _digests;
    integrity_error_handler _error_handler;
    uint64_t _chunk_size_trailing_zeros;
    uint64_t _file_len;
    uint64_t _underlying_pos;
    uint64_t _pos;
    uint64_t _beg_pos;
    uint64_t _end_pos;
public:
    checksummed_file_data_source_impl(file f, uint64_t file_len,
                const checksum& checksum, uint64_t pos, size_t len,
                file_input_stream_options options,
                std::optional<uint32_t> digest,
                integrity_error_handler error_handler)
            : _checksum(checksum)
            , _error_handler(error_handler)
            , _file_len(file_len)
            , _pos(pos)
            , _beg_pos(pos)
            , _end_pos(pos + len)
    {
        // _beg_pos and _end_pos specify positions in the stream.
        // These are not necessarily aligned on chunk boundaries.
        // To be able to verify the checksums, we need to translate
        // them into a range of chunks that contain the given
        // address range, and open a file input stream to read that
        // range. The _underlying_pos always points to the current
        // chunk-aligned position of the file input stream.
        uint64_t chunk_size = checksum.chunk_size;
        if (chunk_size == 0 || (chunk_size & (chunk_size - 1)) != 0) {
            on_internal_error(sstlog, format("Invalid chunk size: {}", chunk_size));
        }
        _chunk_size_trailing_zeros = count_trailing_zeros(chunk_size);
        if (_pos > _file_len) {
            on_internal_error(sstlog, "attempt to read beyond end");
        }
        if (len == 0 || _pos == _file_len) {
            // Nothing to read
            _end_pos = _pos;
            return;
        }
        if (len > _file_len - _pos) {
            _end_pos = _file_len;
        }
        if constexpr (check_digest) {
            if (!digest) {
                on_internal_error(sstlog, "Requested digest check but no digest was provided.");
            }
            if (_end_pos - _pos < _file_len) {
                sstlog.debug("Checksummed reader cannot calculate digest with partial read: current pos={}, end pos={}, file len={}. Disabling digest check.",
                        _pos, _end_pos, _file_len);
                _digests = {false};
            } else {
                _digests = {true, *digest, ChecksumType::init_checksum()};
            }
        }
        auto start = align_down(_beg_pos, chunk_size);
        auto end = std::min(_file_len, align_up(_end_pos, chunk_size));
        _input_stream = make_file_input_stream(std::move(f), start, end - start, std::move(options));
        _underlying_pos = start;
    }

    virtual future<temporary_buffer<char>> get() override {
        uint64_t chunk_size = _checksum.chunk_size;
        if (_pos >= _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        // Read the next chunk. We need to skip part of the first
        // chunk, but then continue to read from beginning of chunks.
        // Also, we need to take into account that the last chunk can
        // be smaller than `chunk_size`.
        if (_pos != _beg_pos && (_pos & (chunk_size - 1)) != 0) {
            throw std::runtime_error(format("Checksummed reader not aligned to chunk boundary: pos={}, chunk_size={}", _pos, chunk_size));
        }
        return _input_stream->read_exactly(chunk_size).then([this, chunk_size](temporary_buffer<char> buf) {
            uint32_t chunk_index = _pos >> _chunk_size_trailing_zeros;
            if (buf.size() != chunk_size) {
                auto actual_end = _underlying_pos + buf.size();
                if (chunk_index + 1 < _checksum.checksums.size()) {
                    throw malformed_sstable_exception(seastar::format("Checksummed reader hit premature end-of-file at file offset {}: expected {} chunks of size {} but data file has {}",
                            actual_end, _checksum.checksums.size(), chunk_size, chunk_index + 1));
                } else if (actual_end < _file_len) {
                    // Truncation on last chunk. Update _end_pos so that future
                    // calls to get() return immediately.
                    _end_pos = actual_end;
                }
            }
            if (chunk_index >= _checksum.checksums.size()) {
                throw malformed_sstable_exception(seastar::format("Chunk count mismatch between CRC and Data.db: expected {} but data file has more", _checksum.checksums.size()));
            }
            auto expected_checksum = _checksum.checksums[chunk_index];
            auto actual_checksum = ChecksumType::checksum(buf.get(), buf.size());
            if (expected_checksum != actual_checksum) {
                _error_handler(seastar::format(
                        "Checksummed chunk of size {} at file offset {} failed checksum: expected={}, actual={}",
                        buf.size(), _underlying_pos, expected_checksum, actual_checksum));
            }

            if constexpr (check_digest) {
                if (_digests.can_calculate_digest) {
                    _digests.actual_digest = checksum_combine_or_feed<ChecksumType>(_digests.actual_digest, actual_checksum, buf.begin(), buf.size());
                }
            }

            buf.trim_front(_pos & (chunk_size - 1));
            _pos += buf.size();
            _underlying_pos += chunk_size;

            if constexpr (check_digest) {
                if (_digests.can_calculate_digest && _pos == _file_len && _digests.expected_digest != _digests.actual_digest) {
                    _error_handler(seastar::format("Digest mismatch: expected={}, actual={}", _digests.expected_digest, _digests.actual_digest));
                }
            }
            return buf;
        });
    }

    virtual future<> close() override {
        if (!_input_stream) {
            return make_ready_future<>();
        }
        return _input_stream->close();
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        if constexpr (check_digest) {
            if (_digests.can_calculate_digest) {
                sstlog.debug("Checksummed reader cannot calculate digest with skipped data: current pos={}, end pos={}, skip len={}. Disabling digest check.", _pos, _end_pos, n);
                _digests.can_calculate_digest = false;
            }
        }
        uint64_t chunk_size = _checksum.chunk_size;
        if (_pos + n > _end_pos) {
            on_internal_error(sstlog, format("Skipping over the end position is disallowed: current pos={}, end pos={}, skip len={}", _pos, _end_pos, n));
        }
        _pos += n;
        if (_pos == _end_pos) {
            return make_ready_future<temporary_buffer<char>>();
        }
        auto underlying_n = align_down(_pos, chunk_size) - _underlying_pos;
        _beg_pos = _pos;
        _underlying_pos += underlying_n;
        return _input_stream->skip(underlying_n).then([] {
            return make_ready_future<temporary_buffer<char>>();
        });
    }
};

template <ChecksumUtils ChecksumType, bool check_digest>
class checksummed_file_data_source : public data_source {
public:
    checksummed_file_data_source(file f, uint64_t file_len, const checksum& checksum,
            uint64_t offset, size_t len, file_input_stream_options options,
            std::optional<uint32_t> digest, integrity_error_handler error_handler)
        : data_source(std::make_unique<checksummed_file_data_source_impl<ChecksumType, check_digest>>(
                std::move(f), file_len, checksum, offset, len, std::move(options), digest,
                error_handler))
    {}
};

template <ChecksumUtils ChecksumType>
inline input_stream<char> make_checksummed_file_input_stream(
        file f, uint64_t file_len, const checksum& checksum, uint64_t offset,
        size_t len, file_input_stream_options options, std::optional<uint32_t> digest,
        integrity_error_handler error_handler)
{
    if (digest) {
        return input_stream<char>(checksummed_file_data_source<ChecksumType, true>(
            std::move(f), file_len, checksum, offset, len, std::move(options), digest,
            error_handler));
    }
    return input_stream<char>(checksummed_file_data_source<ChecksumType, false>(
        std::move(f), file_len, checksum, offset, len, std::move(options), digest, error_handler));
}

input_stream<char> make_checksummed_file_k_l_format_input_stream(
        file f, uint64_t file_len, const checksum& checksum, uint64_t offset,
        size_t len, file_input_stream_options options, std::optional<uint32_t> digest,
        integrity_error_handler error_handler)
{
    return make_checksummed_file_input_stream<adler32_utils>(std::move(f), file_len,
            checksum, offset, len, std::move(options), digest, error_handler);
}

input_stream<char> make_checksummed_file_m_format_input_stream(
        file f, uint64_t file_len, const checksum& checksum, uint64_t offset,
        size_t len, file_input_stream_options options, std::optional<uint32_t> digest,
        integrity_error_handler error_handler)
{
    return make_checksummed_file_input_stream<crc32_utils>(std::move(f), file_len,
            checksum, offset, len, std::move(options), digest, error_handler);
}

void throwing_integrity_error_handler(sstring msg) {
        throw sstables::malformed_sstable_exception(msg);
};

}
