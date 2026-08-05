/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/iostream.hh>

#include "digest_checked_data_source.hh"
#include "utils/log.hh"
#include "sstables/checksum_utils.hh"

namespace sstables {

extern logging::logger sstlog;

class digest_check_data_source_impl : public data_source_impl {
    input_stream<char> _input;
    uint32_t _expected_digest;
    uint32_t _calculated_digest;
    digest_integrity_error_handler _error_handler;
private:
    void update_digest(const temporary_buffer<char>& data) {
        if (!data.empty()) {
            // This check is needed, as crc32_utils::checksum will return 0, if the input pointer is NULL.
            _calculated_digest = crc32_utils::checksum(_calculated_digest, data.get(), data.size());
        }
    }

    void validate_digest() const {
        if (_expected_digest != _calculated_digest) {
            _error_handler(fmt::format("digest mismatch: expected {}, computed {}", _expected_digest, _calculated_digest));
        }
    }
public:
    digest_check_data_source_impl(input_stream<char> source, uint32_t digest, digest_integrity_error_handler error_handler)
        : _input(std::move(source))
        , _expected_digest(digest)
        , _calculated_digest(crc32_utils::init_checksum())
        , _error_handler(error_handler)
    {}

    virtual future<seastar::temporary_buffer<char>> get() override {
        auto ret = co_await _input.read();
        update_digest(ret);

        if (ret.empty()) {
            validate_digest();
        }

        co_return ret;
    }

    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        auto skipped = co_await _input.read_exactly(n);
        update_digest(skipped);

        if (skipped.size() < n) {
            validate_digest();
        }

        co_return temporary_buffer<char>();
    }

    virtual future<> close() override {
        return _input.close();
    }
};

class digest_check_data_source : public data_source {
public:
    digest_check_data_source(input_stream<char> source, uint32_t digest, digest_integrity_error_handler error_handler)
        : data_source(std::make_unique<digest_check_data_source_impl>(std::move(source), digest, std::move(error_handler)))
    {}
};

input_stream<char> make_digest_checked_input_stream(input_stream<char> source, uint32_t digest, digest_integrity_error_handler error_handler) {
    return input_stream<char>(digest_check_data_source{std::move(source), digest, std::move(error_handler)});
}

} // namespace sstables