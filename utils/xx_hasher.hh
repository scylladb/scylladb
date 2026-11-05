/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <array>

#include "bytes_fwd.hh"
#include "utils/serialization.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#include <xxhash.h>
#pragma GCC diagnostic pop

class xx_hasher {
    static constexpr size_t digest_size = 16;
    XXH64_state_t _state;

public:
    explicit xx_hasher(uint64_t seed = 0) noexcept {
        XXH64_reset(&_state, seed);
    }

    void update(const char* ptr, size_t length) noexcept {
        XXH64_update(&_state, ptr, length);
    }

    bytes finalize() {
        bytes digest{bytes::initialized_later(), digest_size};
        serialize_to(digest.begin());
        return digest;
    }

    std::array<uint8_t, digest_size> finalize_array() {
        std::array<uint8_t, digest_size> digest;
        serialize_to(digest.begin());
        return digest;
    }

    uint64_t finalize_uint64() {
        return XXH64_digest(&_state);
    }

private:
    template<typename OutIterator>
    void serialize_to(OutIterator&& out) {
        serialize_int64(out, 0);
        serialize_int64(out, finalize_uint64());
    }
};

// Same digest as xx_hasher. Batches small updates into one XXH64_update call,
// which is cheaper when fields are fed a few bytes at a time.
class buffered_xx_hasher {
public:
    static constexpr size_t buffer_size = 4096;
    static constexpr size_t bypass_size = 256;

private:
    xx_hasher _h;
    std::array<char, buffer_size> _buf;
    size_t _len = 0;

public:
    explicit buffered_xx_hasher(uint64_t seed = 0) noexcept
        : _h(seed) {
    }

    void update(const char* ptr, size_t length) noexcept {
        // Batching only pays for small fields; copying larger ones just doubles memory traffic.
        if (length >= bypass_size) {
            flush();
            _h.update(ptr, length);
            return;
        }
        if (length > _buf.size() - _len) {
            flush();
        }
        std::copy_n(ptr, length, _buf.data() + _len);
        _len += length;
    }

    uint64_t finalize_uint64() {
        flush();
        return _h.finalize_uint64();
    }

    bytes finalize() {
        flush();
        return _h.finalize();
    }

    auto finalize_array() {
        flush();
        return _h.finalize_array();
    }

private:
    void flush() noexcept {
        if (_len) {
            _h.update(_buf.data(), _len);
            _len = 0;
        }
    }
};
