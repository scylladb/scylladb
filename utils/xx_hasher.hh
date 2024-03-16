/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"
#include "utils/serialization.hh"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#include <xxhash.h>
#pragma GCC diagnostic pop

#include <array>

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
