/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "bytes.hh"
#include "utils/hashing.hh"

template<typename H>
concept HasherReturningBytes = HasherReturning<H, bytes>;

class md5_hasher;

template <typename T, size_t size> class cryptopp_hasher : public hasher {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    cryptopp_hasher();
    ~cryptopp_hasher();
    cryptopp_hasher(cryptopp_hasher&&) noexcept;
    cryptopp_hasher(const cryptopp_hasher&);
    cryptopp_hasher& operator=(cryptopp_hasher&&) noexcept;
    cryptopp_hasher& operator=(const cryptopp_hasher&);

    bytes finalize();
    std::array<uint8_t, size> finalize_array();
    void update(const char* ptr, size_t length) noexcept override;

    // Use update and finalize to compute the hash over the full view.
    static bytes calculate(const std::string_view& s);
};

class md5_hasher final : public cryptopp_hasher<md5_hasher, 16> {};

class sha256_hasher final : public cryptopp_hasher<sha256_hasher, 32> {};
