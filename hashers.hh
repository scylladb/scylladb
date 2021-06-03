/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "bytes.hh"
#include "hashing.hh"

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
