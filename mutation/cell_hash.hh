/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>

#include <seastar/util/optimized_optional.hh>

#include "utils/hashing.hh"

struct cell_hash {
    using size_type = uint64_t;
    static constexpr size_type no_hash = 0;

    size_type hash = no_hash;

    explicit operator bool() const noexcept {
        return hash != no_hash;
    }
};

template<>
struct appending_hash<cell_hash> {
    template<typename Hasher>
    void operator()(Hasher& h, const cell_hash& ch) const {
        feed_hash(h, ch.hash);
    }
};

using cell_hash_opt = seastar::optimized_optional<cell_hash>;
