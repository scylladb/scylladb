/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>

#include "utils/chunked_vector.hh"

// Invertible Bloom lookup table over 64-bit row hashes. Subtracting two
// sketches of equal size and peeling the result recovers the symmetric
// difference of the hashed sets, as long as it is small relative to cells().
struct repair_hash_sketch {
    static constexpr unsigned hash_count = 4;
    static constexpr size_t cell_bytes = sizeof(int16_t) + sizeof(uint64_t) + sizeof(uint32_t);

    // Counts wrap modulo 2^16 by design: only the post-subtraction difference matters.
    utils::chunked_vector<int16_t> counts;
    utils::chunked_vector<uint64_t> key_sums;
    utils::chunked_vector<uint32_t> check_sums;

    // Rounds `cells` up to a non-zero multiple of hash_count.
    static repair_hash_sketch with_cells(size_t cells) {
        cells = std::max<size_t>(hash_count, (cells + hash_count - 1) / hash_count * hash_count);
        return repair_hash_sketch{utils::chunked_vector<int16_t>(cells), utils::chunked_vector<uint64_t>(cells), utils::chunked_vector<uint32_t>(cells)};
    }

    size_t cells() const {
        return counts.size();
    }

    bool well_formed() const {
        return !counts.empty() && counts.size() % hash_count == 0
                && key_sums.size() == counts.size() && check_sums.size() == counts.size();
    }

    void add(uint64_t h) {
        for (unsigned j = 0; j < hash_count; ++j) {
            auto idx = index(h, j);
            counts[idx] += 1;
            key_sums[idx] ^= h;
            check_sums[idx] ^= check(h);
        }
    }

    // Both sketches must have the same cells().
    void subtract(const repair_hash_sketch& o) {
        for (size_t i = 0; i < cells(); ++i) {
            counts[i] -= o.counts[i];
            key_sums[i] ^= o.key_sums[i];
            check_sums[i] ^= o.check_sums[i];
        }
    }

    struct difference {
        utils::chunked_vector<uint64_t> added;   // hashed into this sketch only
        utils::chunked_vector<uint64_t> removed; // hashed into the subtracted sketch only
    };

    // Peels the sketch; nullopt if it cannot be emptied.
    std::optional<difference> decode() && {
        difference d;
        utils::chunked_vector<uint32_t> pure_cells;
        for (size_t i = 0; i < cells(); ++i) {
            if (is_pure(i)) {
                pure_cells.push_back(uint32_t(i));
            }
        }
        while (!pure_cells.empty()) {
            auto i = pure_cells.back();
            pure_cells.pop_back();
            if (!is_pure(i)) {
                continue;
            }
            // A corrupt sketch can keep producing false pure cells; a valid one never peels more keys than cells.
            if (d.added.size() + d.removed.size() == cells()) {
                return std::nullopt;
            }
            auto h = key_sums[i];
            auto c = counts[i];
            (c > 0 ? d.added : d.removed).push_back(h);
            for (unsigned j = 0; j < hash_count; ++j) {
                auto idx = index(h, j);
                counts[idx] -= c;
                key_sums[idx] ^= h;
                check_sums[idx] ^= check(h);
                if (is_pure(idx)) {
                    pure_cells.push_back(uint32_t(idx));
                }
            }
        }
        for (size_t i = 0; i < cells(); ++i) {
            if (counts[i] || key_sums[i] || check_sums[i]) {
                return std::nullopt;
            }
        }
        return d;
    }

private:
    static uint64_t mix(uint64_t x) {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9;
        x ^= x >> 27;
        x *= 0x94d049bb133111eb;
        x ^= x >> 31;
        return x;
    }

    static uint32_t check(uint64_t h) {
        return uint32_t(mix(h ^ 0x5bd1e9955bd1e995) >> 32);
    }

    // Hash j maps into its own sub-table, so a key's cells are always distinct.
    size_t index(uint64_t h, unsigned j) const {
        size_t sub = cells() / hash_count;
        auto r = mix(h + (j + 1) * 0x9e3779b97f4a7c15);
        return j * sub + size_t((unsigned __int128)r * sub >> 64);
    }

    bool is_pure(size_t i) const {
        return (counts[i] == 1 || counts[i] == -1)
                && check_sums[i] == check(key_sums[i])
                && index(key_sums[i], i / (cells() / hash_count)) == i;
    }
};
