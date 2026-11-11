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
// sketches with the same number of cells and peeling the result lists the
// symmetric difference of the two hash sets, as long as the difference is
// small relative to the number of cells (about half of it with 4 hash
// functions). A sketch that does not peel still estimates the size of the
// difference it holds from its counts.
//
// Each hash lands in one cell of each of hash_count sub-tables, so its cells
// are always distinct. A cell holds a count, the XOR of the hashes and the XOR
// of a 16-bit check value of each hash; the check value, together with the
// hash mapping to the cell at all, tells a cell with one hash from a
// coincidence of several.
struct repair_hash_sketch {
    static constexpr unsigned hash_count = 4;
    static constexpr size_t cell_bytes = sizeof(int16_t) + sizeof(uint64_t) + sizeof(uint16_t);

    // Counts wrap modulo 2^16 by design: only the difference of two counts is
    // read, and it is small whenever the sketch is of any use.
    utils::chunked_vector<int16_t> counts;
    utils::chunked_vector<uint64_t> key_sums;
    utils::chunked_vector<uint16_t> check_sums;

    // Rounds `cells` up to a non-zero multiple of hash_count.
    static repair_hash_sketch with_cells(size_t cells) {
        cells = std::max<size_t>(hash_count, (cells + hash_count - 1) / hash_count * hash_count);
        return repair_hash_sketch{utils::chunked_vector<int16_t>(cells), utils::chunked_vector<uint64_t>(cells), utils::chunked_vector<uint16_t>(cells)};
    }

    static size_t bytes(size_t cells) {
        return cells * cell_bytes;
    }

    size_t cells() const {
        return counts.size();
    }

    // A sketch from the wire can have any shape; everything below assumes this.
    bool well_formed() const {
        return !counts.empty() && counts.size() % hash_count == 0
                && key_sums.size() == counts.size() && check_sums.size() == counts.size();
    }

    void add(uint64_t h) {
        for (unsigned j = 0; j < hash_count; ++j) {
            auto i = index(h, j);
            counts[i] += 1;
            key_sums[i] ^= h;
            check_sums[i] ^= check(h);
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

    // Unbiased estimate of the size of the symmetric difference held by a
    // subtracted sketch: the centered second moment of the counts over
    // k (1 - k / cells) (Wu et al. 2026). Valid whether or not the sketch
    // peels, for any mix of hashes from either side.
    double estimate_difference() const {
        double sum = 0, squares = 0;
        for (auto c : counts) {
            sum += c;
            squares += double(c) * c;
        }
        double centered = squares - sum * sum / cells();
        return std::max(0.0, centered / (hash_count * (1.0 - double(hash_count) / cells())));
    }

    struct difference {
        utils::chunked_vector<uint64_t> added;   // hashed into this sketch only
        utils::chunked_vector<uint64_t> removed; // hashed into the subtracted sketch only
    };

    // Peels a subtracted sketch; nullopt if it does not empty.
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
            // A damaged sketch can keep producing pure cells; a sound one never
            // peels more hashes than it has cells.
            if (d.added.size() + d.removed.size() == cells()) {
                return std::nullopt;
            }
            auto h = key_sums[i];
            auto c = counts[i];
            (c > 0 ? d.added : d.removed).push_back(h);
            for (unsigned j = 0; j < hash_count; ++j) {
                auto t = index(h, j);
                counts[t] -= c;
                key_sums[t] ^= h;
                check_sums[t] ^= check(h);
                if (is_pure(t)) {
                    pure_cells.push_back(uint32_t(t));
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
    // splitmix64 finalizer
    static uint64_t mix(uint64_t x) {
        x ^= x >> 30;
        x *= 0xbf58476d1ce4e5b9;
        x ^= x >> 27;
        x *= 0x94d049bb133111eb;
        x ^= x >> 31;
        return x;
    }

    static uint16_t check(uint64_t h) {
        return uint16_t(mix(h ^ 0x5bd1e9955bd1e995) >> 48);
    }

    // Cell of hash h in sub-table j.
    size_t index(uint64_t h, unsigned j) const {
        size_t sub = cells() / hash_count;
        auto r = mix(h + (j + 1) * 0x9e3779b97f4a7c15);
        return j * sub + size_t((unsigned __int128)r * sub >> 64);
    }

    // One hash, and that hash maps to this cell.
    bool is_pure(size_t i) const {
        return (counts[i] == 1 || counts[i] == -1)
                && check_sums[i] == check(key_sums[i])
                && index(key_sums[i], i / (cells() / hash_count)) == i;
    }
};
