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
//
// Sketches nest: cell i of a sub-table of a sketch with s times as many cells
// holds hashes that land in cell i / s of the smaller one, so the larger sketch
// folds into the smaller by adding each run of s cells, and a receiver that
// already has the smaller sketch can rebuild the larger from all but one cell
// of each run (an extension).
struct repair_hash_sketch_extension;
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

    // Folds into a sketch of `cells` cells; cells() must be a multiple of it.
    repair_hash_sketch fold(size_t cells) const {
        auto folded = with_cells(cells);
        const size_t sub = this->cells() / hash_count;
        const size_t fsub = folded.cells() / hash_count;
        const size_t stride = sub / fsub;
        for (unsigned j = 0; j < hash_count; ++j) {
            for (size_t i = 0; i < sub; ++i) {
                auto from = j * sub + i;
                auto to = j * fsub + i / stride;
                folded.counts[to] += counts[from];
                folded.key_sums[to] ^= key_sums[from];
                folded.check_sums[to] ^= check_sums[from];
            }
        }
        return folded;
    }

    // The cells of this sketch a receiver holding its fold to `base_cells` cells
    // lacks: all but the last of each run of cells() / base_cells.
    repair_hash_sketch_extension strip(size_t base_cells) const;

    // Rebuilds the sketch `ext` was stripped from, given its fold to base.
    // nullopt if the shapes do not match.
    static std::optional<repair_hash_sketch> extended(const repair_hash_sketch& base, const repair_hash_sketch_extension& ext);

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

struct repair_hash_sketch_extension {
    uint32_t stride;              // cells of the extended sketch over the base's
    repair_hash_sketch partial;   // (stride - 1) / stride of its cells, in order
};

inline repair_hash_sketch_extension repair_hash_sketch::strip(size_t base_cells) const {
    const size_t sub = cells() / hash_count;
    const size_t bsub = base_cells / hash_count;
    const size_t stride = sub / bsub;
    auto ext = repair_hash_sketch_extension{uint32_t(stride), with_cells(cells() - base_cells)};
    size_t o = 0;
    for (unsigned j = 0; j < hash_count; ++j) {
        for (size_t i = 0; i < sub; ++i) {
            if (i % stride + 1 == stride) {
                continue;
            }
            auto from = j * sub + i;
            ext.partial.counts[o] = counts[from];
            ext.partial.key_sums[o] = key_sums[from];
            ext.partial.check_sums[o] = check_sums[from];
            ++o;
        }
    }
    return ext;
}

inline std::optional<repair_hash_sketch> repair_hash_sketch::extended(const repair_hash_sketch& base, const repair_hash_sketch_extension& ext) {
    if (ext.stride < 2 || !base.well_formed() || !ext.partial.well_formed()
            || ext.partial.cells() != base.cells() * (ext.stride - 1)) {
        return std::nullopt;
    }
    const size_t stride = ext.stride;
    const size_t bsub = base.cells() / hash_count;
    const size_t sub = bsub * stride;
    auto s = with_cells(base.cells() * stride);
    size_t o = 0;
    for (unsigned j = 0; j < hash_count; ++j) {
        for (size_t g = 0; g < bsub; ++g) {
            auto last = j * sub + g * stride + stride - 1;
            s.counts[last] = base.counts[j * bsub + g];
            s.key_sums[last] = base.key_sums[j * bsub + g];
            s.check_sums[last] = base.check_sums[j * bsub + g];
            for (size_t t = 0; t + 1 < stride; ++t, ++o) {
                auto i = j * sub + g * stride + t;
                s.counts[i] = ext.partial.counts[o];
                s.key_sums[i] = ext.partial.key_sums[o];
                s.check_sums[i] = ext.partial.check_sums[o];
                s.counts[last] -= s.counts[i];
                s.key_sums[last] ^= s.key_sums[i];
                s.check_sums[last] ^= s.check_sums[i];
            }
        }
    }
    return s;
}
