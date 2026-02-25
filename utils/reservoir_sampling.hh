/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cassert>
#include <random>
#include <cmath>
#include <span>
#include <optional>
#include <cstring>
#include <limits>

namespace utils {

// Selects a random sample of a given size from a stream,
// with uniform probability.
//
// For example: in the below usage, at the beginning of every loop,
// `storage` contains a uniformly random sample (of size `min(sample_size, i)`)
// of the values observed so far.
//
// ```
// value_type generate_value() {...}
//
// const int sample_size = 10;
// auto rs = utils::reservoir_sampler(sample_size);
// std::vector<value_type> storage;
// storage.reserve(sample_size);
//
// for (size_t i = 0; true; ++i) {
//     auto value = generate_value();
//     // rs.next_replace() is the index (in the stream) of the next element
//     // selected for the sample.
//     if (i == rs.next_replace()) {
//         // rs.replace() advances next_replace() and returns the slot
//         // (in the storage) of the replaced element.
//         uint64_t idx = rs.replace();
//         if (idx == storage.size()) {
//             storage.push(std::move(value));
//         } else {
//             storage[idx] = std::move(value);
//         }
//     }
// }
// ```
class reservoir_sampler {
    // The index of the next element picked for the sample.
    uint64_t _next = 0;
    // The max capacity of the sample.
    uint64_t _size;
    // Conceptually:
    // Every element in the stream is associated with a random number in range [0;1].
    // The ones with the lowest numbers are the ones picked for the sample.
    // _w is the greatest random number among the ones currently in the sample.
    double _w = 0;
    // The random number generator.
    // Perhaps it should be passed through the constructor,
    // but currently we only pass the seed to avoid a template.
    std::default_random_engine _rng;

    // Random double in [0;1].
    double random() {
        return std::uniform_real_distribution<double>(0, 1)(_rng);
    }
public:
    reservoir_sampler(uint64_t sample_size, uint64_t random_seed)
        : _size(sample_size)
        , _rng(random_seed)
    {
        // We handle the special case of an empty sample
        // by advancing the selection to infinity.
        if (_size == 0) {
            _next = std::numeric_limits<decltype(_next)>::max();
        }
    }
    // The index of the next element which has been selected into the sample.
    uint64_t next_replace() const noexcept {
        return _next;
    }
    // Returns the slot (in the sample) which should be overwritten by the selected
    // element, and advances the selection.
    //
    // Mustn't be called if sample_size was 0. That would be nonsensical.
    uint64_t replace() {
        assert(_size != 0);
        // The algorithm used below is "Algorithm L"
        // from "Reservoir-sampling algorithms of time complexity O(n(1 + log(N/n)))", Kim-Hung Li 1994
        if (_next < _size) {
            auto retval = _next++;
            if (_next == _size) {
                _w = std::exp(std::log(random()) / _size);
                _next += std::log(random())/std::log(1-_w);
            }
            return retval; 
        }
        _w *= std::exp(std::log(random()) / _size);
        _next += 1 + std::log(random())/std::log(1-_w);
        uint64_t replaced = std::uniform_int_distribution<uint64_t>(0, _size - 1)(_rng);
        return replaced;
    }
};

// Splits a stream of bytes into pages,
// and selects a random sample of them.
//
// For example: in the below usage, at the beginning of every loop,
// `storage` contains a uniformly random sample of the pages
// ingested so far.
//
// ```
// std::span<std::bytes> generate_some_bytes() {...}
//
// const int pages_in_sample = 10;
// const int bytes_in_page = 4096;
//
// auto ps = utils::page_sampler(bytes_in_page, pages_in_sample);
//
// std::vector<std::byte> storage;
// storage.reserve(pages_in_sample * bytes_in_page);
//
// while (true) {
//     auto value = generate_some_bytes();
//     while (value.size()) {
//         if (auto cmd = ps.ingest_some(value)) {
//             auto pos = cmd->slot * bytes_in_page;
//             if (pos >= storage.size()) {
//                 storage.resize(pos + bytes_in_page);
//             }
//             memcpy(&storage[pos], cmd->data.data(), cmd->data.size());
//         }
//     }
// }
// ```
class page_sampler {
    // Contents of the next sampled page.
    std::vector<std::byte> _page;
    // Index in the stream of the next sampled page.
    uint64_t _page_idx = -1;
    // How many bytes we have to copy until we finish the next sampled page.
    uint64_t _bytes_to_copy = 0;
    // How many bytes we have to skip until we start the next sampled page.
    uint64_t _bytes_to_skip = 0;
    // Chooses the pages to be sampled.
    reservoir_sampler _rs;

    void move_to_next_page() {
        auto new_idx = _rs.next_replace();
        _bytes_to_skip = (new_idx - _page_idx - 1) * _page.size();
        _bytes_to_copy = _page.size();
        _page_idx = new_idx;
    }
public:
    page_sampler(uint64_t page_size, uint64_t pages_in_sample, uint64_t random_seed)
        : _page(page_size)
        , _rs(pages_in_sample, random_seed)
    {
        move_to_next_page();
    }

    struct replace_cmd {
        // This slot in the sample which should be overwritten with `data`.
        uint64_t slot;
        // Contents of the page selected into the sample.
        // Invalidated by the next call to ingest_some() or the destruction of the page_sampler (obviously).
        std::span<std::byte> data;
    };
    // Processes some bytes from span `x` (and advances the front of `x` beyond
    // the processed bytes).
    // If this completes the next page to be added to the sample, this page is returned.
    std::optional<replace_cmd> ingest_some(std::span<const std::byte>& x) {
        if (_bytes_to_skip) [[likely]] {
            auto n = std::min(_bytes_to_skip, x.size());
            _bytes_to_skip -= n;
            x = x.subspan(n);
            return std::nullopt;
        } else {
            auto n = std::min(_bytes_to_copy, x.size());
            std::memcpy(_page.data() + (_page.size() - _bytes_to_copy), x.data(), n);
            _bytes_to_copy -= n;
            x = x.subspan(n);
            if (_bytes_to_copy == 0) {
                uint64_t replaced_slot = _rs.replace();
                move_to_next_page();
                return replace_cmd{replaced_slot, _page};
            } else {
                return std::nullopt;
            }
        }
    }
};

} // namespace utils
