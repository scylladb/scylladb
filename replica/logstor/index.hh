/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "types.hh"
#include "utils/bptree.hh"

namespace replica::logstor {

struct index_key_less {
    bool operator()(const index_key& a, const index_key& b) const noexcept {
        return a < b;
    }
};

class log_index_bucket {
    using index_tree = bplus::tree<index_key, index_entry, index_key_less, 16>;

    index_tree _index;

public:
    log_index_bucket() : _index(index_key_less{}) {}

    std::optional<index_entry> get(const index_key& key) const {
        auto it = _index.find(key);
        return it != _index.end() ? std::make_optional(*it) : std::nullopt;
    }

    std::optional<index_entry> exchange(const index_key& key, index_entry new_entry) {
        auto it = _index.find(key);
        std::optional<index_entry> old_entry;

        if (it != _index.end()) {
            old_entry = *it;
            *it = std::move(new_entry);
        } else {
            _index.emplace(std::move(key), std::move(new_entry));
        }

        return old_entry;
    }

    bool update_record_location(const index_key& key, log_location old_location, log_location new_location) {
        auto it = _index.find(key);
        if (it != _index.end()) {
            if (it->location == old_location) {
                it->location = new_location;
                return true;
            }
        }
        return false;
    }

    auto begin() const { return _index.begin(); }
    auto end() const { return _index.end(); }
};

class log_index {

    static constexpr size_t NUM_BUCKETS = 1 << 20; // power of 2

    using bucket_array = std::array<log_index_bucket, NUM_BUCKETS>;

    bucket_array _buckets;

    size_t bucket_index(const index_key& key) const noexcept {
        uint32_t prefix;
        std::memcpy(&prefix, key.digest.data(), sizeof(prefix));
        return static_cast<size_t>(prefix & (NUM_BUCKETS - 1));
    }

    log_index_bucket& get_bucket(const index_key& key) noexcept {
        return _buckets[bucket_index(key)];
    }

    const log_index_bucket& get_bucket(const index_key& key) const noexcept {
        return _buckets[bucket_index(key)];
    }

public:
    std::optional<index_entry> get(const index_key& key) const {
        return get_bucket(key).get(key);
    }

    std::optional<index_entry> exchange(const index_key& key, index_entry new_entry) {
        return get_bucket(key).exchange(key, std::move(new_entry));
    }

    bool update_record_location(const index_key& key, log_location old_location, log_location new_location) {
        return get_bucket(key).update_record_location(key, old_location, new_location);
    }

    class const_iterator {
        using bucket_array = log_index::bucket_array;
        using bucket_iterator = decltype(std::declval<const log_index_bucket>().begin());

        const bucket_array* _buckets;
        size_t _bucket_idx;
        bucket_iterator _bucket_it;

        void advance_to_next_valid() {
            while (_bucket_idx < NUM_BUCKETS) {
                if (_bucket_it != (*_buckets)[_bucket_idx].end()) {
                    return;
                }
                ++_bucket_idx;
                if (_bucket_idx < NUM_BUCKETS) {
                    _bucket_it = (*_buckets)[_bucket_idx].begin();
                }
            }
        }

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = index_entry;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        const_iterator(const bucket_array* buckets, size_t bucket_idx, bucket_iterator bucket_it)
                : _buckets(buckets)
                , _bucket_idx(bucket_idx)
                , _bucket_it(bucket_it) {
            advance_to_next_valid();
        }

        reference operator*() const {
            return *_bucket_it;
        }

        pointer operator->() const {
            return &(*_bucket_it);
        }

        const_iterator& operator++() {
            ++_bucket_it;
            advance_to_next_valid();
            return *this;
        }

        const_iterator operator++(int) {
            const_iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const const_iterator& other) const {
            if (_bucket_idx != other._bucket_idx) {
                return false;
            }
            if (_bucket_idx >= NUM_BUCKETS) {
                return true;
            }
            return _bucket_it == other._bucket_it;
        }

        bool operator!=(const const_iterator& other) const {
            return !(*this == other);
        }
    };

    const_iterator begin() const {
        return const_iterator(&_buckets, 0, _buckets[0].begin());
    }

    const_iterator end() const {
        return const_iterator(&_buckets, NUM_BUCKETS, {});
    }
};

}
