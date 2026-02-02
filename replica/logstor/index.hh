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
};

class log_index {

    static constexpr size_t NUM_BUCKETS = 1 << 20; // power of 2

    std::array<log_index_bucket, NUM_BUCKETS> _buckets;

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

};

}
