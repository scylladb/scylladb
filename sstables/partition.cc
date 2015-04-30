/*
 * Copyright 2015 Cloudius Systems
 */
#include "database.hh"
#include "sstables.hh"
#include "types.hh"
#include "core/future-util.hh"
#include "key.hh"

#include "dht/i_partitioner.hh"

namespace sstables {

// Origin uses this slightly modified binary search for the Summary, that will
// indicate in which bucket the element would be in case it is not a match.
//
// For the Index entries, it uses a "normal", java.lang binary search. Because
// we have made the explicit decision to open code the comparator for
// efficiency, using a separate binary search would be possible, but very
// messy.
//
// It's easier to reuse the same code for both binary searches, and just ignore
// the extra information when not needed.
//
// This code should work in all kinds of vectors in whose's elements is possible to aquire
// a key view.
template <typename T>
int sstable::binary_search(const T& entries, const key& sk) {
    int low = 0, mid = entries.size(), high = mid - 1, result = -1;

    auto& partitioner = dht::global_partitioner();
    auto sk_bytes = bytes_view(sk);
    auto token = partitioner.get_token(key_view(sk_bytes));

    while (low <= high) {
        // The token comparison should yield the right result most of the time.
        // So we avoid expensive copying operations that happens at key
        // creation by keeping only a key view, and then manually carrying out
        // both parts of the comparison ourselves.
        mid = low + ((high - low) >> 1);
        auto mid_bytes = bytes_view(entries[mid]);
        auto mid_key = key_view(mid_bytes);
        auto mid_token = partitioner.get_token(mid_key);

        if (token == mid_token) {
            result = compare_unsigned(sk_bytes, mid_bytes);
        } else {
            result = token < mid_token ? -1 : 1;
        }

        if (result > 0) {
            low = mid + 1;
        } else if (result < 0) {
            high = mid - 1;
        } else {
            return mid;
        }
    }

    return -mid - (result < 0 ? 1 : 2);
}

// Force generation, so we make it available outside this compilation unit without moving that
// much code to .hh
template int sstable::binary_search<>(const std::vector<summary_entry>& entries, const key& sk);
template int sstable::binary_search<>(const std::vector<index_entry>& entries, const key& sk);
}
