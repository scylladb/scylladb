/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/chunked_string.hh"

namespace utils {

sstring chunked_string::linearize() const {
    return _data.with_linearized([&] (bytes_view bv) {
        return sstring(to_string_view(bv));
    });
}

managed_bytes from_hex(utils::chunked_string_view view) {
    if (view.size() % 2) {
        throw std::invalid_argument("A hex string representing bytes must have an even length");
    }
    managed_bytes result{managed_bytes::initialized_later(), view.size() / 2};
    managed_bytes_mutable_view dest(result);
    // A hex pair may straddle a fragment boundary, so carry over an unpaired nibble.
    std::optional<char> carry;
    for (auto frag : fragment_range(view.data())) {
        auto v = std::string_view(reinterpret_cast<const char*>(frag.data()), frag.size());
        size_t start = 0;
        if (carry) {
            // Complete the straddling pair using the first char of this fragment.
            char pair[2] = {*carry, v[0]};
            write_fragmented(dest, single_fragmented_view(::from_hex(std::string_view(pair, 2))));
            carry.reset();
            start = 1;
        }
        size_t len = v.size() - start;
        if (len % 2) {
            // Odd number of chars remaining: park the last one for the next fragment.
            carry = v[start + len - 1];
            --len;
        }
        write_fragmented(dest, single_fragmented_view(::from_hex(v.substr(start, len))));
    }
    return result;
}

} // namespace utils
