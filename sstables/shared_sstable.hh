/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <utility>
#include <functional>
#include <unordered_set>

#include <fmt/format.h>

#include <seastar/core/shared_ptr.hh>

namespace sstables {

class sstable;

};

// Customize deleter so that lw_shared_ptr can work with an incomplete sstable class
namespace seastar {

template <>
struct lw_shared_ptr_deleter<sstables::sstable> {
    static void dispose(sstables::sstable* sst);
};

}

namespace sstables {

using shared_sstable = seastar::lw_shared_ptr<sstable>;
using sstable_list = std::unordered_set<shared_sstable>;

std::string to_string(const shared_sstable& sst, bool include_origin = true);

} // namespace sstables

template <>
struct fmt::formatter<sstables::shared_sstable> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::shared_sstable& sst, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", sstables::to_string(sst));
    }
};

namespace std {

inline std::ostream& operator<<(std::ostream& os, const sstables::shared_sstable& sst) {
    return os << fmt::format("{}", sst);
}

} // namespace std
