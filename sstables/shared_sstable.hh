/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <utility>
#include <functional>
#include <unordered_set>

#include <fmt/format.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace sstables {

class sstable;

// Reference counting hooks for boost::intrusive_ptr. They are defined out of
// line so that shared_sstable can be used with an incomplete sstable class.
void intrusive_ptr_add_ref(sstable* sst) noexcept;
void intrusive_ptr_release(sstable* sst) noexcept;

using shared_sstable = boost::intrusive_ptr<sstable>;
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
