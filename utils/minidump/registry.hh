/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <cstdint>
#include <span>
#include <string_view>
#include "utils/small_vector.hh"

// Memory ranges which the minidump writer selects with "tag:<name>" rules.
//
// Owners of memory which must not end up in a minidump by default (because it
// holds user data, or because it is big and useless for debugging) register it
// here. Registration is shard-local and lasts as long as the
// registered_ranges object.
namespace minidump {

namespace tags {
inline constexpr std::string_view bloom_filter = "bloom_filter";
inline constexpr std::string_view sstable_summary = "sstable_summary";
inline constexpr std::string_view commitlog_buffer = "commitlog_buffer";
}

struct range {
    uintptr_t start;
    size_t size;
};

class registered_ranges {
    using hook_type = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    hook_type _hook;
    std::string_view _tag;
    utils::small_vector<range, 1> _ranges;
public:
    using list_type = boost::intrusive::list<registered_ranges,
            boost::intrusive::member_hook<registered_ranges, hook_type, &registered_ranges::_hook>,
            boost::intrusive::constant_time_size<false>>;

    // `tag` must have static storage duration.
    explicit registered_ranges(std::string_view tag) noexcept;
    registered_ranges(registered_ranges&& o) noexcept;
    registered_ranges& operator=(registered_ranges&& o) noexcept;

    void add(const void* start, size_t size);
    void clear() noexcept { _ranges.clear(); }

    std::string_view tag() const noexcept { return _tag; }
    std::span<const range> ranges() const noexcept { return {_ranges.data(), _ranges.size()}; }
};

// All registered_ranges objects of the calling shard.
registered_ranges::list_type& registered_ranges_list() noexcept;

}
