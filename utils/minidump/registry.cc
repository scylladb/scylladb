/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/minidump/registry.hh"

namespace minidump {

registered_ranges::list_type& registered_ranges_list() noexcept {
    static thread_local registered_ranges::list_type list;
    return list;
}

registered_ranges::registered_ranges(std::string_view tag) noexcept
    : _tag(tag)
{
    registered_ranges_list().push_back(*this);
}

registered_ranges::registered_ranges(registered_ranges&& o) noexcept
    : _tag(o._tag)
    , _ranges(std::move(o._ranges))
{
    o._ranges.clear();
    registered_ranges_list().push_back(*this);
}

registered_ranges& registered_ranges::operator=(registered_ranges&& o) noexcept {
    if (this != &o) {
        _tag = o._tag;
        _ranges = std::move(o._ranges);
        o._ranges.clear();
    }
    return *this;
}

void registered_ranges::add(const void* start, size_t size) {
    _ranges.push_back(range{reinterpret_cast<uintptr_t>(start), size});
}

}
