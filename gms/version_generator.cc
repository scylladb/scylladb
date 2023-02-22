/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <exception>

#include <fmt/format.h>

#include "version_generator.hh"

namespace gms {

void validate_gossip_generation(int64_t generation_number) {
    if (static_cast<uint64_t>(generation_number) > std::numeric_limits<std::make_unsigned_t<gms::generation_type>>::max()) {
        throw std::out_of_range(fmt::format("gossip generation {} is out of range", generation_number));
    }
}

namespace version_generator {
// In the original Cassandra code, version was an AtomicInteger.
// For us, we run the gossiper on a single CPU, and don't need to use atomics.
static version_type version = 0;

version_type get_next_version() noexcept
{
    return ++version;
}

} // namespace version_generator
} // namespace gms
