/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/util/modules.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/print.hh>
#include "log.hh"
#include "seastarx.hh"
#include "version_generator.hh"

namespace gms {
namespace version_generator {
// In the original Cassandra code, version was an AtomicInteger.
// For us, we run the gossiper on a single CPU, and don't need to use atomics.
static version_type version;

static logging::logger logger("version_generator");

version_type get_next_version() noexcept
{
    if (this_shard_id() != 0) [[unlikely]] {
        on_fatal_internal_error(logger, format(
                "{} can only be called on shard 0, but it was called on shard {}",
                __FUNCTION__, this_shard_id()));
    }
    return ++version;
}

} // namespace version_generator
} // namespace gms
