/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/util/bool_class.hh>

// Scylla includes.
#include "locator/host_id.hh"

// STD.
#include <cstdint>

namespace db::hints {
namespace internal {

/// Type identifying the host a specific subset of hints should be sent to.
using endpoint_id = locator::host_id;

/// Tag specifying if hint sending should enter the so-called "drain mode".
/// If it should, that means that if a failure while sending a hint occurs,
/// the hint will be ignored rather than attempted to be sent again.
///
/// The tag is useful in communication between internal data structures
/// of the hinted handoff module.
using drain = seastar::bool_class<class drain_tag>;

/// Statistics related to hint sending. They should collect information
/// about a whole shard.
struct hint_stats {
    uint64_t size_of_hints_in_progress  = 0;
    uint64_t written                    = 0;
    uint64_t errors                     = 0;
    uint64_t dropped                    = 0;
    uint64_t sent_total                 = 0;
    uint64_t sent_hints_bytes_total     = 0;
    uint64_t discarded                  = 0;
    uint64_t send_errors                = 0;
    uint64_t corrupted_files            = 0;
};

} // namespace internal
} // namespace db::hints
