/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/sstring.hh>

namespace alternator {
// We use the table tag TTL_TAG_KEY ("system:ttl_attribute") to remember
// which attribute was chosen as the expiration-time attribute for
// Alternator's TTL and CQL's per-row TTL features.
// Currently, the *value* of this tag is simply the name of the attribute:
// It can refer to a real column or if that doesn't exist, to a member of
// the ":attrs" map column (which Alternator uses).
extern const sstring TTL_TAG_KEY;
} // namespace alternator

// let users use TTL_TAG_KEY without the "alternator::" prefix,
// to make it easier to move it to a different namespace later.
using alternator::TTL_TAG_KEY;
