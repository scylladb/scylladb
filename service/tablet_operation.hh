// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <cstdint>
#include <optional>
#include <variant>
#include "gc_clock.hh"

namespace service {

struct tablet_operation_empty_result {
};

struct tablet_operation_repair_result {
    gc_clock::time_point repair_time;
};

// How a tablet repair deals with the hints and batchlog flush that
// precedes a repair of tables with repair mode tombstone GC.
enum class tablet_repair_flush_mode : uint8_t {
    flush,      // the repair flushes on all nodes itself
    skip,       // no flush is needed
    supplied,   // the caller has flushed on all nodes
};

struct tablet_repair_flush_info {
    tablet_repair_flush_mode mode;
    // Mode supplied only. nullopt means the caller's flush failed, which the
    // repair reports the same way as a failure of its own flush.
    std::optional<gc_clock::time_point> time;
};

using tablet_operation_result = std::variant<tablet_operation_empty_result, tablet_operation_repair_result>;

}

