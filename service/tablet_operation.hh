// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

#include <variant>
#include "gc_clock.hh"

namespace service {

struct tablet_operation_empty_result {
};

struct tablet_operation_repair_result {
    gc_clock::time_point repair_time;
};

using tablet_operation_result = std::variant<tablet_operation_empty_result, tablet_operation_repair_result>;

}

