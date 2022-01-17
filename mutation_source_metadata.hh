/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>

#include "timestamp.hh"

struct mutation_source_metadata {
    std::optional<api::timestamp_type> min_timestamp;
    std::optional<api::timestamp_type> max_timestamp;
};