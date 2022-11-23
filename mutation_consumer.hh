/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation_consumer_concepts.hh"

enum class consume_in_reverse {
    no = 0,
    yes,
    legacy_half_reverse,
};
