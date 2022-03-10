/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <vector>
#include "schema_fwd.hh"

class mutation;

namespace query {
    class partition_slice;
}

std::vector<mutation> slice_mutations(schema_ptr schema, std::vector<mutation> ms, const query::partition_slice& slice);

