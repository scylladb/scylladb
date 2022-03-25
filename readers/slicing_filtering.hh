/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "interval.hh"

class flat_mutation_reader;

namespace dht {
class ring_position;
using partition_range = nonwrapping_interval<ring_position>;
}

namespace query {
class partition_slice;
}

/// Create a wrapper that filters fragments according to partition range and slice.
flat_mutation_reader make_slicing_filtering_reader(flat_mutation_reader, const dht::partition_range&, const query::partition_slice&);
