/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "interval.hh"

class flat_mutation_reader_v2;

namespace query {
class partition_slice;
}

/// A reader which applies slicing on another reader, which was created with schema::full_slice()
///
/// The slicing is applied with fast-forwarding the reader the clustering ranges,
/// so it has to be created with streamed_mutation::forwarding::yes
/// Intended to allow use-cases where a single reader is reused across multiple
/// partitions, reading a different slice from each one.
flat_mutation_reader_v2 make_slicing_forwarding_reader(
        flat_mutation_reader_v2 rd,
        const query::partition_slice& ps);
