/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "schema/schema_fwd.hh"
#include <deque>
#include "dht/i_partitioner_fwd.hh"
#include "readers/mutation_reader.hh"

class mutation_reader;
class mutation_fragment_v2;

namespace query {
    class partition_slice;
}

mutation_reader
make_mutation_reader_from_fragments(schema_ptr, mutation_reader::reader_params, std::deque<mutation_fragment_v2>);

mutation_reader
make_mutation_reader_from_fragments(schema_ptr, mutation_reader::reader_params, std::deque<mutation_fragment_v2>, const dht::partition_range& pr);

mutation_reader
make_mutation_reader_from_fragments(schema_ptr, mutation_reader::reader_params, std::deque<mutation_fragment_v2>, const dht::partition_range& pr, const query::partition_slice& slice);
