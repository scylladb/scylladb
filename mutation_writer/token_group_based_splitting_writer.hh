/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include "readers/mutation_reader.hh"
#include "dht/token.hh"

namespace mutation_writer {

using token_group_id = size_t;
using classify_by_token_group = std::function<token_group_id(dht::token)>;

// This segregation mechanism work with the concept of token groups, where
// different groups don't overlap each other in their token range, and also
// group 0 always precedes group 1 in the token order.
// Therefore, it's guaranteed that the segregator will emit data for only
// one group at a time. When moving to the next group, we're guaranteed
// that producer won't longer emit data for the previous group.
future<> segregate_by_token_group(mutation_reader producer, classify_by_token_group classify, reader_consumer_v2 consumer);

} // namespace mutation_writer
