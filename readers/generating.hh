/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema_fwd.hh"
#include <seastar/core/future.hh>
#include <seastar/util/optimized_optional.hh>
#include <functional>

using namespace seastar;

class flat_mutation_reader;
class reader_permit;
class mutation_fragment;

using mutation_fragment_opt = optimized_optional<mutation_fragment>;

flat_mutation_reader_v2
make_generating_reader(schema_ptr s, reader_permit permit, std::function<future<mutation_fragment_opt> ()> get_next_fragment);

