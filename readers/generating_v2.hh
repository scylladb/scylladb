/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "schema/schema_fwd.hh"
#include <seastar/core/future.hh>
#include "mutation/mutation_fragment_fwd.hh"

using namespace seastar;

class flat_mutation_reader_v2;
class reader_permit;

flat_mutation_reader_v2
make_generating_reader_v2(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_v2_opt> ()> get_next_fragment);

flat_mutation_reader_v2
make_generating_reader_v1(schema_ptr s, reader_permit permit, noncopyable_function<future<mutation_fragment_opt> ()> get_next_fragment);
