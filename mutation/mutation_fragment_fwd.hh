/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/util/optimized_optional.hh>

using namespace seastar;

class mutation_fragment;
class mutation_fragment_v2;

using mutation_fragment_opt = optimized_optional<mutation_fragment>;
using mutation_fragment_v2_opt = optimized_optional<mutation_fragment_v2>;

