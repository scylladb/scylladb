/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <seastar/util/bool_class.hh>

using namespace seastar;

class mutation_source;
class position_in_partition;
class mutation_reader;

namespace streamed_mutation {
    class forwarding_tag;
    using forwarding = bool_class<forwarding_tag>;
}
