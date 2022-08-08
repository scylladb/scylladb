/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "idl/keys.idl.hh"

enum class bound_weight : int8_t {
    before_all_prefixed = -1,
    equal = 0,
    after_all_prefixed = 1,
};

enum class partition_region : uint8_t {
    partition_start,
    static_row,
    clustered,
    partition_end,
};

class position_in_partition {
    partition_region get_type();
    bound_weight get_bound_weight();
    std::optional<clustering_key_prefix> get_clustering_key_prefix();
};

struct full_position {
    partition_key partition;
    position_in_partition position;
};
