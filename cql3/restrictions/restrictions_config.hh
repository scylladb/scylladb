/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#pragma once

#include <cstdint>
#include "db/config.hh"
#include "utils/updateable_value.hh"

namespace cql3::restrictions {

struct restrictions_config {
    utils::updateable_value<uint32_t> partition_key_restrictions_max_cartesian_product_size;
    utils::updateable_value<uint32_t> clustering_key_restrictions_max_cartesian_product_size;

    explicit restrictions_config(const db::config& cfg)
        : partition_key_restrictions_max_cartesian_product_size(cfg.max_partition_key_restrictions_per_query)
        , clustering_key_restrictions_max_cartesian_product_size(cfg.max_clustering_key_restrictions_per_query)
    {}

    struct default_tag{};
    restrictions_config(default_tag)
        : partition_key_restrictions_max_cartesian_product_size(100)
        , clustering_key_restrictions_max_cartesian_product_size(100)
    {}
};

}
