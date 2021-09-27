/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
