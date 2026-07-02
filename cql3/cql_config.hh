/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */



#pragma once

#include "restrictions/restrictions_config.hh"
#include "cql3/restrictions/replication_restrictions.hh"
#include "cql3/restrictions/twcs_restrictions.hh"
#include "cql3/restrictions/view_restrictions.hh"
#include "db/tri_mode_restriction.hh"
#include "utils/updateable_value.hh"

namespace db { class config; }

namespace cql3 {

struct cql_config {
    restrictions::restrictions_config restrictions;
    replication_restrictions replication_restrictions;
    twcs_restrictions twcs_restrictions;
    view_restrictions view_restrictions;
    utils::updateable_value<uint32_t> select_internal_page_size;
    utils::updateable_value<db::tri_mode_restriction> strict_allow_filtering;
    utils::updateable_value<bool> enable_parallelized_aggregation;
    utils::updateable_value<uint32_t> batch_size_warn_threshold_in_kb;
    utils::updateable_value<uint32_t> batch_size_fail_threshold_in_kb;
    utils::updateable_value<bool> restrict_future_timestamp;
    utils::updateable_value<bool> enable_create_table_with_compact_storage;
    utils::updateable_value<uint32_t> secondary_index_intersection_skip_max_rows;

    explicit cql_config(const db::config& cfg)
        : restrictions(cfg)
        , replication_restrictions(cfg)
        , twcs_restrictions(cfg)
        , view_restrictions(cfg)
        , select_internal_page_size(cfg.select_internal_page_size)
        , strict_allow_filtering(cfg.strict_allow_filtering)
        , enable_parallelized_aggregation(cfg.enable_parallelized_aggregation)
        , batch_size_warn_threshold_in_kb(cfg.batch_size_warn_threshold_in_kb)
        , batch_size_fail_threshold_in_kb(cfg.batch_size_fail_threshold_in_kb)
        , restrict_future_timestamp(cfg.restrict_future_timestamp)
        , enable_create_table_with_compact_storage(cfg.enable_create_table_with_compact_storage)
        , secondary_index_intersection_skip_max_rows(cfg.secondary_index_intersection_skip_max_rows)
    {}
    struct default_tag{};
    cql_config(default_tag)
        : restrictions(restrictions::restrictions_config::default_tag{})
        , replication_restrictions(replication_restrictions::default_tag{})
        , twcs_restrictions(twcs_restrictions::default_tag{})
        , view_restrictions(view_restrictions::default_tag{})
        , select_internal_page_size(10000)
        , strict_allow_filtering(db::tri_mode_restriction(db::tri_mode_restriction_t::mode::WARN))
        , enable_parallelized_aggregation(true)
        , batch_size_warn_threshold_in_kb(128)
        , batch_size_fail_threshold_in_kb(1024)
        , restrict_future_timestamp(true)
        , enable_create_table_with_compact_storage(false)
        , secondary_index_intersection_skip_max_rows(100)
    {}
};

extern const cql_config default_cql_config;

}
