/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "db/config.hh"
#include "utils/updateable_value.hh"

namespace cql3 {

struct replication_restrictions {
    utils::updateable_value<db::tri_mode_restriction> restrict_replication_simplestrategy;
    utils::updateable_value<std::vector<enum_option<db::replication_strategy_restriction_t>>> replication_strategy_warn_list;
    utils::updateable_value<std::vector<enum_option<db::replication_strategy_restriction_t>>> replication_strategy_fail_list;
    utils::updateable_value<int> minimum_replication_factor_fail_threshold;
    utils::updateable_value<int> minimum_replication_factor_warn_threshold;
    utils::updateable_value<int> maximum_replication_factor_fail_threshold;
    utils::updateable_value<int> maximum_replication_factor_warn_threshold;

    explicit replication_restrictions(const db::config& cfg)
        : restrict_replication_simplestrategy(cfg.restrict_replication_simplestrategy)
        , replication_strategy_warn_list(cfg.replication_strategy_warn_list)
        , replication_strategy_fail_list(cfg.replication_strategy_fail_list)
        , minimum_replication_factor_fail_threshold(cfg.minimum_replication_factor_fail_threshold)
        , minimum_replication_factor_warn_threshold(cfg.minimum_replication_factor_warn_threshold)
        , maximum_replication_factor_fail_threshold(cfg.maximum_replication_factor_fail_threshold)
        , maximum_replication_factor_warn_threshold(cfg.maximum_replication_factor_warn_threshold)
    {}

    struct default_tag{};
    replication_restrictions(default_tag)
        : restrict_replication_simplestrategy(db::tri_mode_restriction(db::tri_mode_restriction_t::mode::FALSE))
        , replication_strategy_warn_list(std::vector<enum_option<db::replication_strategy_restriction_t>>{})
        , replication_strategy_fail_list(std::vector<enum_option<db::replication_strategy_restriction_t>>{})
        , minimum_replication_factor_fail_threshold(-1)
        , minimum_replication_factor_warn_threshold(3)
        , maximum_replication_factor_fail_threshold(-1)
        , maximum_replication_factor_warn_threshold(-1)
    {}
};

} // namespace cql3
