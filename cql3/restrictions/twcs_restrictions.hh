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

struct twcs_restrictions {
    utils::updateable_value<uint32_t> twcs_max_window_count;
    utils::updateable_value<db::tri_mode_restriction> restrict_twcs_without_default_ttl;

    explicit twcs_restrictions(const db::config& cfg)
        : twcs_max_window_count(cfg.twcs_max_window_count)
        , restrict_twcs_without_default_ttl(cfg.restrict_twcs_without_default_ttl)
    {}

    struct default_tag{};
    twcs_restrictions(default_tag)
        : twcs_max_window_count(10000)
        , restrict_twcs_without_default_ttl(db::tri_mode_restriction(db::tri_mode_restriction_t::mode::WARN))
    {}
};

} // namespace cql3
