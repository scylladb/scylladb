/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "db/config.hh"
#include "db/tri_mode_restriction.hh"
#include "utils/updateable_value.hh"

namespace db { class config; }

namespace cql3 {

struct view_restrictions {
    utils::updateable_value<db::tri_mode_restriction> strict_is_not_null_in_views;

    explicit view_restrictions(const db::config& cfg)
        : strict_is_not_null_in_views(cfg.strict_is_not_null_in_views)
    {}

    struct default_tag{};
    view_restrictions(default_tag)
        : strict_is_not_null_in_views(db::tri_mode_restriction(db::tri_mode_restriction_t::mode::WARN))
    {}
};

}
