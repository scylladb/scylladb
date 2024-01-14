/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service_level_statement.hh"

namespace cql3 {

namespace statements {

uint32_t service_level_statement::get_bound_terms() const {
    return 0;
}

bool service_level_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return false;
}

future<> service_level_statement::check_access(query_processor& qp, const service::client_state &state) const {
    return make_ready_future<>();
}

}
}
