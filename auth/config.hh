/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <seastar/core/sstring.hh>

#include "utils/updateable_value.hh"

namespace auth {

struct config {
    std::string auth_superuser_name;
    std::string auth_superuser_salted_password;
    seastar::sstring saslauthd_socket_path;
    std::vector<std::unordered_map<seastar::sstring, seastar::sstring>> auth_certificate_role_queries;
    seastar::sstring ldap_url_template;
    seastar::sstring ldap_attr_role;
    seastar::sstring ldap_bind_dn;
    seastar::sstring ldap_bind_passwd;
    utils::updateable_value<uint32_t> permissions_update_interval_in_ms;
};

}
