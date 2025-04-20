
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/net/socket_defs.hh>

// Common values used in multiple LDAP tests.
namespace {

constexpr auto base_dn = "dc=example,dc=com";
constexpr auto manager_dn = "cn=root,dc=example,dc=com";
constexpr auto manager_password = "secret";
const auto ldap_envport = std::getenv("SEASTAR_LDAP_PORT");
const auto ldap_envhost = std::getenv("SEASTAR_LDAP_HOST");
const std::string ldap_port(ldap_envport ? ldap_envport : "389");
const std::string ldap_host(ldap_envhost ? ldap_envhost : "127.0.0.1");
const seastar::socket_address local_ldap_address(seastar::ipv4_addr(ldap_host, std::stoi(ldap_port)));
const seastar::socket_address local_fail_inject_address(seastar::ipv4_addr(ldap_host, std::stoi(ldap_port) + 2));

} // anonymous namespace
