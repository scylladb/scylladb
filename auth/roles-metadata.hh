/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string_view>

namespace auth {

namespace meta {

namespace roles_table {

constexpr std::string_view name{"roles", 5};

constexpr std::string_view role_col_name{"role", 4};

} // namespace roles_table

} // namespace meta

} // namespace auth
