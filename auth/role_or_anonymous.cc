/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/role_or_anonymous.hh"

namespace auth {

bool is_anonymous(const role_or_anonymous& mr) noexcept {
    return !mr.name.has_value();
}

}

auto fmt::formatter<auth::role_or_anonymous>::format(const auth::role_or_anonymous& mr,
                                                     fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", mr.name.value_or("<anonymous>"));
}
