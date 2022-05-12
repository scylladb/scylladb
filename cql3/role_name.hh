/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <string_view>
#include <iosfwd>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace cql3 {

enum class preserve_role_case { yes, no };

class role_name final {
    sstring _name;

public:
    role_name(sstring name, preserve_role_case);

    std::string_view to_string() const noexcept {
        return _name;
    }
};

std::ostream& operator<<(std::ostream&, const role_name&);

}
