/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string_view>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

class unqualified_name {
    sstring _qname;
public:
    unqualified_name(std::string_view pkg_pfx, std::string_view name)
        : _qname(name.compare(0, pkg_pfx.size(), pkg_pfx) == 0 ? name.substr(pkg_pfx.size()) : name)
    {}
    operator const sstring&() const {
        return _qname;
    }
};
