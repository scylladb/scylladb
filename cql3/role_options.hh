#pragma once

#include <experimental/optional>
#include <map>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"
#include "stdx.hh"

namespace cql3 {

struct role_options final {
    stdx::optional<bool> is_superuser{};
    stdx::optional<bool> can_login{};
    stdx::optional<sstring> password{};

    // The parser makes a `std::map`, not a `std::unordered_map`.
    stdx::optional<std::map<sstring, sstring>> options{};
};

}
