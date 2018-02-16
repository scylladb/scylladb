#pragma once

#include <map>
#include <optional>

#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace cql3 {

struct role_options final {
    std::optional<bool> is_superuser{};
    std::optional<bool> can_login{};
    std::optional<sstring> password{};

    // The parser makes a `std::map`, not a `std::unordered_map`.
    std::optional<std::map<sstring, sstring>> options{};
};

}
