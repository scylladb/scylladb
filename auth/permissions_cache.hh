/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <chrono>
#include <experimental/string_view>
#include <functional>
#include <iostream>
#include <optional>
#include <utility>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include "auth/authenticated_user.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "auth/role_or_anonymous.hh"
#include "log.hh"
#include "stdx.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"

namespace std {

inline std::ostream& operator<<(std::ostream& os, const pair<auth::role_or_anonymous, auth::resource>& p) {
    os << "{role: " << p.first << ", resource: " << p.second << "}";
    return os;
}

}

namespace db {
class config;
}

namespace auth {

class service;

struct permissions_cache_config final {
    static permissions_cache_config from_db_config(const db::config&);

    std::size_t max_entries;
    std::chrono::milliseconds validity_period;
    std::chrono::milliseconds update_period;
};

class permissions_cache final {
    using cache_type = utils::loading_cache<
            std::pair<role_or_anonymous, resource>,
            permission_set,
            utils::loading_cache_reload_enabled::yes,
            utils::simple_entry_size<permission_set>,
            utils::tuple_hash>;

    using key_type = typename cache_type::key_type;

    cache_type _cache;

public:
    explicit permissions_cache(const permissions_cache_config&, service&, logging::logger&);

    future <> stop() {
        return _cache.stop();
    }

    future<permission_set> get(const role_or_anonymous&, const resource&);
};

}
