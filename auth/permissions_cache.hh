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
#include <functional>
#include <iostream>
#include <utility>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/authenticated_user.hh"
#include "auth/data_resource.hh"
#include "auth/permission.hh"
#include "log.hh"
#include "utils/loading_cache.hh"

namespace std {

template <>
struct hash<auth::data_resource> final {
    size_t operator()(const auth::data_resource & v) const {
        return v.hash_value();
    }
};

template <>
struct hash<auth::authenticated_user> final {
    size_t operator()(const auth::authenticated_user & v) const {
        return utils::tuple_hash()(v.name(), v.is_anonymous());
    }
};

inline std::ostream& operator<<(std::ostream& os, const std::pair<auth::authenticated_user, auth::data_resource>& p) {
    os << "{user: " << p.first.name() << ", data_resource: " << p.second << "}";
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
            std::pair<authenticated_user, data_resource>,
            permission_set,
            utils::loading_cache_reload_enabled::yes,
            utils::simple_entry_size<permission_set>,
            utils::tuple_hash>;

    using key_type = typename cache_type::key_type;

    cache_type _cache;

public:
    explicit permissions_cache(const permissions_cache_config&, service&, logging::logger&);

    future<> start() {
        return make_ready_future<>();
    }

    future <> stop() {
        return _cache.stop();
    }

    future<permission_set> get(::shared_ptr<authenticated_user>, data_resource);
};

}
