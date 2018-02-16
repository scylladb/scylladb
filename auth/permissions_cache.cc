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

#include "auth/permissions_cache.hh"

#include "auth/authorizer.hh"
#include "auth/common.hh"
#include "auth/service.hh"
#include "db/config.hh"

namespace auth {

permissions_cache_config permissions_cache_config::from_db_config(const db::config& dc) {
    permissions_cache_config c;
    c.max_entries = dc.permissions_cache_max_entries();
    c.validity_period = std::chrono::milliseconds(dc.permissions_validity_in_ms());
    c.update_period = std::chrono::milliseconds(dc.permissions_update_interval_in_ms());

    return c;
}

permissions_cache::permissions_cache(const permissions_cache_config& c, service& ser, logging::logger& log)
        : _cache(c.max_entries, c.validity_period, c.update_period, log, [&ser, &log](const key_type& k) {
              log.debug("Refreshing permissions for {}", k.first);
              return ser.get_uncached_permissions(k.first, k.second);
          }) {
}

future<permission_set> permissions_cache::get(const role_or_anonymous& maybe_role, const resource& r) {
    return do_with(key_type(maybe_role, r), [this](const auto& k) {
        return _cache.get(k);
    });
}

}
