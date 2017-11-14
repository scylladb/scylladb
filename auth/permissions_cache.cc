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

namespace auth {

permissions_cache::permissions_cache(const permissions_cache_config& c, authorizer& a, logging::logger& log)
        : _cache(c.max_entries, c.validity_period, c.update_period, log, [&a, &log](const key_type& k) {
              log.debug("Refreshing permissions for {}", k.first.name());
              return a.authorize(::make_shared<authenticated_user>(k.first), k.second);
          }) {
}

future<permission_set> permissions_cache::get(::shared_ptr<authenticated_user> user, data_resource r) {
    return _cache.get(key_type(*user, r));
}

}
