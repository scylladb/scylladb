/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "standard_service_level_distributed_data_accessor.hh"
#include "db/system_distributed_keyspace.hh"

namespace qos {

standard_service_level_distributed_data_accessor::standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks):
_sys_dist_ks(sys_dist_ks) {
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_levels() const {
    return _sys_dist_ks.get_service_levels();
}

future<qos::service_levels_info> standard_service_level_distributed_data_accessor::get_service_level(sstring service_level_name) const {
    return _sys_dist_ks.get_service_level(service_level_name);
}

future<> standard_service_level_distributed_data_accessor::set_service_level(sstring service_level_name, qos::service_level_options slo) const {
    return _sys_dist_ks.set_service_level(service_level_name, slo);
}

future<> standard_service_level_distributed_data_accessor::drop_service_level(sstring service_level_name) const {
    return _sys_dist_ks.drop_service_level(service_level_name);
}

}
