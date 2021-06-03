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

#pragma once

#include "seastarx.hh"
#include "service_level_controller.hh"


namespace db {
    class system_distributed_keyspace;
}
namespace qos {
class standard_service_level_distributed_data_accessor : public service_level_controller::service_level_distributed_data_accessor,
         public ::enable_shared_from_this<standard_service_level_distributed_data_accessor> {
private:
    db::system_distributed_keyspace& _sys_dist_ks;
public:
    standard_service_level_distributed_data_accessor(db::system_distributed_keyspace &sys_dist_ks);
    virtual future<qos::service_levels_info> get_service_levels() const override;
    virtual future<qos::service_levels_info> get_service_level(sstring service_level_name) const override;
    virtual future<> set_service_level(sstring service_level_name, qos::service_level_options slo) const override;
    virtual future<> drop_service_level(sstring service_level_name) const override;
};
}
