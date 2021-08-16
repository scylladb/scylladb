/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "qos_common.hh"

namespace qos {
    class qos_configuration_change_subscriber {
    public:
        /** This callback is going to be called just before the service level is available **/
        virtual future<> on_before_service_level_add(sstring name, service_level_options slo) = 0;
        /** This callback is going to be called just after the service level is removed **/
        virtual future<> on_after_service_level_remove(sstring name) = 0;
        /** This callback is going to be called just before the service level is changed **/
        virtual future<> on_before_service_level_change(sstring name, service_level_options slo_before, service_level_options slo_after) = 0;

        virtual ~qos_configuration_change_subscriber() {};
    };
}