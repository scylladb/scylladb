/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "qos_common.hh"


namespace qos {

    struct service_level_info {
        sstring name;
    };
    class qos_configuration_change_subscriber {
    public:
        /** This callback is going to be called just before the service level is available **/
        virtual future<> on_before_service_level_add(service_level_options slo, service_level_info sl_info) = 0;
        /** This callback is going to be called just after the service level is removed **/
        virtual future<> on_after_service_level_remove(service_level_info sl_info) = 0;
        /** This callback is going to be called just before the service level is changed **/
        virtual future<> on_before_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) = 0;

        virtual future<> on_effective_service_levels_cache_reloaded() = 0;

        virtual ~qos_configuration_change_subscriber() {};
    };
}
