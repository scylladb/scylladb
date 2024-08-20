/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "utils/UUID.hh"
#include <seastar/core/future.hh>

namespace gms {
class gossiper;
}


namespace service {

class group0_state_id_handler {

    gms::gossiper& _gossiper;

    utils::UUID _state_id_last_advertised;

public:
    explicit group0_state_id_handler(gms::gossiper& gossiper);

    future<> advertise_state_id(utils::UUID state_id);
};

} // namespace service
