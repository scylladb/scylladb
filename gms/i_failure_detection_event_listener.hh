/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "gms/inet_address.hh"

namespace gms {
/**
 * Implemented by the Gossiper to convict an endpoint
 * based on the PHI calculated by the Failure Detector on the inter-arrival
 * times of the heart beats.
 */

class i_failure_detection_event_listener {
public:
    virtual ~i_failure_detection_event_listener() {}
    /**
     * Convict the specified endpoint.
     *
     * @param ep  endpoint to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    virtual void convict(inet_address ep, double phi) = 0;
};

} // namespace gms
