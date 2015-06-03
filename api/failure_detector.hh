/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_FAILURE_DETECTOR_HH_
#define API_FAILURE_DETECTOR_HH_

#include "api.hh"

namespace api {

void set_failure_detector(http_context& ctx, routes& r);

}



#endif /* API_FAILURE_DETECTOR_HH_ */
