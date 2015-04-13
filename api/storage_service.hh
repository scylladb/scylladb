/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_STORAGE_SERVICE_HH_
#define API_STORAGE_SERVICE_HH_

#include "api.hh"

namespace api {

future<> set_storage_service(http_context& ctx);

}

#endif /* API_APP_HH_ */
