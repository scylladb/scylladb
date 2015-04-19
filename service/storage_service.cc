/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "storage_service.hh"

namespace service {

int storage_service::RING_DELAY = storage_service::getRingDelay();

storage_service storage_service_instance;

}
