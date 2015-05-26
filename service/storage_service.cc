/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "storage_service.hh"
#include "core/distributed.hh"

namespace service {

int storage_service::RING_DELAY = storage_service::getRingDelay();

distributed<storage_service> _the_storage_service;

}
