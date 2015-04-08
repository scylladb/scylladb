/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "message/messaging_service.hh"
#include "core/distributed.hh"

namespace net {
    distributed<messaging_service> _the_messaging_service;
}
