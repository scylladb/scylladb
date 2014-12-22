/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef APPS_SEASTAR_THRIFT_HANDLER_HH_
#define APPS_SEASTAR_THRIFT_HANDLER_HH_

#include "Cassandra.h"
#include <memory>

std::unique_ptr<org::apache::cassandra::CassandraCobSvIfFactory> create_handler_factory();

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
