/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef APPS_SEASTAR_THRIFT_HANDLER_HH_
#define APPS_SEASTAR_THRIFT_HANDLER_HH_

#include "Cassandra.h"
#include "auth/service.hh"
#include "cql3/query_processor.hh"
#include <memory>

struct timeout_config;
class service_permit;
namespace service { class storage_service; }

namespace data_dictionary {
class database;
}

std::unique_ptr<::cassandra::CassandraCobSvIfFactory> create_handler_factory(data_dictionary::database db, distributed<cql3::query_processor>& qp, sharded<service::storage_service>& ss, sharded<service::storage_proxy>& proxy, auth::service&, const updateable_timeout_config&, service_permit& current_permit);

#endif /* APPS_SEASTAR_THRIFT_HANDLER_HH_ */
