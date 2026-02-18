/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/config.hh"
#include "gms/gossiper.hh"
#include "message/messaging_service.hh"
#include "node_ops/node_ops_ctl.hh"
#include "service/storage_service.hh"
#include "replica/database.hh"

#include <fmt/ranges.h>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "idl/node_ops.dist.hh"

static logging::logger nlogger("node_ops");