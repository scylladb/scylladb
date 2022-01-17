/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "seastar/core/sharded.hh"
#include "seastar/core/future.hh"

namespace service {
class migration_manager;
}
namespace db {
class config;
}

namespace gms {
class gossiper;
}

namespace redis {

static constexpr auto DATA_COLUMN_NAME = "data";
static constexpr auto STRINGs         = "STRINGs";
static constexpr auto LISTs           = "LISTs";
static constexpr auto HASHes          = "HASHes";
static constexpr auto SETs            = "SETs";
static constexpr auto ZSETs           = "ZSETs";

seastar::future<> maybe_create_keyspace(seastar::sharded<service::migration_manager>& mm, db::config& cfg, seastar::sharded<gms::gossiper>& g);

}
