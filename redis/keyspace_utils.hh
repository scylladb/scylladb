/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/future.hh>

namespace service {
class migration_manager;
class storage_proxy;
}
namespace db {
class config;
}

namespace gms {
class gossiper;
}

namespace data_dictionary {
class database;
}

namespace redis {

static constexpr auto DATA_COLUMN_NAME = "data";
static constexpr auto STRINGs         = "STRINGs";
static constexpr auto LISTs           = "LISTs";
static constexpr auto HASHes          = "HASHes";
static constexpr auto SETs            = "SETs";
static constexpr auto ZSETs           = "ZSETs";

seastar::future<> maybe_create_keyspace(seastar::sharded<service::storage_proxy>& proxy, data_dictionary::database db, seastar::sharded<service::migration_manager>& mm, db::config& cfg, seastar::sharded<gms::gossiper>& g);

}
