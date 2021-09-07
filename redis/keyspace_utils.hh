/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
