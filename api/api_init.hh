/*
 * Copyright 2016 ScylaDB
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
#include "database.hh"
#include "service/storage_proxy.hh"
#include "http/httpd.hh"

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    http_context(distributed<database>& _db,
            distributed<service::storage_proxy>& _sp)
            : db(_db), sp(_sp) {
    }
};

future<> set_server_init(http_context& ctx);
future<> set_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx);
future<> set_server_gossip(http_context& ctx);
future<> set_server_load_sstable(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx);
future<> set_server_stream_manager(http_context& ctx);
future<> set_server_gossip_settle(http_context& ctx);
future<> set_server_cache(http_context& ctx);
future<> set_server_done(http_context& ctx);

}
