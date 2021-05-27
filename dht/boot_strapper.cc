/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
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

#include "dht/boot_strapper.hh"
#include "dht/range_streamer.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "log.hh"
#include "db/config.hh"
#include "database.hh"
#include "streaming/stream_reason.hh"
#include "locator/abstract_replication_strategy.hh"

static logging::logger blogger("boot_strapper");

namespace dht {

future<> boot_strapper::bootstrap(streaming::stream_reason reason) {
    blogger.debug("Beginning bootstrap process: sorted_tokens={}", get_token_metadata().sorted_tokens());
    sstring description;
    if (reason == streaming::stream_reason::bootstrap) {
        description = "Bootstrap";
    } else if (reason == streaming::stream_reason::replace) {
        description = "Replace";
    } else {
        return make_exception_future<>(std::runtime_error("Wrong stream_reason provided: it can only be replace or bootstrap"));
    }
    auto streamer = make_lw_shared<range_streamer>(_db, _token_metadata_ptr, _abort_source, _tokens, _address, description, reason);
    auto nodes_to_filter = gms::get_local_gossiper().get_unreachable_members();
    if (reason == streaming::stream_reason::replace && _db.local().get_replace_address()) {
        nodes_to_filter.insert(_db.local().get_replace_address().value());
    }
    blogger.debug("nodes_to_filter={}", nodes_to_filter);
    streamer->add_source_filter(std::make_unique<range_streamer::failure_detector_source_filter>(nodes_to_filter));
    auto keyspaces = make_lw_shared<std::vector<sstring>>(_db.local().get_non_system_keyspaces());
    return do_for_each(*keyspaces, [this, keyspaces, streamer] (sstring& keyspace_name) {
        auto& ks = _db.local().find_keyspace(keyspace_name);
        auto& strategy = ks.get_replication_strategy();
        dht::token_range_vector ranges = strategy.get_pending_address_ranges(_token_metadata_ptr, _tokens, _address, utils::can_yield::no);
        blogger.debug("Will stream keyspace={}, ranges={}", keyspace_name, ranges);
        return streamer->add_ranges(keyspace_name, ranges);
    }).then([this, streamer] {
        _abort_source.check();
        return streamer->stream_async().handle_exception([streamer] (std::exception_ptr eptr) {
            blogger.warn("Error during bootstrap: {}", eptr);
            return make_exception_future<>(std::move(eptr));
        });
    });

}

std::unordered_set<token> boot_strapper::get_bootstrap_tokens(const token_metadata_ptr tmptr, database& db) {
    auto initial_tokens = db.get_initial_tokens();
    // if user specified tokens, use those
    if (initial_tokens.size() > 0) {
        blogger.debug("tokens manually specified as {}", initial_tokens);
        std::unordered_set<token> tokens;
        for (auto& token_string : initial_tokens) {
            auto token = dht::token::from_sstring(token_string);
            if (tmptr->get_endpoint(token)) {
                throw std::runtime_error(format("Bootstrapping to existing token {} is not allowed (decommission/removenode the old node first).", token_string));
            }
            tokens.insert(token);
        }
        blogger.debug("Get manually specified bootstrap_tokens={}", tokens);
        return tokens;
    }

    size_t num_tokens = db.get_config().num_tokens();
    if (num_tokens < 1) {
        throw std::runtime_error("num_tokens must be >= 1");
    }

    if (num_tokens == 1) {
        blogger.warn("Picking random token for a single vnode.  You should probably add more vnodes; failing that, you should probably specify the token manually");
    }

    auto tokens = get_random_tokens(std::move(tmptr), num_tokens);
    blogger.debug("Get random bootstrap_tokens={}", tokens);
    return tokens;
}

std::unordered_set<token> boot_strapper::get_random_tokens(const token_metadata_ptr tmptr, size_t num_tokens) {
    std::unordered_set<token> tokens;
    while (tokens.size() < num_tokens) {
        auto token = dht::token::get_random_token();
        auto ep = tmptr->get_endpoint(token);
        if (!ep) {
            tokens.emplace(token);
        }
    }
    return tokens;
}


} // namespace dht
