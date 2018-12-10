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

#pragma once

#include "core/sstring.hh"
#include "utils/serialization.hh"
#include "utils/UUID.hh"
#include "version_generator.hh"
#include "gms/inet_address.hh"
#include "dht/i_partitioner.hh"
#include "to_string.hh"
#include "version.hh"
#include <unordered_set>
#include <vector>
#include <boost/range/adaptor/transformed.hpp>

namespace gms {

/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 * <p></p>
 * e.g. if we want to disseminate load information for node A do the following:
 * <p></p>
 * ApplicationState loadState = new ApplicationState(<string representation of load>);
 * Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

class versioned_value {
public:
    // this must be a char that cannot be present in any token
    static constexpr char DELIMITER = ',';
    static constexpr const char DELIMITER_STR[] = { DELIMITER, 0 };

    // values for ApplicationState.STATUS
    static constexpr const char* STATUS_BOOTSTRAPPING = "BOOT";
    static constexpr const char* STATUS_NORMAL = "NORMAL";
    static constexpr const char* STATUS_LEAVING = "LEAVING";
    static constexpr const char* STATUS_LEFT = "LEFT";
    static constexpr const char* STATUS_MOVING = "MOVING";

    static constexpr const char* REMOVING_TOKEN = "removing";
    static constexpr const char* REMOVED_TOKEN = "removed";

    static constexpr const char* HIBERNATE = "hibernate";
    static constexpr const char* SHUTDOWN = "shutdown";

    // values for ApplicationState.REMOVAL_COORDINATOR
    static constexpr const char* REMOVAL_COORDINATOR = "REMOVER";

    int version;
    sstring value;
public:
    bool operator==(const versioned_value& other) const {
        return version == other.version &&
               value   == other.value;
    }

public:
    versioned_value(const sstring& value, int version = version_generator::get_next_version())
        : version(version), value(value) {
#if 0
        // blindly interning everything is somewhat suboptimal -- lots of VersionedValues are unique --
        // but harmless, and interning the non-unique ones saves significant memory.  (Unfortunately,
        // we don't really have enough information here in VersionedValue to tell the probably-unique
        // values apart.)  See CASSANDRA-6410.
        this.value = value.intern();
#endif
    }

    versioned_value(sstring&& value, int version = version_generator::get_next_version())
        : version(version), value(std::move(value)) {
    }

    versioned_value()
        : version(-1) {
    }

    int compare_to(const versioned_value &value) {
        return version - value.version;
    }

    friend inline std::ostream& operator<<(std::ostream& os, const versioned_value& x) {
        return os << "Value(" << x.value << "," << x.version <<  ")";
    }

    static sstring version_string(const std::initializer_list<sstring>& args) {
        return ::join(sstring(versioned_value::DELIMITER_STR), args);
    }

public:
    class factory {
        using token = dht::token;
    public:
        sstring make_full_token_string(const std::unordered_set<token>& tokens) {
            return ::join(";", tokens | boost::adaptors::transformed([] (const token& t) {
                return dht::global_partitioner().to_sstring(t); })
            );
        }
        sstring make_token_string(const std::unordered_set<token>& tokens) {
            if (tokens.empty()) {
                return "";
            }
            return dht::global_partitioner().to_sstring(*tokens.begin());
        }

        versioned_value clone_with_higher_version(const versioned_value& value) {
            return versioned_value(value.value);
        }

        versioned_value bootstrapping(const std::unordered_set<token>& tokens) {
            return versioned_value(version_string({sstring(versioned_value::STATUS_BOOTSTRAPPING),
                                                   make_token_string(tokens)}));
        }

        versioned_value normal(const std::unordered_set<token>& tokens) {
            return versioned_value(version_string({sstring(versioned_value::STATUS_NORMAL),
                                                   make_token_string(tokens)}));
        }

        versioned_value load(double load) {
            return versioned_value(to_sstring(load));
        }

        versioned_value schema(const utils::UUID &new_version) {
            return versioned_value(new_version.to_sstring());
        }

        versioned_value leaving(const std::unordered_set<token>& tokens) {
            return versioned_value(version_string({sstring(versioned_value::STATUS_LEAVING),
                                                   make_token_string(tokens)}));
        }

        versioned_value left(const std::unordered_set<token>& tokens, int64_t expire_time) {
            return versioned_value(version_string({sstring(versioned_value::STATUS_LEFT),
                                                   make_token_string(tokens),
                                                   std::to_string(expire_time)}));
        }

        versioned_value moving(token t) {
            std::unordered_set<token> tokens = {t};
            return versioned_value(version_string({sstring(versioned_value::STATUS_MOVING),
                                                   make_token_string(tokens)}));
        }

        versioned_value host_id(const utils::UUID& host_id) {
            return versioned_value(host_id.to_sstring());
        }

        versioned_value tokens(const std::unordered_set<token>& tokens) {
            return versioned_value(make_full_token_string(tokens));
        }

        versioned_value removing_nonlocal(const utils::UUID& host_id) {
            return versioned_value(sstring(REMOVING_TOKEN) +
                sstring(DELIMITER_STR) + host_id.to_sstring());
        }

        versioned_value removed_nonlocal(const utils::UUID& host_id, int64_t expire_time) {
            return versioned_value(sstring(REMOVED_TOKEN) + sstring(DELIMITER_STR) +
                host_id.to_sstring() + sstring(DELIMITER_STR) + to_sstring(expire_time));
        }

        versioned_value removal_coordinator(const utils::UUID& host_id) {
            return versioned_value(sstring(REMOVAL_COORDINATOR) +
                sstring(DELIMITER_STR) + host_id.to_sstring());
        }

        versioned_value hibernate(bool value) {
            return versioned_value(sstring(HIBERNATE) + sstring(DELIMITER_STR) + (value ? "true" : "false"));
        }

        versioned_value shutdown(bool value) {
            return versioned_value(sstring(SHUTDOWN) + sstring(DELIMITER_STR) + (value ? "true" : "false"));
        }

        versioned_value datacenter(const sstring& dc_id) {
            return versioned_value(dc_id);
        }

        versioned_value rack(const sstring& rack_id) {
            return versioned_value(rack_id);
        }

        versioned_value rpcaddress(gms::inet_address endpoint) {
            return versioned_value(sprint("%s", endpoint));
        }

        versioned_value release_version() {
            return versioned_value(version::release());
        }

        versioned_value network_version();

        versioned_value internal_ip(const sstring &private_ip) {
            return versioned_value(private_ip);
        }

        versioned_value severity(double value) {
            return versioned_value(to_sstring(value));
        }

        versioned_value supported_features(const sstring& features) {
            return versioned_value(features);
        }

        versioned_value cache_hitrates(const sstring& hitrates) {
            return versioned_value(hitrates);
        }

        versioned_value cql_ready(bool value) {
            return versioned_value(to_sstring(int(value)));
        }
    };
}; // class versioned_value

} // namespace gms
