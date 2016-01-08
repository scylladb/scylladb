/*
 * Copyright 2016 ScyllaDB
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
namespace utils {
class UUID final {
    int64_t most_sig_bits;
    int64_t least_sig_bits;
};

}

namespace gms {
enum class application_state:int {STATUS = 0,
        LOAD,
        SCHEMA,
        DC,
        RACK,
        RELEASE_VERSION,
        REMOVAL_COORDINATOR,
        INTERNAL_IP,
        RPC_ADDRESS,
        X_11_PADDING,
        SEVERITY,
        NET_VERSION,
        HOST_ID,
        TOKENS,
        X1,
        X2,
        X3,
        X4,
        X5,
        X6,
        X7,
        X8,
        X9,
        X10};
class inet_address final {
  uint32_t raw_addr();
};

class versioned_value {
    sstring value;
    int version;
};

class heart_beat_state {
    int32_t get_generation();
    int32_t get_heart_beat_version();
};

class endpoint_state {
    gms::heart_beat_state get_heart_beat_state();
    std::map<gms::application_state, gms::versioned_value> get_application_state_map();
}

class gossip_digest {
    gms::inet_address get_endpoint();
    int32_t get_generation();
    int32_t get_max_version();
}

class gossip_digest_ack {
    std::vector<gms::gossip_digest> get_gossip_digest_list();
    std::map<gms::inet_address, gms::endpoint_state> get_endpoint_state_map();
}
}
