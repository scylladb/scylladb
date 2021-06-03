/*
 * Copyright (C) 2020-present ScyllaDB
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

#include "cdc_partitioner.hh"
#include "dht/token.hh"
#include "schema.hh"
#include "sstables/key.hh"
#include "utils/class_registrator.hh"
#include "cdc/generation.hh"
#include "keys.hh"

static const sstring cdc_partitioner_name = "com.scylladb.dht.CDCPartitioner";

namespace cdc {

const sstring cdc_partitioner::name() const {
    return cdc_partitioner_name;
}

static dht::token to_token(int64_t value) {
    return dht::token(dht::token::kind::key, value);
}

static dht::token to_token(bytes_view key) {
    // Key should be 16 B long, of which first 8 B are used for token calculation
    if (key.size() != 2*sizeof(int64_t)) {
        return dht::minimum_token();
    }
    return to_token(stream_id::token_from_bytes(key));
}

dht::token
cdc_partitioner::get_token(const sstables::key_view& key) const {
    return to_token(bytes_view(key));
}

dht::token
cdc_partitioner::get_token(const schema& s, partition_key_view key) const {
    auto exploded_key = key.explode(s);
    return to_token(exploded_key[0]);
}

using registry = class_registrator<dht::i_partitioner, cdc_partitioner>;
static registry registrator(cdc_partitioner_name);
static registry registrator_short_name("CDCPartitioner");

}
