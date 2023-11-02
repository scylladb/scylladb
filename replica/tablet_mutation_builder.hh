/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation.hh"
#include "db/system_keyspace.hh"
#include "replica/tablets.hh"
#include "service/session.hh"

namespace replica {

class tablet_mutation_builder {
    api::timestamp_type _ts;
    schema_ptr _s;
    mutation _m;
private:
    clustering_key get_ck(dht::token last_token) {
        return clustering_key::from_single_value(*_s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
    }
public:
    tablet_mutation_builder(api::timestamp_type ts, const sstring& keyspace_name, table_id table)
            : _ts(ts)
            , _s(db::system_keyspace::tablets())
            , _m(_s, partition_key::from_exploded(*_s, {
                    data_value(keyspace_name).serialize_nonnull(),
                    data_value(table.uuid()).serialize_nonnull()
            }))
    { }

    tablet_mutation_builder& set_new_replicas(dht::token last_token, locator::tablet_replica_set replicas);
    tablet_mutation_builder& set_replicas(dht::token last_token, locator::tablet_replica_set replicas);
    tablet_mutation_builder& set_stage(dht::token last_token, locator::tablet_transition_stage stage);
    tablet_mutation_builder& set_session(dht::token last_token, service::session_id);
    tablet_mutation_builder& del_session(dht::token last_token);
    tablet_mutation_builder& del_transition(dht::token last_token);

    mutation build() {
        return std::move(_m);
    }
};

} // namespace replica
